-- Fleet movement analytics schema.
--
-- Layered:  raw    (as-landed telemetry pings)
--           ref    (fleet reference data: vehicles, geofences, routes, trips)
--           stream (output of the stateful stream processor: detections and
--                   the per-vehicle state it checkpoints)
--        -> stg    (typed/cleaned, built by the model runner)
--        -> mart   (dims, facts, aggregates, built by the model runner)
--           meta   (ingest + transform run log)
--
-- This file owns raw, ref, stream and meta. stg and mart are (re)built from
-- models/**.sql by `python -m fleet transform`, so their DDL lives there
-- rather than here -- the runner drops and recreates those objects.
--
-- Every statement is idempotent so the file can be re-applied at any time.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS stream;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS meta;

-- ---------------------------------------------------------------- meta ----

-- One row per consumer flush. A batch is the unit that makes the whole
-- pipeline restartable: pings, detections, the updated vehicle state and this
-- row are written inside one transaction, and only then are Kafka offsets
-- committed.
CREATE TABLE IF NOT EXISTS meta.ingest_batches (
    batch_id         BIGSERIAL PRIMARY KEY,
    consumer_group   TEXT        NOT NULL,
    topic            TEXT        NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,
    status           TEXT        NOT NULL DEFAULT 'running'
                     CHECK (status IN ('running', 'committed', 'failed')),
    messages_read    INTEGER     NOT NULL DEFAULT 0,
    rows_inserted    INTEGER     NOT NULL DEFAULT 0,
    rows_duplicate   INTEGER     NOT NULL DEFAULT 0,
    rows_rejected    INTEGER     NOT NULL DEFAULT 0,
    detections_found INTEGER     NOT NULL DEFAULT 0,
    -- {"<partition>": <last offset read>} -- lineage for replay/debugging.
    offsets          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    error            TEXT
);

CREATE INDEX IF NOT EXISTS ix_ingest_batches_started_at
    ON meta.ingest_batches (started_at DESC);

-- One row per model per `transform` invocation.
CREATE TABLE IF NOT EXISTS meta.model_runs (
    model_run_id  BIGSERIAL PRIMARY KEY,
    run_id        UUID        NOT NULL,
    model_name    TEXT        NOT NULL,
    materialized  TEXT        NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT        NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running', 'success', 'skipped', 'failed')),
    rows_affected BIGINT,
    full_refresh  BOOLEAN     NOT NULL DEFAULT false,
    error         TEXT
);

CREATE INDEX IF NOT EXISTS ix_model_runs_run_id
    ON meta.model_runs (run_id, started_at);

-- One row per schema test per `test` invocation.
CREATE TABLE IF NOT EXISTS meta.test_results (
    test_result_id BIGSERIAL PRIMARY KEY,
    run_id         UUID        NOT NULL,
    model_name     TEXT        NOT NULL,
    column_name    TEXT,
    test_name      TEXT        NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('pass', 'fail', 'error')),
    failing_rows   BIGINT,
    executed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    error          TEXT
);

CREATE INDEX IF NOT EXISTS ix_test_results_run_id
    ON meta.test_results (run_id, model_name);

-- ----------------------------------------------------------------- ref ----
--
-- Reference data, loaded from config/fleet.json by `python -m fleet seed`.
-- Everything geospatial is `geography` rather than `geometry`: the questions
-- asked of it are "how many metres" and "is this point inside", and geography
-- answers those on the spheroid in metres without picking a projected SRID
-- that would only be valid for one city.

CREATE TABLE IF NOT EXISTS ref.zones (
    zone_id           TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    zone_kind         TEXT NOT NULL
                      CHECK (zone_kind IN ('depot', 'customer', 'restricted',
                                           'congestion')),
    -- Dwell beyond this raises a breach. NULL means "no dwell limit", which
    -- is the right answer for a congestion corridor: sitting in traffic is
    -- worth measuring but is not a violation.
    max_dwell_minutes INTEGER CHECK (max_dwell_minutes > 0),
    boundary          geography(POLYGON, 4326) NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Point-in-polygon on every ping of every batch goes through this.
CREATE INDEX IF NOT EXISTS ix_zones_boundary ON ref.zones USING GIST (boundary);

CREATE TABLE IF NOT EXISTS ref.routes (
    route_id                 TEXT PRIMARY KEY,
    name                     TEXT NOT NULL,
    start_zone_id            TEXT NOT NULL REFERENCES ref.zones (zone_id),
    end_zone_id              TEXT NOT NULL REFERENCES ref.zones (zone_id),
    planned_duration_minutes INTEGER NOT NULL CHECK (planned_duration_minutes > 0),
    -- Minutes allowed at each served stop. Held separately from the total
    -- because the two are spent differently: driving time is spread along
    -- the route, service time is spent standing still at one point on it.
    service_minutes          INTEGER NOT NULL DEFAULT 0 CHECK (service_minutes >= 0),
    stop_zone_ids            TEXT[]  NOT NULL DEFAULT '{}',
    path                     geography(LINESTRING, 4326) NOT NULL,
    loaded_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_routes_path ON ref.routes USING GIST (path);

-- The planned schedule as checkpoints along the route, derived at seed time
-- from the route geometry and its stops.
--
-- This table exists because "how late is this vehicle" is meaningless
-- against a single planned duration. Spreading 75 planned minutes evenly
-- over the route says a vehicle should be 40% done after 30 minutes -- but
-- it may have spent 8 of those minutes parked at a customer, which the plan
-- allows for and the straight line does not. Every trip would then look late
-- just after each stop and early just before it.
--
-- Each row is a *departure*: by `elapsed_seconds` after the planned start,
-- the vehicle should have left `fraction` of the way along the route.
-- Interpolating between consecutive rows gives an expected elapsed time that
-- steps over service stops instead of averaging them away.
CREATE TABLE IF NOT EXISTS ref.route_schedule (
    route_id        TEXT    NOT NULL REFERENCES ref.routes (route_id)
                    ON DELETE CASCADE,
    seq             INTEGER NOT NULL,
    fraction        DOUBLE PRECISION NOT NULL CHECK (fraction BETWEEN 0 AND 1),
    elapsed_seconds INTEGER NOT NULL CHECK (elapsed_seconds >= 0),
    -- The stop being departed, or NULL for the two route endpoints.
    zone_id         TEXT    REFERENCES ref.zones (zone_id),
    PRIMARY KEY (route_id, seq)
);

CREATE TABLE IF NOT EXISTS ref.vehicles (
    vehicle_id    TEXT PRIMARY KEY,
    plate         TEXT NOT NULL,
    vehicle_type  TEXT NOT NULL
                  CHECK (vehicle_type IN ('van', 'truck', 'motorbike')),
    capacity_kg   INTEGER NOT NULL CHECK (capacity_kg > 0),
    home_depot_id TEXT NOT NULL REFERENCES ref.zones (zone_id),
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The planned schedule. A trip is what makes "delayed" and "off-route"
-- answerable at all: without an assignment there is no route to deviate from
-- and no arrival time to be late for.
CREATE TABLE IF NOT EXISTS ref.trips (
    trip_id          TEXT PRIMARY KEY,
    vehicle_id       TEXT NOT NULL REFERENCES ref.vehicles (vehicle_id),
    route_id         TEXT NOT NULL REFERENCES ref.routes (route_id),
    planned_start_at TIMESTAMPTZ NOT NULL,
    planned_end_at   TIMESTAMPTZ NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (planned_end_at > planned_start_at)
);

CREATE INDEX IF NOT EXISTS ix_trips_vehicle_start
    ON ref.trips (vehicle_id, planned_start_at);

-- ----------------------------------------------------------------- raw ----

-- Landing table for validated pings.
--
-- ping_id is the device-assigned UUID and the primary key, which is what
-- makes redelivery harmless: the consumer inserts ON CONFLICT DO NOTHING, so
-- at-least-once delivery from Kafka lands exactly once here.
--
-- lat/lon are stored as plain doubles and `location` is generated from them.
-- Storing the pair keeps COPY free of any geometry encoding, and generating
-- the geography means there is exactly one derivation of it rather than one
-- per query.
CREATE TABLE IF NOT EXISTS raw.pings (
    ping_id         UUID        PRIMARY KEY,
    vehicle_id      TEXT        NOT NULL,
    trip_id         TEXT,
    recorded_at     TIMESTAMPTZ NOT NULL,
    lat             DOUBLE PRECISION NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lon             DOUBLE PRECISION NOT NULL CHECK (lon BETWEEN -180 AND 180),
    location        geography(POINT, 4326)
                    GENERATED ALWAYS AS
                    (ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography) STORED,
    -- H3 cell ids, computed by the producer-side contract in fleet/geo.py.
    -- Indexing a point into a fixed hexagonal grid at ingest turns "where is
    -- the fleet dense" into a GROUP BY on a text column instead of a spatial
    -- join against a polygon table.
    h3_r8           TEXT        NOT NULL,
    h3_r9           TEXT        NOT NULL,
    speed_kph       NUMERIC(6,2) NOT NULL CHECK (speed_kph >= 0),
    heading_deg     NUMERIC(5,2) CHECK (heading_deg >= 0 AND heading_deg < 360),
    ignition        BOOLEAN     NOT NULL,
    odometer_km     NUMERIC(12,2) CHECK (odometer_km >= 0),
    fuel_pct        NUMERIC(5,2) CHECK (fuel_pct BETWEEN 0 AND 100),
    payload         JSONB       NOT NULL,
    kafka_topic     TEXT        NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    batch_id        BIGINT      REFERENCES meta.ingest_batches (batch_id),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Drives the incremental staging model's watermark.
CREATE INDEX IF NOT EXISTS ix_pings_ingested_at ON raw.pings (ingested_at);

-- Every per-vehicle window function downstream, and the detector's own
-- enrichment query, read pings for one vehicle in time order.
CREATE INDEX IF NOT EXISTS ix_pings_vehicle_time
    ON raw.pings (vehicle_id, recorded_at);

-- The detector enriches exactly one batch at a time, so this lookup is on
-- the hot path of every flush.
CREATE INDEX IF NOT EXISTS ix_pings_batch ON raw.pings (batch_id);

CREATE INDEX IF NOT EXISTS ix_pings_trip ON raw.pings (trip_id, recorded_at);
CREATE INDEX IF NOT EXISTS ix_pings_location ON raw.pings USING GIST (location);

-- Messages that could not be parsed or failed validation. Keeping them out
-- of raw.pings means one malfunctioning tracker cannot stall the consumer,
-- and keeping them at all means the loss is visible instead of silent.
CREATE TABLE IF NOT EXISTS raw.pings_dead_letter (
    dead_letter_id  BIGSERIAL PRIMARY KEY,
    batch_id        BIGINT      REFERENCES meta.ingest_batches (batch_id),
    kafka_topic     TEXT        NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    message_key     TEXT,
    -- Stored as text, not jsonb: the whole point is that it may not be valid
    -- JSON, and a jsonb column would reject exactly the rows we need to keep.
    message_value   TEXT,
    error           TEXT        NOT NULL,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_pings_dead_letter_rejected_at
    ON raw.pings_dead_letter (rejected_at DESC);

-- Replaying the topic must not multiply the dead letters either.
--
-- A rejected message cannot be deduplicated on anything inside itself: it was
-- rejected precisely because its contents could not be trusted, and the worst
-- offenders are not even JSON. Its position on the topic, though, is assigned
-- by the broker and is stable across any number of replays -- so that is the
-- identity used.
CREATE UNIQUE INDEX IF NOT EXISTS ux_pings_dead_letter_coordinates
    ON raw.pings_dead_letter (kafka_topic, kafka_partition, kafka_offset);

-- -------------------------------------------------------------- stream ----

-- What the stream processor found. One row per *episode*, not per ping: an
-- idle stop is one row with a duration, not 40 rows saying "still stopped".
--
-- An episode is written twice. When it opens, `ended_at` is NULL -- the
-- vehicle is idling, or off route, or inside a zone, right now. When it
-- closes, the same row is written again with the end time and the duration,
-- and the upsert on detection_key merges it. That way a dashboard sees an
-- alert while it is still happening, and history still gets one complete row
-- per episode instead of an open-ended fragment.
--
-- detection_key is a deterministic function of (type, vehicle, zone, start),
-- so re-processing the same pings produces the same keys and the unique
-- constraint absorbs them. That is the second line of defence; the first is
-- that the detector only ever reads the batch it is currently committing.
CREATE TABLE IF NOT EXISTS stream.detections (
    detection_id     BIGSERIAL PRIMARY KEY,
    detection_key    TEXT        NOT NULL UNIQUE,
    detection_type   TEXT        NOT NULL
                     CHECK (detection_type IN ('zone_visit', 'geofence_breach',
                                               'idle', 'route_deviation',
                                               'delay', 'gps_gap')),
    severity         TEXT        NOT NULL
                     CHECK (severity IN ('info', 'warning', 'critical')),
    vehicle_id       TEXT        NOT NULL,
    trip_id          TEXT,
    zone_id          TEXT,
    started_at       TIMESTAMPTZ NOT NULL,
    ended_at         TIMESTAMPTZ,
    duration_seconds INTEGER     CHECK (duration_seconds >= 0),
    lat              DOUBLE PRECISION,
    lon              DOUBLE PRECISION,
    -- Type-specific magnitude, so "how bad was it" is one column rather than
    -- a jsonb lookup whose shape differs per type: idle -> minutes stopped,
    -- route_deviation -> peak metres off route, delay -> minutes behind
    -- schedule, gps_gap -> minutes without a fix, zone_visit and
    -- geofence_breach -> minutes dwelled.
    magnitude        NUMERIC(12,2),
    details          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    batch_id         BIGINT      REFERENCES meta.ingest_batches (batch_id),
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Bumped when an open episode is closed. The staging model watermarks on
    -- this rather than on detected_at, because an episode that opened an hour
    -- ago and closed a minute ago has to be re-read -- and detected_at, which
    -- is deliberately the time it was first *seen*, would never pull it back.
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

CREATE INDEX IF NOT EXISTS ix_detections_updated_at
    ON stream.detections (updated_at);

CREATE INDEX IF NOT EXISTS ix_detections_started_at
    ON stream.detections (started_at DESC);
CREATE INDEX IF NOT EXISTS ix_detections_vehicle
    ON stream.detections (vehicle_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_detections_type
    ON stream.detections (detection_type, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_detections_batch ON stream.detections (batch_id);

-- The stream processor's checkpoint: one row per vehicle, holding everything
-- needed to continue a partially-observed episode.
--
-- This is the piece that makes the processing genuinely stateful rather than
-- per-batch. It is written in the SAME transaction as the pings and the
-- detections from that batch, so state can never be ahead of or behind the
-- data it was derived from -- which is what a restart depends on.
CREATE TABLE IF NOT EXISTS stream.vehicle_state (
    vehicle_id         TEXT PRIMARY KEY,

    -- last observed position, for the first segment of the next batch
    last_ping_id       UUID,
    last_ping_at       TIMESTAMPTZ,
    last_lat           DOUBLE PRECISION,
    last_lon           DOUBLE PRECISION,
    last_odometer_km   NUMERIC(12,2),
    last_trip_id       TEXT,

    -- open idle run (NULL = the vehicle was moving at the last ping)
    idle_since         TIMESTAMPTZ,
    idle_lat           DOUBLE PRECISION,
    idle_lon           DOUBLE PRECISION,
    idle_pings         INTEGER NOT NULL DEFAULT 0,
    -- How many of those pings had the engine running. An idling engine is
    -- burning fuel; a parked one is not, and the two should not carry the
    -- same severity even though both look identical in the speed column.
    idle_ignition_pings INTEGER NOT NULL DEFAULT 0,
    idle_zone_id       TEXT,
    -- Whether the open idle run has already been reported. An episode is
    -- announced once when it crosses the threshold and updated once when it
    -- ends; this is what stops the announcement repeating every ping.
    idle_reported      BOOLEAN NOT NULL DEFAULT false,

    -- zones the vehicle is currently inside: {"<zone_id>": "<entered_at>"}.
    -- A map rather than an array because the exit event needs the dwell, and
    -- zones overlap, so several may be open at once.
    open_zones         JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- zones already reported as a dwell breach on this visit, so a vehicle
    -- parked for two hours raises one breach and not one per ping
    breached_zones     JSONB NOT NULL DEFAULT '[]'::jsonb,

    -- open route deviation
    deviating_since    TIMESTAMPTZ,
    deviation_peak_m   DOUBLE PRECISION,
    deviation_reported BOOLEAN NOT NULL DEFAULT false,

    -- largest schedule slip already reported for the current trip, in
    -- seconds, so an alert re-fires only when the vehicle falls further
    -- behind rather than on every ping
    delay_reported_s   INTEGER,

    pings_seen         BIGINT NOT NULL DEFAULT 0,
    pings_out_of_order BIGINT NOT NULL DEFAULT 0,
    batch_id           BIGINT REFERENCES meta.ingest_batches (batch_id),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
