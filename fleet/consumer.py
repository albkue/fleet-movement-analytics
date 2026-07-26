"""Consume the telemetry topic: land pings, run the detector, checkpoint state.

Delivery semantics: at-least-once from Kafka, effectively-once in the
database. The ordering is what buys that --

    1. buffer messages,
    2. in ONE Postgres transaction: write the pings, enrich them, fold them
       through the detector, write the detections, write the updated
       per-vehicle state, and close the batch record,
    3. only then commit Kafka offsets.

A crash between (2) and (3) replays the batch. Every replayed ping collides
with raw.pings' primary key on ping_id and is dropped by ON CONFLICT DO
NOTHING -- and because enrichment selects `WHERE batch_id = <this batch>`,
those dropped rows are not in the new batch and so are never fed to the
detector a second time. A crash before (2) loses nothing, because the offsets
never moved. What is never possible is committing offsets for work that did
not land, or advancing the detector's state past pings that were rolled back:
the state lives in the same transaction as the pings it was derived from.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .config import Settings
from .detector import Detection, VehicleState, ZoneMeta, detect
from .enrich import enrich_batch
from .pings import Ping, PingValidationError, decode_message

log = logging.getLogger(__name__)

_STAGE_DDL = """
CREATE TEMP TABLE _stage_pings (
    ping_id         UUID,
    vehicle_id      TEXT,
    trip_id         TEXT,
    recorded_at     TIMESTAMPTZ,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    h3_r8           TEXT,
    h3_r9           TEXT,
    speed_kph       NUMERIC(6,2),
    heading_deg     NUMERIC(5,2),
    ignition        BOOLEAN,
    odometer_km     NUMERIC(12,2),
    fuel_pct        NUMERIC(5,2),
    payload         JSONB,
    kafka_topic     TEXT,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    batch_id        BIGINT
) ON COMMIT DROP
"""

_PING_COLUMNS = (
    "ping_id",
    "vehicle_id",
    "trip_id",
    "recorded_at",
    "lat",
    "lon",
    "h3_r8",
    "h3_r9",
    "speed_kph",
    "heading_deg",
    "ignition",
    "odometer_km",
    "fuel_pct",
    "payload",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "batch_id",
)

# An open episode may be re-emitted (on replay) after it has already been
# closed. The WHERE clause is what stops that re-opening it: only a row that
# carries an end time is allowed to overwrite what is already there.
_DETECTION_UPSERT = """
INSERT INTO stream.detections
    (detection_key, detection_type, severity, vehicle_id, trip_id, zone_id,
     started_at, ended_at, duration_seconds, lat, lon, magnitude, details,
     batch_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (detection_key) DO UPDATE SET
    severity         = EXCLUDED.severity,
    ended_at         = EXCLUDED.ended_at,
    duration_seconds = EXCLUDED.duration_seconds,
    magnitude        = EXCLUDED.magnitude,
    details          = EXCLUDED.details,
    trip_id          = coalesce(stream.detections.trip_id, EXCLUDED.trip_id),
    batch_id         = EXCLUDED.batch_id,
    updated_at       = now()
WHERE EXCLUDED.ended_at IS NOT NULL
"""

_STATE_UPSERT = """
INSERT INTO stream.vehicle_state
    (vehicle_id, last_ping_id, last_ping_at, last_lat, last_lon,
     last_odometer_km, last_trip_id, idle_since, idle_lat, idle_lon,
     idle_pings, idle_ignition_pings, idle_zone_id, idle_reported,
     open_zones, breached_zones, deviating_since, deviation_peak_m,
     deviation_reported, delay_reported_s, pings_seen, pings_out_of_order,
     batch_id, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (vehicle_id) DO UPDATE SET
    last_ping_id       = EXCLUDED.last_ping_id,
    last_ping_at       = EXCLUDED.last_ping_at,
    last_lat           = EXCLUDED.last_lat,
    last_lon           = EXCLUDED.last_lon,
    last_odometer_km   = EXCLUDED.last_odometer_km,
    last_trip_id       = EXCLUDED.last_trip_id,
    idle_since         = EXCLUDED.idle_since,
    idle_lat           = EXCLUDED.idle_lat,
    idle_lon           = EXCLUDED.idle_lon,
    idle_pings         = EXCLUDED.idle_pings,
    idle_ignition_pings = EXCLUDED.idle_ignition_pings,
    idle_zone_id       = EXCLUDED.idle_zone_id,
    idle_reported      = EXCLUDED.idle_reported,
    open_zones         = EXCLUDED.open_zones,
    breached_zones     = EXCLUDED.breached_zones,
    deviating_since    = EXCLUDED.deviating_since,
    deviation_peak_m   = EXCLUDED.deviation_peak_m,
    deviation_reported = EXCLUDED.deviation_reported,
    delay_reported_s   = EXCLUDED.delay_reported_s,
    pings_seen         = EXCLUDED.pings_seen,
    pings_out_of_order = EXCLUDED.pings_out_of_order,
    batch_id           = EXCLUDED.batch_id,
    updated_at         = now()
"""


@dataclass
class ConsumeSummary:
    batches: int = 0
    messages_read: int = 0
    rows_inserted: int = 0
    rows_duplicate: int = 0
    rows_rejected: int = 0
    detections: dict[str, int] = field(default_factory=dict)
    elapsed_seconds: float = 0.0
    stopped_because: str = "idle"

    @property
    def rate(self) -> float:
        return self.messages_read / self.elapsed_seconds if self.elapsed_seconds else 0.0

    @property
    def detections_total(self) -> int:
        return sum(self.detections.values())

    def add_detections(self, counts: Mapping[str, int]) -> None:
        for name, value in counts.items():
            self.detections[name] = self.detections.get(name, 0) + value


@dataclass
class _Rejected:
    partition: int
    offset: int
    key: str | None
    value: str | None
    error: str


@dataclass
class _Batch:
    """Messages buffered since the last flush."""

    topic: str
    pings: list[tuple[Ping, int, int]] = field(default_factory=list)
    rejected: list[_Rejected] = field(default_factory=list)
    # partition -> highest offset seen, so we know where to resume.
    max_offsets: dict[int, int] = field(default_factory=dict)
    opened_at: float = field(default_factory=time.monotonic)

    def __len__(self) -> int:
        return len(self.pings) + len(self.rejected)

    def note_offset(self, partition: int, offset: int) -> None:
        current = self.max_offsets.get(partition)
        if current is None or offset > current:
            self.max_offsets[partition] = offset

    def commit_offsets(self) -> list[TopicPartition]:
        # Kafka commits the NEXT offset to read, not the last one read.
        return [
            TopicPartition(self.topic, partition, offset + 1)
            for partition, offset in self.max_offsets.items()
        ]


def build_consumer(settings: Settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "client.id": "fleet-consumer",
            # Offsets are committed by hand after the database transaction
            # commits. Auto-commit would advance them on a timer, which is
            # exactly the way to lose a batch -- and here it would also leave
            # the detector's state describing pings nobody can replay.
            "enable.auto.commit": False,
            # A brand-new group reads the topic from the beginning so the
            # warehouse can be rebuilt from retained telemetry.
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        }
    )


def load_zone_catalogue(conn: psycopg.Connection) -> dict[str, ZoneMeta]:
    """The geofences, read once per run rather than once per ping."""
    zones: dict[str, ZoneMeta] = {}
    for zone_id, kind, dwell in conn.execute(
        "SELECT zone_id, zone_kind, max_dwell_minutes FROM ref.zones"
    ):
        zones[zone_id] = ZoneMeta(
            zone_id=zone_id, zone_kind=kind, max_dwell_minutes=dwell
        )
    return zones


def _parse_open_zones(raw: Any) -> dict[str, datetime]:
    from .pings import parse_timestamp

    if not raw:
        return {}
    return {zone_id: parse_timestamp(value) for zone_id, value in raw.items()}


def load_states(
    conn: psycopg.Connection, vehicle_ids: list[str]
) -> dict[str, VehicleState]:
    """Read back the checkpointed state for the vehicles in this batch.

    Read from the database every batch rather than cached in the process:
    the state has to describe exactly the pings that committed, and a
    rolled-back transaction would leave an in-memory cache describing pings
    that no longer exist.
    """
    if not vehicle_ids:
        return {}

    states: dict[str, VehicleState] = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM stream.vehicle_state WHERE vehicle_id = ANY(%s)",
            (vehicle_ids,),
        )
        for row in cur:
            states[row["vehicle_id"]] = VehicleState(
                vehicle_id=row["vehicle_id"],
                last_ping_id=row["last_ping_id"],
                last_ping_at=row["last_ping_at"],
                last_lat=row["last_lat"],
                last_lon=row["last_lon"],
                last_odometer_km=(
                    None
                    if row["last_odometer_km"] is None
                    else float(row["last_odometer_km"])
                ),
                last_trip_id=row["last_trip_id"],
                idle_since=row["idle_since"],
                idle_lat=row["idle_lat"],
                idle_lon=row["idle_lon"],
                idle_pings=row["idle_pings"],
                idle_ignition_pings=row["idle_ignition_pings"],
                idle_zone_id=row["idle_zone_id"],
                idle_reported=row["idle_reported"],
                open_zones=_parse_open_zones(row["open_zones"]),
                breached_zones=set(row["breached_zones"] or []),
                deviating_since=row["deviating_since"],
                deviation_peak_m=row["deviation_peak_m"],
                deviation_reported=row["deviation_reported"],
                delay_reported_s=row["delay_reported_s"],
                pings_seen=row["pings_seen"],
                pings_out_of_order=row["pings_out_of_order"],
            )
    return states


def _write_detections(
    cur: psycopg.Cursor, detections: list[Detection], batch_id: int
) -> None:
    if not detections:
        return
    # executemany rather than one multi-row INSERT: an episode that opens and
    # closes inside the same batch appears twice with the same key, and
    # Postgres refuses to let ON CONFLICT DO UPDATE touch a row twice within
    # a single statement.
    cur.executemany(
        _DETECTION_UPSERT,
        [
            (
                d.detection_key,
                d.detection_type,
                d.severity,
                d.vehicle_id,
                d.trip_id,
                d.zone_id,
                d.started_at,
                d.ended_at,
                d.duration_seconds,
                d.lat,
                d.lon,
                d.magnitude,
                Jsonb(d.details),
                batch_id,
            )
            for d in detections
        ],
    )


def _write_states(
    cur: psycopg.Cursor, states: dict[str, VehicleState], batch_id: int
) -> None:
    if not states:
        return
    cur.executemany(
        _STATE_UPSERT,
        [
            (
                s.vehicle_id,
                s.last_ping_id,
                s.last_ping_at,
                s.last_lat,
                s.last_lon,
                s.last_odometer_km,
                s.last_trip_id,
                s.idle_since,
                s.idle_lat,
                s.idle_lon,
                s.idle_pings,
                s.idle_ignition_pings,
                s.idle_zone_id,
                s.idle_reported,
                Jsonb({z: at.isoformat() for z, at in s.open_zones.items()}),
                Jsonb(sorted(s.breached_zones)),
                s.deviating_since,
                s.deviation_peak_m,
                s.deviation_reported,
                s.delay_reported_s,
                s.pings_seen,
                s.pings_out_of_order,
                batch_id,
            )
            for s in states.values()
        ],
    )


def _flush(
    conn: psycopg.Connection,
    settings: Settings,
    zones: Mapping[str, ZoneMeta],
    batch: _Batch,
    summary: ConsumeSummary,
) -> None:
    """Land one batch and detect over it, inside a single transaction."""
    inserted = 0
    duplicates = 0
    detected: dict[str, int] = {}

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta.ingest_batches
                    (consumer_group, topic, messages_read, offsets)
                VALUES (%s, %s, %s, %s)
                RETURNING batch_id
                """,
                (
                    settings.kafka_consumer_group,
                    batch.topic,
                    len(batch),
                    Jsonb({str(p): o for p, o in batch.max_offsets.items()}),
                ),
            )
            batch_id = cur.fetchone()[0]

            if batch.pings:
                cur.execute(_STAGE_DDL)
                columns = ", ".join(_PING_COLUMNS)
                with cur.copy(f"COPY _stage_pings ({columns}) FROM STDIN") as copy:
                    for ping, partition, offset in batch.pings:
                        copy.write_row(
                            (
                                ping.ping_id,
                                ping.vehicle_id,
                                ping.trip_id,
                                ping.recorded_at,
                                ping.lat,
                                ping.lon,
                                ping.h3(settings.h3_resolution_coarse),
                                ping.h3(settings.h3_resolution_fine),
                                ping.speed_kph,
                                ping.heading_deg,
                                ping.ignition,
                                ping.odometer_km,
                                ping.fuel_pct,
                                Jsonb(ping.payload),
                                batch.topic,
                                partition,
                                offset,
                                batch_id,
                            )
                        )

                # DISTINCT ON collapses a redelivery that landed twice inside
                # this same buffer, before it can reach the insert.
                cur.execute(
                    f"""
                    INSERT INTO raw.pings ({columns})
                    SELECT DISTINCT ON (ping_id) {columns}
                    FROM _stage_pings
                    ORDER BY ping_id, kafka_offset
                    ON CONFLICT (ping_id) DO NOTHING
                    """
                )
                inserted = cur.rowcount
                duplicates = len(batch.pings) - inserted

            if batch.rejected:
                cur.executemany(
                    """
                    INSERT INTO raw.pings_dead_letter
                        (batch_id, kafka_topic, kafka_partition, kafka_offset,
                         message_key, message_value, error)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)
                    DO NOTHING
                    """,
                    [
                        (
                            batch_id,
                            batch.topic,
                            item.partition,
                            item.offset,
                            item.key,
                            item.value,
                            item.error,
                        )
                        for item in batch.rejected
                    ],
                )

            # --- the stateful half -------------------------------------
            enriched = enrich_batch(cur, batch_id)
            if enriched:
                vehicle_ids = sorted({p.vehicle_id for p in enriched})
                states = load_states(conn, vehicle_ids)
                result = detect(enriched, states, settings, zones)
                _write_detections(cur, result.detections, batch_id)
                _write_states(cur, result.states, batch_id)
                detected = result.by_type()

            cur.execute(
                """
                UPDATE meta.ingest_batches
                   SET status = 'committed',
                       finished_at = now(),
                       rows_inserted = %s,
                       rows_duplicate = %s,
                       rows_rejected = %s,
                       detections_found = %s
                 WHERE batch_id = %s
                """,
                (
                    inserted,
                    duplicates,
                    len(batch.rejected),
                    sum(detected.values()),
                    batch_id,
                ),
            )

    summary.batches += 1
    summary.messages_read += len(batch)
    summary.rows_inserted += inserted
    summary.rows_duplicate += duplicates
    summary.rows_rejected += len(batch.rejected)
    summary.add_detections(detected)

    log.info(
        "batch %d: %d inserted, %d duplicate, %d rejected, %d detection(s)",
        batch_id,
        inserted,
        duplicates,
        len(batch.rejected),
        sum(detected.values()),
    )


def _for_storage(raw: bytes | None, limit: int = 8000) -> str | None:
    """Make arbitrary message bytes safe to store in a text column.

    A dead letter is by definition a message we could not parse, so its bytes
    may be anything at all. Two things have to be handled or the dead-letter
    write itself fails -- turning a single bad message into a poison pill that
    stalls the partition forever, which is precisely what this table exists to
    prevent:

      * invalid UTF-8, replaced rather than raised on;
      * NUL bytes, which Postgres text columns reject outright and which
        'replace' error handling does not remove.

    Truncated too: this is for diagnosis, not for storing an unbounded payload
    someone accidentally published.
    """
    if raw is None:
        return None
    return raw.decode("utf-8", "replace").replace("\x00", "\\x00")[:limit]


def _decode(msg: Any) -> tuple[Ping | None, _Rejected | None]:
    try:
        return decode_message(msg.value()), None
    except PingValidationError as exc:
        return None, _Rejected(
            partition=msg.partition(),
            offset=msg.offset(),
            key=_for_storage(msg.key(), limit=512),
            value=_for_storage(msg.value()),
            error=str(exc),
        )


def consume(
    settings: Settings,
    *,
    max_messages: int | None = None,
    idle_timeout_seconds: float | None = None,
) -> ConsumeSummary:
    """Run the consumer loop until idle, message cap, or Ctrl-C."""
    idle_timeout = (
        settings.consumer_idle_timeout_seconds
        if idle_timeout_seconds is None
        else idle_timeout_seconds
    )

    consumer = build_consumer(settings)
    consumer.subscribe([settings.kafka_topic])

    summary = ConsumeSummary()
    started = time.monotonic()
    last_message_at = time.monotonic()

    # autocommit=True at the connection level; each batch opens its own
    # explicit transaction via conn.transaction().
    conn = psycopg.connect(settings.dsn, autocommit=True)
    zones = load_zone_catalogue(conn)
    if not zones:
        # Everything else would still work, and every geofence number would
        # be zero. Silently reporting "no breaches" for an unseeded database
        # is the worst of the available behaviours.
        log.warning(
            "ref.zones is empty -- no geofence detections are possible. "
            "Run: python -m fleet seed"
        )
    else:
        log.info("loaded %d geofence zone(s)", len(zones))

    batch = _Batch(topic=settings.kafka_topic)

    def flush_if_any() -> None:
        nonlocal batch
        if not len(batch):
            return
        _flush(conn, settings, zones, batch, summary)
        # Offsets move only after the transaction above has committed.
        consumer.commit(offsets=batch.commit_offsets(), asynchronous=False)
        batch = _Batch(topic=settings.kafka_topic)

    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            now = time.monotonic()

            if msg is None:
                if (
                    len(batch)
                    and now - batch.opened_at >= settings.consumer_batch_timeout_seconds
                ):
                    flush_if_any()
                if idle_timeout and now - last_message_at >= idle_timeout:
                    summary.stopped_because = "idle"
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            last_message_at = now
            ping, rejected = _decode(msg)
            if ping is not None:
                batch.pings.append((ping, msg.partition(), msg.offset()))
            else:
                assert rejected is not None
                batch.rejected.append(rejected)
            batch.note_offset(msg.partition(), msg.offset())

            if len(batch) >= settings.consumer_batch_size:
                flush_if_any()

            if (
                max_messages is not None
                and summary.messages_read + len(batch) >= max_messages
            ):
                flush_if_any()
                summary.stopped_because = "max-messages"
                break

    except KeyboardInterrupt:
        summary.stopped_because = "interrupted"
        log.info("interrupted -- flushing buffered messages")
    finally:
        try:
            flush_if_any()
        finally:
            # close() commits nothing (auto-commit is off) but does leave the
            # group cleanly, so a restart does not wait out session.timeout.
            consumer.close()
            conn.close()

    summary.elapsed_seconds = time.monotonic() - started
    return summary
