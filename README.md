# Fleet Movement Analytics Platform

A real-time geospatial pipeline for vehicle telemetry: position pings arrive
through Kafka, a stateful stream processor detects idling, geofence breaches,
route deviations and schedule slip against PostGIS, and a dbt-style warehouse
turns the result into trip, vehicle and zone analytics.

```
 simulator ──▶ Kafka topic ──▶ consumer ──┬──▶ raw.pings ──▶ models/ ──▶ mart.*
              fleet.telemetry             │    (PostGIS)     (DAG)      trips
              12 partitions,              │                             alerts
              keyed by vehicle            └──▶ detector ──▶ stream.detections
                                               (stateful)   stream.vehicle_state
```

Everything runs locally against `docker compose`: a single-node Kafka in KRaft
mode (no ZooKeeper) and PostGIS 3.4 on Postgres 16.

---

## Quick start

```bash
docker compose up -d
```

```bash
python -m venv .venv && ./.venv/Scripts/pip install -r requirements.txt
```

```bash
cp .env.example .env && python -m fleet init-db && python -m fleet seed
```

Simulate a shift, publish it, and build the warehouse:

```bash
python -m fleet simulate --hours 8 --seed 42 --corrupt 12
```

```bash
python -m fleet pipeline --idle-timeout 10
```

Then look at what it found:

```bash
python -m fleet alerts
```

```
Detections by type
  type               episodes  crit  warn  info  open      avg      max unit   worst x  vehicles
  delay                    94    46    48     0     0     32.0    111.3 min       11.1        12
  idle                    126    16    41    69     5     16.1     54.5 min       10.9        12
  geofence_breach          23     4    19     0     2     32.3     58.8 min        2.6        11
  zone_visit              161     0     0   161     8     23.5     87.5 min          -        12
  route_deviation          10     0    10     0     0    239.7    344.5 m          2.9         8
  gps_gap                   5     0     1     4     0     22.4     31.8 min        3.2         4
```

```bash
python -m fleet map && start fleet-map.html
```

On Linux/macOS the interpreter is `.venv/bin/python`.

---

## The three layers

### 1. Ingestion — Kafka

`fleet/simulator.py` generates movement, `fleet/producer.py` publishes it,
`fleet/consumer.py` lands it in `raw.pings`.

**Pings are keyed by `vehicle_id`.** That is the load-bearing decision in the
whole design. Every detection here is a fold over one vehicle's pings *in
order* — how long it has been stopped, whether this is the same excursion as
last ping's, when it entered the zone it is still in. Kafka guarantees order
within a partition and nowhere else, so keying by vehicle is what turns
"approximately when it stopped" into "when it stopped".

**Validation is strict at the edge**, because a bad ping is not a bad row, it
is a bad fact about physics. A speed of 940 kph or a latitude of 104.9 does
not become harmless downstream; it silently poisons every average it lands
in. Rejected messages go to `raw.pings_dead_letter` with the reason, and the
consumer keeps going — one malfunctioning tracker cannot stall a partition.

Verified rather than asserted. Publishing 12 deliberately malformed messages
(which bypass the producer's own validator, exactly as a third-party tracker
would) produced 12 dead letters covering six distinct failure modes:

```
  dead letters      12  (raw.pings_dead_letter)
         4  expected a JSON object
         3  message value is not valid JSON: Expecting value: line 1 column 1
         2  speed_kph must be between 0.0 and 250.0
         1  ping_id is not a UUID: 'not-a-uuid'
         1  message contains the non-JSON constant NaN
         1  location.lat must be between -90.0 and 90.0
```

### 2. Stream processing — PostGIS for space, Python for time

The processing is split along one line, and the split is the point:

| | answers | because |
| --- | --- | --- |
| **PostGIS** (`fleet/enrich.py`) | which geofences contain this point, how far it is from the assigned route, how far along that route, how that compares to the plan | one set-based query per batch, against the polygons where they live |
| **Python** (`fleet/detector.py`) | has it been stopped long enough to count, is this the same excursion as last ping's, when did this visit start | a state machine that spans batches, which is not a window function |

Doing the spatial half in Python would mean shipping every polygon to the
client and reimplementing point-in-polygon badly. Doing the temporal half in
SQL would mean expressing cross-batch state as a window function, which is
where this kind of pipeline usually goes wrong.

**State is checkpointed in the same transaction as the data it came from.**
Per batch, in one transaction: write the pings, enrich them, fold them through
the detector, write the detections, write the updated `stream.vehicle_state`,
close the batch record. Kafka offsets are committed only after that commits.

So a crash between the write and the offset commit replays the batch; every
replayed ping collides with `raw.pings`' primary key and is dropped — and
because enrichment selects `WHERE batch_id = <this batch>`, those dropped rows
are never fed to the detector a second time. The state can never be ahead of
or behind the pings it was derived from, which is the whole basis of a
restart.

Verified by replaying the entire topic three times with fresh consumer groups:

```
Read 19,489 message(s) in 39 batch(es) -- stopped: idle
  inserted   0
  duplicate  19,477  (already in raw.pings)
  rejected   12

Detections raised in this run: 0
```

`raw.pings` 19,477, `stream.detections` 419 and `raw.pings_dead_letter` 12,
all unchanged across every pass.

### 3. Warehouse — a small dbt-style transform layer

`fleet/transform/` is a miniature dbt: SQL models declare themselves,
reference each other, and the runner derives build order from that.

```sql
{{ config(materialized='incremental', unique_key='ping_id') }}

select ... from {{ ref('stg_pings') }}
```

| Supported | Meaning |
| --- | --- |
| `{{ config(...) }}` | `materialized`, `unique_key`, `schema`, `indexes`, `description` |
| `{{ ref('model') }}` | resolves to `schema.model` **and records a DAG edge** |
| `{{ source('raw','pings') }}` | a table this project does not build |
| `{{ this }}` | the model's own relation |
| `{{ var('name') }}` | a threshold injected by the runner |
| `{% if is_incremental() %}…{% endif %}` | kept only on incremental builds |

Build order is resolved with Kahn's algorithm, ties broken alphabetically so a
run never reorders itself. Tables are built under a scratch name and swapped
in by `ALTER TABLE … RENAME`, so a rebuild is atomic and readers never see one
half-full.

Detection thresholds reach SQL through `var()` rather than being retyped in
it. A model that decides for itself what "idle" means will eventually disagree
with the detector that raised the alert, and the two numbers will be defended
by different people in the same meeting.

---

## What it detects, and why each rule has the shape it has

| Detection | Rule | The bit that matters |
| --- | --- | --- |
| **idle** | below `IDLE_SPEED_KPH` for over `IDLE_MINUTES`, *and* still within 60 m of where it stopped | speed alone counts an hour of crawling through traffic as parked |
| **zone_visit** | one episode per entry, closed on exit with a dwell | not an enter row and an exit row a reader has to pair back up |
| **geofence_breach** | entering a `restricted` zone at all, or overstaying a zone's `max_dwell_minutes` | a congestion corridor has no dwell limit — sitting in traffic is worth measuring but is not a violation |
| **route_deviation** | over `DEVIATION_METRES` from the assigned route for `DEVIATION_SECONDS` | a single ping off route is GPS error, and alerting on it trains people to ignore alerts |
| **delay** | behind `ref.route_schedule`, re-raised only on a further `DELAY_RESTEP_MINUTES` | otherwise one late trip is two hundred rows |
| **gps_gap** | over `GPS_GAP_MINUTES` without a fix | see below — this one exists to keep the others honest |

**Every episode is written twice.** Once when it opens, with `ended_at` null —
a vehicle that is off route *right now* is the most operationally interesting
row in the table, and dropping it because it has no end time yet would make
the fact table useless for the case it should be best at. Once when it closes,
with the duration, merged onto the same row by an upsert on `detection_key`.
That key is a pure function of (type, vehicle, zone, start), so reprocessing
produces the same keys and collides rather than duplicating.

**The gap detection is there to stop the others lying.** A vehicle that
vanishes for half an hour looks exactly like a vehicle standing still. Without
this, idle time would quietly absorb every tunnel and flat battery in the
fleet. So a gap *closes* whatever was open, at the last moment anything was
actually observed, and the unobserved time is counted as neither moving nor
stopped anywhere downstream.

### The delay model is a schedule, not a straight line

Spreading 75 planned minutes evenly along a route says a vehicle should be 40%
of the way round after 30 minutes — but it may have spent 8 of those minutes
parked at a customer, which the plan allows for and the straight line does
not. Every trip would look late just after each stop and early just before it.

So `python -m fleet seed` derives `ref.route_schedule`: the elapsed time by
which the vehicle should have *departed* each fraction of the route, stepping
over the service allowance at each stop. Stop positions and per-ping progress
are both located with `ST_LineLocatePoint`, so the schedule and the
measurement against it share whatever distortion that function has and it
cancels.

---

## Was it right? Reconciling detections against injected incidents

The simulator counts the incidents it deliberately introduces, before any
detection runs. For seed 42 over 8 hours:

| Injected | | Detected | |
| --- | --- | --- | --- |
| signal gaps | 5 | `gps_gap` | **5** |
| deviations | 12 | `route_deviation` | **10** |
| dwell overruns | 16 | `geofence_breach` (dwell) | 19 |
| slow trips | 16 | `delay` | 94 |

Only the first is expected to match exactly, and it does. The others are worth
being precise about rather than waving at:

**Deviations: 12 injected, 10 found — and 10 is correct.** Re-deriving the
excursions independently in Python, straight from the simulated coordinates
and the route geometry, without going near the detector:

```
trips with ANY off-route ping : 11
sustained >= 90s (detectable) : 10
too brief to detect           : 1  {'TR-007-003': 45s}
```

One planned excursion never reached its trigger point before the simulation
window closed, so it never happened. One lasted 45 seconds — under the
90-second sustain rule, and correctly ignored, because that rule is the entire
reason the alerts are worth reading. Ten were detectable and ten were found.

**Dwell overruns: 19 > 16, because injection is not the only cause.** A
vehicle can overstay because the simulator told it to, or because congestion
delayed it into overstaying. Both are real overstays.

**Delays: 94 from 16 slow trips, by design.** A trip that falls further behind
re-alerts every `DELAY_RESTEP_MINUTES`, so one badly late trip is an alert
followed by escalations — the worst here reached 111 minutes behind schedule,
11× the threshold.

---

## Model DAG

```
raw.pings         stream.detections        ref.zones / routes / route_schedule
    │                     │                     vehicles / trips
    ▼                     ▼                              │
stg_pings           stg_detections                       │
    │                     │                              │
    │                     └──▶ fct_alerts ◀──────────────┤
    │                              │                     │
    └──▶ int_ping_segments         │            dim_zone / dim_route
             │                     │            dim_vehicle
             ├──▶ fct_vehicle_hours│                     │
             │         │           │                     │
             │         │           └──▶ fct_trips ◀──────┘
             │         │                    │
             │         └──▶ agg_daily_fleet ◀┘
             │
             └──▶ agg_h3_activity          agg_zone_activity
```

| Model | Grain |
| --- | --- |
| `stg_pings` | one validated ping |
| `stg_detections` | one detection episode |
| `int_ping_segments` | one ping paired with the vehicle's previous one |
| `dim_vehicle` | one vehicle |
| `dim_zone` | one geofence |
| `dim_route` | one route |
| `fct_alerts` | one detection episode, with context |
| `fct_trips` | one assigned trip — **the central fact** |
| `fct_vehicle_hours` | one (vehicle, hour) |
| `agg_daily_fleet` | one (date, vehicle type) |
| `agg_zone_activity` | one (date, zone) |
| `agg_h3_activity` | one (date, H3 r8 cell) |

`stg` holds staging and intermediate models; only `mart` is meant to be
queried from outside the project.

### `int_ping_segments` is where the care went

Nearly every operational number is a sum over this model, so three decisions
in it decide whether the rest is true.

**A segment belongs to the ping that ends it.** The interval between two fixes
is attributed to the later one, so summing by hour or by trip never
double-counts and never orphans time.

**Movement is measured, not reported.** `implied_speed_kph` comes from
distance actually covered over time actually elapsed. A tracker's speed field
can be stale, can be fabricated from a lost fix, and on a stationary vehicle
wanders with GPS noise. At rest, 7 m of noise across a 15-second gap reads as
about 1.7 kph — comfortably under the threshold. Distance is credited only to
segments classified as moving, so a vehicle parked overnight does not
accumulate a kilometre of jitter.

**Unobserved time is counted as neither.** It gets its own column, so
utilisation can be stated against observed time and the remainder stays
visible instead of being silently charged to one side.

That last identity is asserted, not hoped for — `moving_seconds +
stopped_seconds = observed_seconds` is a schema test, and it survives the
rollup exactly:

```
segments        338672.2 s
moving+stopped  338672.2 s
```

---

## Geospatial

**PostGIS `geography`, not `geometry`.** Every question asked of these shapes
is "how many metres" or "is this point inside", and geography answers on the
spheroid in metres without picking a projected SRID valid for one city only.
`mart.dim_zone.area_km2` is real area; the same figure computed from spans in
degrees would be out by about a factor of a hundred and would still look
plausible.

**H3 and the geofences answer different questions.** The zones tell you what
happened at the places someone already drew a box around. The H3 grid tells
you where the fleet actually spends its time, which a polygon table
structurally cannot — congestion on a road nobody geofenced shows up in
`agg_h3_activity` and nowhere else. The cell id is computed once per ping at
ingest, so the density query is a `GROUP BY` on a text column rather than a
spatial join against a grid table, per query, for ever.

```bash
python -m fleet hotspots --limit 3
```

```
Top 3 H3 r8 cell(s) by time spent stopped
  cell                 pings  vehicles  stopped h  moving h  avg kph  stopped %  centre
  88658468cbfffff      1,830        12      12.90      5.24     11.7       71.5  11.5882, 104.8898
  8865846f43fffff      2,461        10       6.87      3.38      7.5       66.4  11.5463, 104.8454
  8865846ae5fffff      1,508         9       4.26      2.02      8.8       66.9  11.5697, 104.9319
```

Ordered by stopped time, not ping count: the busiest cell is always a depot,
and nobody needs a heatmap to find the depot.

---

## The map

```bash
python -m fleet map --hours 6
```

One HTML file, ~100 KB, no tile server, no CDN, no JavaScript library. It
opens offline and it will still open in five years. What a basemap would add
is streets to recognise; what it would cost is a network dependency, an API
key, and a picture that changes underneath the numbers printed beside it.

Every shape comes from the warehouse — `dim_zone.boundary_geojson`,
`dim_route.path_geojson`, `agg_h3_activity`, `fct_alerts`, `stg_pings` — so
the map and the CLI reports cannot disagree. The hexagons are drawn from real
H3 cell geometry, reconstructed from the same cell ids the aggregate is
grouped on, not approximated as circles. Positions are Web Mercator, north is
up, and the scale bar is measured at the centre of the frame.

Layers (geofences, planned routes, vehicle tracks, alerts, H3 density) toggle
independently; every shape carries a native SVG tooltip.

---

## Commands

| Command | What it does |
| --- | --- |
| `init-db` | create/refresh the schemas, and check PostGIS is actually installed |
| `seed` | load `config/fleet.json` into `ref.*` and derive the route schedules |
| `simulate` | generate movement, seed the trips, publish the pings |
| `consume` | load the topic into `raw.pings` and run the detector |
| `transform` | build the model DAG |
| `test` | run the schema tests |
| `pipeline` | consume → transform → test in one pass |
| `alerts` | detections, worst first, filterable by type/severity/vehicle |
| `trips` | trip performance against plan, and punctuality by vehicle type |
| `vehicles` | per-vehicle distance, utilisation and idling |
| `zones` | geofence visits, dwell and breaches |
| `hotspots` | the H3 cells where the fleet sits still |
| `map` | write the self-contained HTML map |
| `status` | ingestion, reference data, batches, open episodes, table counts, last run |

Useful flags:

```bash
python -m fleet simulate --hours 12 --seed 7 --rate 200 --incidents 2.0 --corrupt 20
```

`--rate` paces publishing to simulate live telemetry instead of a bulk dump.
`--incidents` scales the injected incident rates (`0` disables them, which is
the control case). `--dry-run` simulates and summarises without touching Kafka
or the database; `--out FILE` also writes JSON-lines.

```bash
python -m fleet alerts --type route_deviation --severity critical --open
```

Exit codes follow the batch convention: `0` ok, `1` partial, `2` failed.

---

## Data quality

Two independent suites, checking different things.

**`pytest` (233 tests)** — pure logic, no services needed:

```bash
python -m pytest
```

Covers ping validation and every dead-letter path, the great-circle and
polyline maths, H3 indexing, the simulator's invariants, the detector state
machine, model parsing and DAG resolution, SQL compilation, and the map
projection.

The detector tests are the ones worth reading. Because `detect()` is a pure
fold over enriched pings, the cases that are nearly impossible to provoke
against a live stream can be constructed directly: a stop that spans two
batches, an excursion interrupted by signal loss, a tracker with a clock that
runs backwards. Two properties are pinned explicitly — that reprocessing the
same pings produces identical detection keys, and that **splitting the batch
differently does not change the episodes found**, because otherwise every
alert count in the system would silently depend on `CONSUMER_BATCH_SIZE`.

**`python -m fleet test` (174 tests)** — assertions against the built
warehouse, declared in `models/**/schema.yml`:

| Test | Scope |
| --- | --- |
| `not_null`, `unique` | column |
| `accepted_values` | column |
| `relationships` | column → referenced model |
| `assert_expression` | model (a boolean every row must satisfy) |
| `unique_combination` | model (a compound grain is unique) |

Results land in `meta.test_results` and are surfaced by `status`. The sharpest
ones are the time-accounting identity in `int_ping_segments`, and
`fct_trips`' `is_on_time is null or is_complete` — which catches the classic
bug where the trip still in progress gets scored as late.

### Three things these caught during the build

Recorded because they are the kind of thing that otherwise ships.

**Alert counts were inflated by roughly the batch rate.** An episode that
opens in one batch and closes in the next is written twice, and the run
summary counted both — so the reported number of alerts depended on
`CONSUMER_BATCH_SIZE`, which is the sort of metric that is wrong in a
different way every day. Detections now carry whether the write is the first
the pipeline has heard of the episode.

**Rounding broke an additive identity.** `agg_h3_activity` stored moving,
stopped and observed seconds each rounded to one decimal place. Two halves
that both round up exceed a whole that rounds down: 12104.3 + 28221.8 =
40326.1 against an observed 40326.0. Components that have to add up are now
stored at the precision of their inputs; rounding is for display only.

**Dwell breaches all scored identically.** The breach fired the moment the
allowance ran out, so its magnitude was always the limit plus one reporting
interval — a vehicle two minutes over and one two hours over both scored
1.01×, and the severity column ranked nothing. Breaches are now episodes like
everything else: opened at the crossing, closed on exit with the dwell that
actually happened.

---

## Configuration

All settings live in `.env` (copy from `.env.example`). The ones that change
behaviour rather than just addresses:

| Variable | Default | Effect |
| --- | --- | --- |
| `IDLE_SPEED_KPH` | `3.0` | below this counts as stopped — set from GPS noise, not from zero |
| `IDLE_MINUTES` | `5` | how long a stop must last to be reported |
| `DEVIATION_METRES` | `120` | clears normal GPS error and a parallel service road |
| `DEVIATION_SECONDS` | `90` | sustain window; a single ping off route is noise |
| `DELAY_MINUTES` | `10` | schedule slip that opens a delay alert |
| `DELAY_RESTEP_MINUTES` | `10` | further slip before it re-alerts |
| `GPS_GAP_MINUTES` | `10` | beyond this, silence is lost signal rather than standing still |
| `H3_RESOLUTION_COARSE` / `_FINE` | `8` / `9` | grid resolutions stored per ping |
| `KAFKA_TOPIC_PARTITIONS` | `12` | caps consumer parallelism |
| `CONSUMER_BATCH_SIZE` | `500` | pings per transaction |
| `SIMULATOR_SEED` | *(unset)* | set for a reproducible stream |

Ports stay out of the way of the other projects in this directory: Postgres on
**5436**, Kafka on **9096**.

Fleet reference data — geofences, routes, vehicles — lives in
`config/fleet.json` and is validated hard on load. A zone whose ring is not
closed, a route that starts nowhere, a vehicle homed at a customer site, or
coordinates written `[lat, lon]` instead of `[lon, lat]` all fail there by
name, rather than surfacing much later as an empty join or a detection that
never fires. (The transposition check is on latitude specifically: Phnom
Penh's latitude is a perfectly legal longitude, so only the out-of-range
latitude gives the mistake away.)

---

## Layout

```
fleet/
  config.py        settings, fleet reference data, detection vocabulary
  geo.py           great-circle maths, polyline walking, H3 indexing
  pings.py         the telemetry contract and its validation
  simulator.py     synthetic movement with counted, injected incidents
  producer.py      topic provisioning and publishing
  enrich.py        the PostGIS batch enrichment query
  detector.py      the per-vehicle state machine (a pure fold)
  consumer.py      transactional batches: pings + detections + state, then offsets
  seed.py          ref.* loading and route-schedule derivation
  report.py        read-only queries behind the CLI
  mapviz.py        the self-contained SVG map
  transform/
    models.py      model parsing, DAG resolution, SQL compilation
    runner.py      materializations and execution
    tests.py       schema tests
models/
  staging/         stg_pings, stg_detections
  intermediate/    int_ping_segments
  marts/           dims, facts, aggregates
config/fleet.json  geofences, routes, vehicles
sql/001_schema.sql raw + ref + stream + meta (the runner owns stg and mart)
tests/             pytest suite
```

`meta` carries the run log throughout: `ingest_batches`, `model_runs`,
`test_results`. Every batch, model build and test outcome is queryable after
the fact rather than living only in stdout.

The `transform/` package is shared, near-verbatim, with the clickstream
project in `../project2` — it is genuinely project-agnostic, and the models
and thresholds are the only things that differ.

---

## Known limitations

- **Single-node Kafka, replication factor 1.** Fine locally; `acks=all` and
  idempotent producing are configured correctly for a real cluster, but the
  broker itself is not durable.

- **An hour can measure slightly over 3600 seconds.** A segment is attributed
  whole to the hour containing the ping that ends it, which is what stops
  hourly sums double-counting — the cost is that the first segment of an hour
  carries the silence before it. The overshoot is bounded by the signal-gap
  threshold and is asserted as such; the observed maximum here was 3667 s
  against a 75-second parked reporting cadence. Splitting segments at hour
  boundaries would fix it and would cost `int_ping_segments` its one-row-per-
  ping grain.

- **Progress along a route is measured planar.** `ST_LineLocatePoint` has no
  geography form, so it works on the geometry cast — degrees, not metres.
  Because `ref.route_schedule` is built with the same function, the distortion
  is identical on both sides of every delay comparison and cancels; it would
  matter if the fraction were used for anything else.

- **Delay is measured from `planned_start_at`.** A vehicle that departs late
  is behind schedule for the whole trip, which is correct for schedule
  adherence and wrong if you wanted to ask how well it drove once it left.

- **Incremental models do not observe upstream deletes.** Truncating
  `raw.pings` or `stream.detections` leaves the previous rows in `stg_*`,
  which then coexist with the reload. Run `transform --full-refresh` after
  clearing a source. (This is true of dbt too, but it is easy to trip over.)

- **Dead letters deduplicate on Kafka coordinates, not content.** A message
  that failed validation has no trustworthy identity inside it — the worst
  ones are not even JSON — so `(topic, partition, offset)` is used instead.
  That makes replay idempotent, and it means the same bad payload republished
  to a new offset is recorded twice, correctly.

- **The map has no basemap**, by design; see above. It also draws only the
  most recent `--hours` of track, thinned per vehicle.

- **The simulator deliberately drives vehicles into the restricted precinct.**
  About half of injected deviations are aimed at a restricted zone the route
  passes near. A detection path that only ever fires by accident is a
  detection path that mostly goes untested.
