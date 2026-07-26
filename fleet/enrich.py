"""Batch geospatial enrichment, done in PostGIS.

The division of labour in this pipeline is deliberate:

    PostGIS answers questions about *space*  -- which geofences contain this
                                                point, how far is it from the
                                                assigned route, how far along
                                                that route has it got, and
                                                therefore how it compares to
                                                the planned schedule;

    Python answers questions about *time*    -- has it been stopped long
                                                enough to count, is this the
                                                same excursion as last ping's
                                                or a new one, when did this
                                                zone visit start.

Doing the spatial half in Python would mean shipping every polygon to the
client and re-implementing point-in-polygon and distance-to-line badly. Doing
the temporal half in SQL would mean expressing a state machine that spans
batches as a window function, which is where this kind of pipeline usually
goes wrong.

So one set-based query per batch enriches every ping at once, and the state
machine folds over the result in vehicle/time order.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

# Enrichment is scoped to a single batch_id rather than to a time window, and
# that is what makes reprocessing safe. A replayed message collides with
# raw.pings' primary key and is dropped, so it never carries the current
# batch_id, so it is never enriched or fed to the detector a second time.
_ENRICH_SQL = """
WITH batch AS (
    SELECT p.ping_id,
           p.vehicle_id,
           p.trip_id,
           p.recorded_at,
           p.lat,
           p.lon,
           p.location,
           p.speed_kph,
           p.ignition,
           p.odometer_km
    FROM raw.pings p
    WHERE p.batch_id = %(batch_id)s
),
zoned AS (
    -- One row per ping carrying every zone that contains it. Zones may
    -- overlap (a customer site inside a congestion corridor), so this is an
    -- array and not a column.
    SELECT b.ping_id,
           coalesce(
               array_agg(z.zone_id ORDER BY z.zone_id)
                   FILTER (WHERE z.zone_id IS NOT NULL),
               ARRAY[]::text[]
           ) AS zone_ids
    FROM batch b
    -- ST_Covers rather than ST_Contains: a vehicle parked exactly on a
    -- depot's boundary line is in the depot.
    LEFT JOIN ref.zones z ON ST_Covers(z.boundary, b.location)
    GROUP BY b.ping_id
),
routed AS (
    SELECT b.ping_id,
           t.route_id,
           t.planned_start_at,
           t.planned_end_at,
           -- Metres, on the spheroid, because both operands are geography.
           ST_Distance(b.location, r.path) AS route_distance_m,
           -- Progress along the route as a 0..1 fraction. This is the planar
           -- geometry variant; ref.route_schedule was built with the same
           -- function, so the schedule and the measurement against it share
           -- whatever distortion it has.
           ST_LineLocatePoint(r.path::geometry, b.location::geometry)
               AS route_fraction
    FROM batch b
    JOIN ref.trips t  ON t.trip_id = b.trip_id
    JOIN ref.routes r ON r.route_id = t.route_id
)
SELECT b.ping_id,
       b.vehicle_id,
       b.trip_id,
       b.recorded_at,
       b.lat,
       b.lon,
       b.speed_kph::double precision  AS speed_kph,
       b.ignition,
       b.odometer_km::double precision AS odometer_km,
       z.zone_ids,
       r.route_id,
       r.route_distance_m,
       r.route_fraction,
       s.expected_elapsed_s,
       CASE
           WHEN s.expected_elapsed_s IS NULL THEN NULL
           ELSE extract(epoch FROM (b.recorded_at - r.planned_start_at))
                - s.expected_elapsed_s
       END AS delay_seconds
FROM batch b
JOIN zoned z USING (ping_id)
LEFT JOIN routed r USING (ping_id)
LEFT JOIN LATERAL (
    -- Planned elapsed time at this point on the route, linearly interpolated
    -- between the two schedule checkpoints that bracket it.
    SELECT lo.elapsed_seconds
           + (hi.elapsed_seconds - lo.elapsed_seconds)
             * (r.route_fraction - lo.fraction)
             / nullif(hi.fraction - lo.fraction, 0) AS expected_elapsed_s
    FROM ref.route_schedule lo
    JOIN ref.route_schedule hi
      ON hi.route_id = lo.route_id AND hi.seq = lo.seq + 1
    WHERE lo.route_id = r.route_id
      AND r.route_fraction >= lo.fraction
      AND r.route_fraction <= hi.fraction
    ORDER BY lo.seq
    LIMIT 1
) s ON true
-- The detector folds over this in order, and the fold is per vehicle, so the
-- ordering is part of the contract rather than a nicety. ping_id breaks ties
-- between two fixes sharing a timestamp so the result is deterministic.
ORDER BY b.vehicle_id, b.recorded_at, b.ping_id
"""


@dataclass(frozen=True)
class EnrichedPing:
    """One ping with everything spatial already answered."""

    ping_id: UUID
    vehicle_id: str
    trip_id: str | None
    recorded_at: datetime
    lat: float
    lon: float
    speed_kph: float
    ignition: bool
    odometer_km: float | None
    # Every zone containing this ping. Zone attributes (kind, dwell limit)
    # are not repeated per ping -- the detector is handed the zone catalogue
    # once, because it is the same eight rows for every ping in the stream.
    zone_ids: tuple[str, ...]
    route_id: str | None
    route_distance_m: float | None
    route_fraction: float | None
    # Seconds behind (positive) or ahead of (negative) the planned schedule.
    delay_seconds: float | None

    @property
    def position(self) -> tuple[float, float]:
        return self.lon, self.lat


def _row_to_ping(row: dict[str, Any]) -> EnrichedPing:
    return EnrichedPing(
        ping_id=row["ping_id"],
        vehicle_id=row["vehicle_id"],
        trip_id=row["trip_id"],
        recorded_at=row["recorded_at"],
        lat=float(row["lat"]),
        lon=float(row["lon"]),
        speed_kph=float(row["speed_kph"]),
        ignition=bool(row["ignition"]),
        odometer_km=None if row["odometer_km"] is None else float(row["odometer_km"]),
        zone_ids=tuple(row["zone_ids"] or ()),
        route_id=row["route_id"],
        route_distance_m=(
            None
            if row["route_distance_m"] is None
            else float(row["route_distance_m"])
        ),
        route_fraction=(
            None if row["route_fraction"] is None else float(row["route_fraction"])
        ),
        delay_seconds=(
            None if row["delay_seconds"] is None else float(row["delay_seconds"])
        ),
    )


def enrich_batch(cur: psycopg.Cursor, batch_id: int) -> list[EnrichedPing]:
    """Enrich every ping in one batch, ordered by vehicle then time.

    Takes a cursor rather than a connection on purpose: this runs inside the
    consumer's open transaction, alongside the writes it will produce, so
    that pings, detections and vehicle state all commit or all do not.
    """
    with cur.connection.cursor(row_factory=dict_row) as dict_cur:
        dict_cur.execute(_ENRICH_SQL, {"batch_id": batch_id})
        return [_row_to_ping(row) for row in dict_cur]
