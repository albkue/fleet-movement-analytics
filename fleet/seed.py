"""Load the fleet reference data into ref.*.

Geofences and routes are handed to PostGIS as WKT and stored as `geography`,
so every distance the pipeline reports is in metres on the spheroid rather
than in degrees.

The one derived thing built here is `ref.route_schedule`: the planned
departure time at each point along a route, which is what "delayed" is
measured against. It is computed in SQL with `ST_LineLocatePoint` -- the same
function the per-ping enrichment uses to measure progress -- so the schedule
and the measurement against it are consistent by construction. Both work on
the geometry cast of the route, which is planar in degrees; the resulting
distortion is identical on both sides of the comparison and therefore cancels.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Sequence

import psycopg

from .config import FleetConfig, Settings
from .geo import to_wkt_linestring, to_wkt_polygon
from .simulator import SimulatedTrip

log = logging.getLogger(__name__)


@dataclass
class SeedSummary:
    zones: int = 0
    routes: int = 0
    vehicles: int = 0
    schedule_points: int = 0
    trips: int = 0


# Rebuilt from scratch on every seed. It is a pure function of the routes,
# their stops and their planned durations, so there is nothing in it worth
# preserving across a reload -- and a stale checkpoint would silently shift
# every delay measurement.
_SCHEDULE_SQL = """
WITH stop_fraction AS (
    SELECT r.route_id,
           s.zone_id,
           ST_LineLocatePoint(
               r.path::geometry,
               ST_Centroid(z.boundary::geometry)
           ) AS fraction
    FROM ref.routes r
    CROSS JOIN LATERAL unnest(r.stop_zone_ids) AS s(zone_id)
    JOIN ref.zones z ON z.zone_id = s.zone_id
),
ordered AS (
    -- A stop that locates to the very start or end of the route is dropped:
    -- it would create a zero-length schedule segment and, more to the point,
    -- it means the stop is the depot rather than something served en route.
    SELECT route_id,
           zone_id,
           fraction,
           row_number() OVER (PARTITION BY route_id
                              ORDER BY fraction, zone_id) AS stop_no,
           count(*)     OVER (PARTITION BY route_id)      AS stop_count
    FROM stop_fraction
    WHERE fraction > 0 AND fraction < 1
),
plan AS (
    SELECT r.route_id,
           r.planned_duration_minutes * 60         AS total_s,
           r.service_minutes * 60                  AS service_s,
           coalesce(max(o.stop_count), 0)::int     AS stops
    FROM ref.routes r
    LEFT JOIN ordered o ON o.route_id = r.route_id
    GROUP BY r.route_id, r.planned_duration_minutes, r.service_minutes
),
points AS (
    SELECT route_id, 0 AS seq, 0.0::double precision AS fraction,
           0 AS elapsed_seconds, NULL::text AS zone_id
    FROM plan

    UNION ALL

    -- Departure from each stop: the driving time to get there, plus every
    -- service allowance consumed up to and including this one.
    SELECT o.route_id,
           o.stop_no::int,
           o.fraction,
           round((p.total_s - p.service_s * p.stops) * o.fraction
                 + p.service_s * o.stop_no)::int,
           o.zone_id
    FROM ordered o
    JOIN plan p USING (route_id)

    UNION ALL

    SELECT p.route_id, p.stops + 1, 1.0, p.total_s, NULL
    FROM plan p
)
INSERT INTO ref.route_schedule (route_id, seq, fraction, elapsed_seconds, zone_id)
SELECT route_id, seq, fraction, elapsed_seconds, zone_id
FROM points
"""


def _seed_zones(cur: psycopg.Cursor, config: FleetConfig) -> int:
    cur.executemany(
        """
        INSERT INTO ref.zones
            (zone_id, name, zone_kind, max_dwell_minutes, boundary)
        VALUES (%s, %s, %s, %s, ST_GeogFromText(%s))
        ON CONFLICT (zone_id) DO UPDATE SET
            name              = EXCLUDED.name,
            zone_kind         = EXCLUDED.zone_kind,
            max_dwell_minutes = EXCLUDED.max_dwell_minutes,
            boundary          = EXCLUDED.boundary,
            loaded_at         = now()
        """,
        [
            (
                zone.zone_id,
                zone.name,
                zone.zone_kind,
                zone.max_dwell_minutes,
                f"SRID=4326;{to_wkt_polygon(zone.boundary)}",
            )
            for zone in config.zones
        ],
    )
    return len(config.zones)


def _seed_routes(cur: psycopg.Cursor, config: FleetConfig) -> int:
    cur.executemany(
        """
        INSERT INTO ref.routes
            (route_id, name, start_zone_id, end_zone_id,
             planned_duration_minutes, service_minutes, stop_zone_ids, path)
        VALUES (%s, %s, %s, %s, %s, %s, %s, ST_GeogFromText(%s))
        ON CONFLICT (route_id) DO UPDATE SET
            name                     = EXCLUDED.name,
            start_zone_id            = EXCLUDED.start_zone_id,
            end_zone_id              = EXCLUDED.end_zone_id,
            planned_duration_minutes = EXCLUDED.planned_duration_minutes,
            service_minutes          = EXCLUDED.service_minutes,
            stop_zone_ids            = EXCLUDED.stop_zone_ids,
            path                     = EXCLUDED.path,
            loaded_at                = now()
        """,
        [
            (
                route.route_id,
                route.name,
                route.start_zone_id,
                route.end_zone_id,
                route.planned_duration_minutes,
                route.service_minutes,
                list(route.stop_zone_ids),
                f"SRID=4326;{to_wkt_linestring(route.path)}",
            )
            for route in config.routes
        ],
    )
    return len(config.routes)


def _seed_vehicles(cur: psycopg.Cursor, config: FleetConfig) -> int:
    cur.executemany(
        """
        INSERT INTO ref.vehicles
            (vehicle_id, plate, vehicle_type, capacity_kg, home_depot_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (vehicle_id) DO UPDATE SET
            plate         = EXCLUDED.plate,
            vehicle_type  = EXCLUDED.vehicle_type,
            capacity_kg   = EXCLUDED.capacity_kg,
            home_depot_id = EXCLUDED.home_depot_id,
            loaded_at     = now()
        """,
        [
            (
                vehicle.vehicle_id,
                vehicle.plate,
                vehicle.vehicle_type,
                vehicle.capacity_kg,
                vehicle.home_depot_id,
            )
            for vehicle in config.vehicles
        ],
    )
    return len(config.vehicles)


def seed_reference(settings: Settings, config: FleetConfig) -> SeedSummary:
    """Load zones, routes and vehicles, then rebuild the route schedules."""
    summary = SeedSummary()
    with psycopg.connect(settings.dsn) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                summary.zones = _seed_zones(cur, config)
                summary.routes = _seed_routes(cur, config)
                summary.vehicles = _seed_vehicles(cur, config)

                cur.execute("DELETE FROM ref.route_schedule")
                cur.execute(_SCHEDULE_SQL)
                summary.schedule_points = cur.rowcount

    log.info(
        "seeded %d zone(s), %d route(s), %d vehicle(s), %d schedule point(s)",
        summary.zones,
        summary.routes,
        summary.vehicles,
        summary.schedule_points,
    )
    return summary


def seed_trips(settings: Settings, trips: Sequence[SimulatedTrip]) -> int:
    """Load the planned schedule produced by the simulator into ref.trips.

    Trips are reference data, not telemetry: they are the *plan*, and they
    exist before any vehicle moves. Publishing them through Kafka alongside
    the pings would be modelling a dispatch system as a sensor.
    """
    if not trips:
        return 0

    with psycopg.connect(settings.dsn) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO ref.trips
                        (trip_id, vehicle_id, route_id,
                         planned_start_at, planned_end_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id) DO UPDATE SET
                        vehicle_id       = EXCLUDED.vehicle_id,
                        route_id         = EXCLUDED.route_id,
                        planned_start_at = EXCLUDED.planned_start_at,
                        planned_end_at   = EXCLUDED.planned_end_at,
                        loaded_at        = now()
                    """,
                    [
                        (
                            trip.trip_id,
                            trip.vehicle_id,
                            trip.route_id,
                            trip.planned_start_at,
                            trip.planned_end_at,
                        )
                        for trip in trips
                    ],
                )
    return len(trips)


def reference_counts(settings: Settings) -> dict[str, int]:
    """Row counts for the ref.* tables, for `status`."""
    tables: Iterable[str] = (
        "ref.zones",
        "ref.routes",
        "ref.route_schedule",
        "ref.vehicles",
        "ref.trips",
    )
    out: dict[str, int] = {}
    with psycopg.connect(settings.dsn) as conn:
        for table in tables:
            row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
            out[table] = int(row[0])
    return out
