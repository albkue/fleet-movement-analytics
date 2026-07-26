"""Read-only queries behind the CLI.

Every function here takes an open connection and returns plain rows. Nothing
in this module writes, and nothing in it computes a business number that is
not already computed by a model -- if a figure is worth showing it is worth
being in the warehouse where a schema test can be pointed at it.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

# Relations shown by `status`, in the order the data flows through them.
TRACKED_RELATIONS: tuple[str, ...] = (
    "raw.pings",
    "raw.pings_dead_letter",
    "stream.detections",
    "stream.vehicle_state",
    "stg.stg_pings",
    "stg.stg_detections",
    "stg.int_ping_segments",
    "mart.dim_vehicle",
    "mart.dim_zone",
    "mart.dim_route",
    "mart.fct_alerts",
    "mart.fct_trips",
    "mart.fct_vehicle_hours",
    "mart.agg_daily_fleet",
    "mart.agg_zone_activity",
    "mart.agg_h3_activity",
)


def _rows(conn: psycopg.Connection, sql: str, params: Any = None) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return list(cur)


def _row(conn: psycopg.Connection, sql: str, params: Any = None) -> dict | None:
    rows = _rows(conn, sql, params)
    return rows[0] if rows else None


# ------------------------------------------------------------- ingestion ----


def ingest_overview(conn: psycopg.Connection) -> dict:
    return _row(
        conn,
        """
        SELECT count(*)                       AS pings,
               count(DISTINCT vehicle_id)     AS vehicles,
               count(DISTINCT trip_id)        AS trips,
               min(recorded_at)               AS first_ping_at,
               max(recorded_at)               AS last_ping_at,
               max(ingested_at)               AS last_ingested_at,
               (SELECT count(*) FROM raw.pings_dead_letter) AS dead_letters
        FROM raw.pings
        """,
    ) or {}


def dead_letter_reasons(conn: psycopg.Connection, limit: int = 8) -> list[dict]:
    """Why messages were rejected, most common first.

    Grouped on the leading fragment of the message rather than the whole
    thing: the errors carry the offending value, so ungrouped they would be
    one row each and tell you nothing about which fault is dominant.
    """
    return _rows(
        conn,
        """
        SELECT split_part(error, ',', 1) AS reason,
               count(*)                  AS messages,
               max(rejected_at)          AS last_seen_at
        FROM raw.pings_dead_letter
        GROUP BY 1
        ORDER BY messages DESC
        LIMIT %s
        """,
        (limit,),
    )


def recent_batches(conn: psycopg.Connection, limit: int = 5) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT batch_id, status, started_at, finished_at, messages_read,
               rows_inserted, rows_duplicate, rows_rejected, detections_found,
               error
        FROM meta.ingest_batches
        ORDER BY batch_id DESC
        LIMIT %s
        """,
        (limit,),
    )


def table_counts(conn: psycopg.Connection) -> list[tuple[str, int | None]]:
    out: list[tuple[str, int | None]] = []
    for relation in TRACKED_RELATIONS:
        exists = conn.execute(
            "SELECT to_regclass(%s) IS NOT NULL", (relation,)
        ).fetchone()[0]
        if not exists:
            out.append((relation, None))
            continue
        count = conn.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
        out.append((relation, int(count)))
    return out


def open_episodes(conn: psycopg.Connection) -> list[dict]:
    """Episodes that were still running as at the last batch processed."""
    return _rows(
        conn,
        """
        SELECT detection_type, severity, vehicle_id, zone_id, started_at,
               magnitude
        FROM stream.detections
        WHERE ended_at IS NULL
        ORDER BY started_at
        """,
    )


# ---------------------------------------------------------------- alerts ----


def alerts(
    conn: psycopg.Connection,
    *,
    limit: int = 25,
    detection_type: str | None = None,
    severity: str | None = None,
    vehicle_id: str | None = None,
    open_only: bool = False,
) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT detection_type, severity, severity_rank, is_open, vehicle_id,
               plate, vehicle_type, trip_id, route_name, zone_id, zone_name,
               zone_kind, started_at, ended_at, duration_minutes, magnitude,
               threshold_multiple, lat, lon, details
        FROM mart.fct_alerts
        -- The casts are load-bearing: a parameter that only ever appears in
        -- an IS NULL test and an equality against a text column gives the
        -- planner nothing to infer a type from, and it refuses the statement
        -- rather than guessing.
        WHERE (%(detection_type)s::text IS NULL
               OR detection_type = %(detection_type)s::text)
          AND (%(severity)s::text   IS NULL OR severity   = %(severity)s::text)
          AND (%(vehicle_id)s::text IS NULL OR vehicle_id = %(vehicle_id)s::text)
          AND (NOT %(open_only)s::boolean OR is_open)
        ORDER BY severity_rank DESC,
                 coalesce(threshold_multiple, 0) DESC,
                 started_at DESC
        LIMIT %(limit)s
        """,
        {
            "detection_type": detection_type,
            "severity": severity,
            "vehicle_id": vehicle_id,
            "open_only": open_only,
            "limit": limit,
        },
    )


def alert_summary(conn: psycopg.Connection) -> list[dict]:
    """Per-type counts, summarised by magnitude rather than duration.

    Duration is the wrong summary for half of these. A delay and a restricted
    zone entry are instants -- they have a duration of zero by construction --
    so a table of average durations would report the two most actionable
    detection types as 0.0 and invite the reader to conclude they were minor.
    Magnitude is defined for every type; its unit is the type's own
    (minutes idle, metres off route, minutes late), which is why the unit is
    reported alongside it rather than assumed.
    """
    return _rows(
        conn,
        """
        SELECT detection_type,
               count(*)                                      AS episodes,
               count(*) FILTER (WHERE severity = 'critical') AS critical,
               count(*) FILTER (WHERE severity = 'warning')  AS warning,
               count(*) FILTER (WHERE severity = 'info')     AS info,
               count(*) FILTER (WHERE is_open)               AS still_open,
               round(avg(magnitude), 1)                      AS avg_magnitude,
               round(max(magnitude), 1)                      AS max_magnitude,
               round(max(threshold_multiple), 1)             AS worst_multiple,
               count(DISTINCT vehicle_id)                    AS vehicles
        FROM mart.fct_alerts
        GROUP BY detection_type
        ORDER BY critical DESC, episodes DESC
        """,
    )


# The unit `magnitude` is measured in, per detection type. Reported next to
# the number because a column of bare magnitudes mixing minutes and metres is
# worse than no column at all.
MAGNITUDE_UNITS: dict[str, str] = {
    "idle": "min",
    "zone_visit": "min",
    "geofence_breach": "min",
    "route_deviation": "m",
    "delay": "min",
    "gps_gap": "min",
}


# ----------------------------------------------------------------- trips ----


def trips(
    conn: psycopg.Connection, *, limit: int = 20, vehicle_id: str | None = None
) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT trip_id, vehicle_id, plate, vehicle_type, route_name,
               planned_start_at, planned_end_at, actual_start_at, actual_end_at,
               distance_km, route_length_km, route_coverage_pct,
               actual_duration_minutes, planned_duration_minutes,
               finish_delay_minutes, moving_share_pct, avg_moving_kph,
               max_speed_kph, alerts, critical_alerts, is_complete, is_on_time,
               has_telemetry
        FROM mart.fct_trips
        -- Cast for the same reason as in alerts(): a parameter used only in
        -- an IS NULL test and a text equality gives the planner no type to
        -- infer, and it refuses rather than guessing.
        WHERE (%(vehicle_id)s::text IS NULL OR vehicle_id = %(vehicle_id)s::text)
        ORDER BY planned_start_at DESC
        LIMIT %(limit)s
        """,
        {"vehicle_id": vehicle_id, "limit": limit},
    )


def trip_punctuality(conn: psycopg.Connection) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT vehicle_type,
               count(*)                                  AS trips,
               count(*) FILTER (WHERE is_complete)       AS completed,
               count(*) FILTER (WHERE is_on_time)        AS on_time,
               round(100.0 * count(*) FILTER (WHERE is_on_time)
                     / nullif(count(*) FILTER (WHERE is_complete), 0), 1)
                                                         AS on_time_pct,
               round(avg(finish_delay_minutes) FILTER (WHERE is_complete), 1)
                                                         AS avg_delay_minutes,
               round(sum(distance_km), 1)                AS distance_km
        FROM mart.fct_trips
        GROUP BY vehicle_type
        ORDER BY vehicle_type
        """,
    )


# -------------------------------------------------------------- vehicles ----


def vehicles(conn: psycopg.Connection) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT v.vehicle_id, v.plate, v.vehicle_type, v.home_depot_name,
               v.pings, v.trips, v.distance_km,
               round(v.moving_seconds / 3600.0, 2)   AS moving_hours,
               round(v.stopped_seconds / 3600.0, 2)  AS stopped_hours,
               round(v.idling_engine_seconds / 60.0, 1) AS idling_minutes,
               v.max_speed_kph,
               round(100.0 * v.moving_seconds
                     / nullif(v.moving_seconds + v.stopped_seconds, 0), 1)
                                                     AS utilisation_pct,
               v.has_reported,
               coalesce(a.alerts, 0)                 AS alerts,
               coalesce(a.critical, 0)               AS critical_alerts
        FROM mart.dim_vehicle v
        LEFT JOIN (
            SELECT vehicle_id,
                   count(*) AS alerts,
                   count(*) FILTER (WHERE severity = 'critical') AS critical
            FROM mart.fct_alerts
            GROUP BY vehicle_id
        ) a ON a.vehicle_id = v.vehicle_id
        ORDER BY v.distance_km DESC, v.vehicle_id
        """,
    )


# ----------------------------------------------------------------- zones ----


def zone_activity(conn: psycopg.Connection, *, days: int = 7) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT zone_id,
               max(zone_name)                AS zone_name,
               max(zone_kind)                AS zone_kind,
               max(max_dwell_minutes)        AS max_dwell_minutes,
               round(max(area_km2), 3)       AS area_km2,
               sum(visits)                   AS visits,
               sum(closed_visits)            AS closed_visits,
               max(vehicles)                 AS peak_vehicles,
               round(sum(dwell_hours), 2)    AS dwell_hours,
               round(avg(avg_dwell_minutes), 1) AS avg_dwell_minutes,
               max(max_dwell_minutes_observed) AS max_dwell_observed,
               sum(breaches)                 AS breaches,
               sum(entry_breaches)           AS entry_breaches,
               sum(dwell_breaches)           AS dwell_breaches
        FROM mart.agg_zone_activity
        WHERE activity_date >= current_date - %s::int
        GROUP BY zone_id
        ORDER BY sum(breaches) DESC, sum(visits) DESC
        """,
        (days,),
    )


def hotspots(conn: psycopg.Connection, *, limit: int = 12) -> list[dict]:
    """The H3 cells where the fleet spends the most stopped time.

    Ordered by stopped time rather than ping count: the busiest cell is
    usually a depot, which nobody needs a heatmap to find. Where vehicles sit
    still *and* many different vehicles do it is where the congestion is.
    """
    return _rows(
        conn,
        """
        SELECT h3_r8,
               sum(pings)                    AS pings,
               max(vehicles)                 AS vehicles,
               round(sum(stopped_seconds) / 3600.0, 2) AS stopped_hours,
               round(sum(moving_seconds) / 3600.0, 2)  AS moving_hours,
               round(avg(avg_moving_kph), 1) AS avg_moving_kph,
               round(avg(stopped_share_pct), 1) AS stopped_share_pct,
               round(avg(mean_lat)::numeric, 5) AS mean_lat,
               round(avg(mean_lon)::numeric, 5) AS mean_lon
        FROM mart.agg_h3_activity
        GROUP BY h3_r8
        ORDER BY sum(stopped_seconds) DESC
        LIMIT %s
        """,
        (limit,),
    )


def daily_fleet(conn: psycopg.Connection, *, days: int = 7) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT activity_date, vehicle_type, active_vehicles, pings, distance_km,
               utilisation_pct, coverage_pct, engine_idle_share_pct,
               trips_planned, trips_completed, on_time_pct,
               avg_finish_delay_minutes, alerts, critical_alerts,
               alerts_per_100km
        FROM mart.agg_daily_fleet
        WHERE activity_date >= current_date - %s::int
        ORDER BY activity_date DESC, vehicle_type
        """,
        (days,),
    )


# ------------------------------------------------------------- run state ----


def last_transform_run(conn: psycopg.Connection) -> dict | None:
    return _row(
        conn,
        """
        SELECT run_id,
               min(started_at)                            AS started_at,
               count(*)                                   AS models,
               count(*) FILTER (WHERE status = 'success') AS succeeded,
               count(*) FILTER (WHERE status = 'failed')  AS failed,
               count(*) FILTER (WHERE status = 'skipped') AS skipped
        FROM meta.model_runs
        GROUP BY run_id
        ORDER BY min(started_at) DESC
        LIMIT 1
        """,
    )


def last_test_run(conn: psycopg.Connection) -> dict | None:
    return _row(
        conn,
        """
        SELECT run_id,
               max(executed_at)                          AS executed_at,
               count(*)                                  AS tests,
               count(*) FILTER (WHERE status = 'pass')   AS passed,
               count(*) FILTER (WHERE status = 'fail')   AS failed,
               count(*) FILTER (WHERE status = 'error')  AS errored
        FROM meta.test_results
        GROUP BY run_id
        ORDER BY max(executed_at) DESC
        LIMIT 1
        """,
    )


def failing_tests(conn: psycopg.Connection, run_id: str) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT model_name, column_name, test_name, status, failing_rows, error
        FROM meta.test_results
        WHERE run_id = %s AND status <> 'pass'
        ORDER BY model_name, test_name
        """,
        (run_id,),
    )


# ------------------------------------------------------------------ map ----


def map_zones(conn: psycopg.Connection) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT z.zone_id, z.name, z.zone_kind, z.boundary_geojson,
               z.centre_lat, z.centre_lon,
               coalesce(a.visits, 0)   AS visits,
               coalesce(a.breaches, 0) AS breaches
        FROM mart.dim_zone z
        LEFT JOIN (
            SELECT zone_id, sum(visits) AS visits, sum(breaches) AS breaches
            FROM mart.agg_zone_activity
            GROUP BY zone_id
        ) a ON a.zone_id = z.zone_id
        ORDER BY z.zone_id
        """,
    )


def map_routes(conn: psycopg.Connection) -> list[dict]:
    return _rows(
        conn,
        "SELECT route_id, name, path_geojson, length_km FROM mart.dim_route "
        "ORDER BY route_id",
    )


def map_tracks(
    conn: psycopg.Connection, *, hours: int = 6, max_points: int = 400
) -> list[dict]:
    """Recent track per vehicle, thinned to at most `max_points` positions.

    Thinned with a modulo on a per-vehicle row number rather than by taking
    the newest N: dropping the tail would silently redraw every track as
    starting wherever the cut fell, which on a map looks exactly like a
    vehicle that appeared out of nowhere.
    """
    return _rows(
        conn,
        """
        WITH windowed AS (
            SELECT vehicle_id, lat, lon, recorded_at,
                   row_number() OVER (PARTITION BY vehicle_id
                                      ORDER BY recorded_at) AS seq,
                   count(*)     OVER (PARTITION BY vehicle_id) AS total
            FROM stg.stg_pings
            WHERE recorded_at >= (SELECT max(recorded_at) FROM stg.stg_pings)
                                 - make_interval(hours => %(hours)s)
        )
        SELECT w.vehicle_id,
               v.plate,
               v.vehicle_type,
               array_agg(w.lon ORDER BY w.recorded_at) AS lons,
               array_agg(w.lat ORDER BY w.recorded_at) AS lats
        FROM windowed w
        JOIN mart.dim_vehicle v ON v.vehicle_id = w.vehicle_id
        WHERE w.seq %% greatest(1, (w.total / %(max_points)s)::int + 1) = 0
        GROUP BY w.vehicle_id, v.plate, v.vehicle_type
        ORDER BY w.vehicle_id
        """,
        {"hours": hours, "max_points": max_points},
    )


def map_alerts(conn: psycopg.Connection, *, hours: int = 6) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT detection_type, severity, vehicle_id, plate, zone_name,
               started_at, duration_minutes, magnitude, lat, lon, is_open
        FROM mart.fct_alerts
        WHERE lat IS NOT NULL
          AND detection_type <> 'zone_visit'
          AND started_at >= (SELECT max(started_at) FROM mart.fct_alerts)
                            - make_interval(hours => %s)
        ORDER BY severity_rank DESC, started_at DESC
        """,
        (hours,),
    )


def map_cells(conn: psycopg.Connection, *, limit: int = 600) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT h3_r8,
               sum(pings)                              AS pings,
               max(vehicles)                           AS vehicles,
               round(sum(stopped_seconds) / 3600.0, 2) AS stopped_hours,
               round(avg(avg_moving_kph), 1)           AS avg_moving_kph
        FROM mart.agg_h3_activity
        GROUP BY h3_r8
        ORDER BY sum(pings) DESC
        LIMIT %s
        """,
        (limit,),
    )
