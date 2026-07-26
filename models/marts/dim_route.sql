{{ config(
    materialized='table',
    description='One row per route, with its measured length and planned pace.',
    indexes=['route_id']
) }}

-- Grain: one row per route in ref.routes.
--
-- `planned_avg_kph` is deliberately computed against *driving* time rather
-- than the whole planned duration. A 90-minute airport shuttle that spends
-- 20 of those minutes on a loading bay is not planned to average
-- length/90min; quoting that figure makes every route look slower than it is
-- planned to be, and makes routes with more stops look worst of all.

with stops as (

    select
        route_id,
        count(*) filter (where zone_id is not null) as scheduled_stops
    from {{ source('ref', 'route_schedule') }}
    group by route_id

)

select
    r.route_id,
    r.name,
    r.start_zone_id,
    r.end_zone_id,
    r.planned_duration_minutes,
    r.service_minutes,
    coalesce(s.scheduled_stops, 0)                       as scheduled_stops,

    round((ST_Length(r.path) / 1000)::numeric, 3)        as length_km,

    r.planned_duration_minutes
      - r.service_minutes * coalesce(s.scheduled_stops, 0) as planned_drive_minutes,

    -- The cast wraps the whole quotient. ST_Length returns double precision,
    -- and double / numeric resolves to double, so casting only the divisor
    -- leaves round() without a two-argument form to call.
    round(
        (
            (ST_Length(r.path) / 1000)
            / nullif(
                (r.planned_duration_minutes
                 - r.service_minutes * coalesce(s.scheduled_stops, 0)) / 60.0,
                0
            )
        )::numeric,
        1
    )                                                    as planned_avg_kph,

    ST_AsGeoJSON(r.path::geometry)                       as path_geojson,
    r.loaded_at

from {{ source('ref', 'routes') }} r
left join stops s on s.route_id = r.route_id
