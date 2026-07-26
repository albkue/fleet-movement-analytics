{{ config(
    materialized='table',
    description='One row per assigned trip: plan versus what the vehicle actually did.',
    indexes=[
        'trip_id',
        'vehicle_id, planned_start_at',
        'route_id',
        'planned_date'
    ]
) }}

-- Grain: one row per row of ref.trips. The central fact of the project --
-- everything else is either an input to it or a rollup of it.
--
-- **The plan is the spine.** A trip that was dispatched and never reported a
-- single ping still gets a row, with nulls where the actuals would be. That
-- is the row an operations team most needs to see, and building this from
-- the telemetry side instead would silently omit it.
--
-- **The newest trip per vehicle is excluded from completion statistics.**
-- It may still be running: its last ping is the last ping, not the end of
-- the work. Counting it as a finished trip would drag every duration and
-- distance average downwards by exactly one truncated trip per vehicle, for
-- ever, in a way that looks like a real signal.

with actual as (

    select
        trip_id,
        count(*)                        as pings,
        min(recorded_at)                as actual_start_at,
        max(recorded_at)                as actual_end_at,
        sum(travelled_m) / 1000.0       as distance_km,
        sum(moving_seconds)             as moving_seconds,
        sum(stopped_seconds)            as stopped_seconds,
        sum(unobserved_seconds)         as unobserved_seconds,
        sum(engine_on_seconds)          as engine_on_seconds,
        max(speed_kph)                  as max_speed_kph,
        avg(implied_speed_kph) filter (where is_moving) as avg_moving_kph
    from {{ ref('int_ping_segments') }}
    where trip_id is not null
    group by trip_id

),

alerts as (

    select
        trip_id,
        count(*)                                                   as alerts,
        count(*) filter (where detection_type = 'idle')            as idle_alerts,
        count(*) filter (where detection_type = 'route_deviation') as deviation_alerts,
        count(*) filter (where detection_type = 'delay')           as delay_alerts,
        count(*) filter (where detection_type = 'geofence_breach') as breach_alerts,
        count(*) filter (where detection_type = 'gps_gap')         as gap_alerts,
        count(*) filter (where severity = 'critical')              as critical_alerts,
        max(magnitude) filter (where detection_type = 'delay')     as worst_delay_minutes,
        max(magnitude) filter (where detection_type = 'route_deviation')
                                                                   as worst_deviation_m
    from {{ ref('fct_alerts') }}
    where trip_id is not null
    group by trip_id

),

ranked as (

    select
        t.trip_id,
        t.vehicle_id,
        t.route_id,
        t.planned_start_at,
        t.planned_end_at,
        row_number() over (
            partition by t.vehicle_id order by t.planned_start_at desc, t.trip_id desc
        ) = 1 as is_final_trip
    from {{ source('ref', 'trips') }} t

)

select
    r.trip_id,
    r.vehicle_id,
    v.plate,
    v.vehicle_type,
    r.route_id,
    rt.name                                            as route_name,
    rt.length_km                                       as route_length_km,

    r.planned_start_at,
    r.planned_end_at,
    (r.planned_start_at at time zone 'UTC')::date       as planned_date,
    rt.planned_duration_minutes,

    a.actual_start_at,
    a.actual_end_at,
    coalesce(a.pings, 0)                                as pings,
    round(coalesce(a.distance_km, 0)::numeric, 2)       as distance_km,
    coalesce(a.moving_seconds, 0)::numeric(12,2)        as moving_seconds,
    coalesce(a.stopped_seconds, 0)::numeric(12,2)       as stopped_seconds,
    coalesce(a.unobserved_seconds, 0)::numeric(12,2)    as unobserved_seconds,
    coalesce(a.engine_on_seconds, 0)::numeric(12,2)     as engine_on_seconds,
    round(a.max_speed_kph::numeric, 1)                  as max_speed_kph,
    round(a.avg_moving_kph::numeric, 1)                 as avg_moving_kph,

    round(
        extract(epoch from (a.actual_end_at - a.actual_start_at)) / 60.0
    ::numeric, 1)                                       as actual_duration_minutes,

    -- Lateness at the finish line: how far past the planned arrival the last
    -- ping of the trip fell. Distinct from the `delay` detections, which are
    -- raised *during* the trip against progress along the route -- a vehicle
    -- can run late in the middle and still arrive on time.
    round(
        extract(epoch from (a.actual_end_at - r.planned_end_at)) / 60.0
    ::numeric, 1)                                       as finish_delay_minutes,

    round(
        100.0 * coalesce(a.distance_km, 0) / nullif(rt.length_km, 0)
    ::numeric, 1)                                       as route_coverage_pct,

    round(
        100.0 * coalesce(a.moving_seconds, 0)
        / nullif(coalesce(a.moving_seconds, 0) + coalesce(a.stopped_seconds, 0), 0)
    ::numeric, 1)                                       as moving_share_pct,

    coalesce(al.alerts, 0)                              as alerts,
    coalesce(al.idle_alerts, 0)                         as idle_alerts,
    coalesce(al.deviation_alerts, 0)                    as deviation_alerts,
    coalesce(al.delay_alerts, 0)                        as delay_alerts,
    coalesce(al.breach_alerts, 0)                       as breach_alerts,
    coalesce(al.gap_alerts, 0)                          as gap_alerts,
    coalesce(al.critical_alerts, 0)                     as critical_alerts,
    al.worst_delay_minutes,
    al.worst_deviation_m,

    r.is_final_trip,
    -- Only a trip that is not the vehicle's newest can be assumed finished.
    not r.is_final_trip                                 as is_complete,
    a.pings is not null                                 as has_telemetry,

    case
        when r.is_final_trip or a.actual_end_at is null then null
        else extract(epoch from (a.actual_end_at - r.planned_end_at)) / 60.0
             <= {{ var('delay_minutes') }}
    end                                                 as is_on_time

from ranked r
join {{ source('ref', 'vehicles') }} v on v.vehicle_id = r.vehicle_id
join {{ ref('dim_route') }}          rt on rt.route_id = r.route_id
left join actual a  on a.trip_id  = r.trip_id
left join alerts al on al.trip_id = r.trip_id
