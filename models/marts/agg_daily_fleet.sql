{{ config(
    materialized='table',
    description='Daily fleet health by vehicle type: distance, utilisation and alert rates.',
    indexes=['activity_date', 'vehicle_type', 'activity_date, vehicle_type']
) }}

-- Grain: one row per (activity_date, vehicle_type).
--
-- Split by type because a motorbike and a seven-tonne truck have nothing in
-- common operationally: they cover different distances, idle for different
-- reasons and breach different things. A single fleet-wide average is the
-- mean of two distributions that never overlap, and it moves whenever the
-- mix changes rather than when performance does.
--
-- Rates are stored, not left to whatever is reading this, so that every
-- consumer computes them the same way.

with hours as (

    select
        h.recorded_date                as activity_date,
        v.vehicle_type,
        count(distinct h.vehicle_id)   as active_vehicles,
        sum(h.pings)                   as pings,
        sum(h.distance_km)             as distance_km,
        sum(h.moving_seconds)          as moving_seconds,
        sum(h.stopped_seconds)         as stopped_seconds,
        sum(h.unobserved_seconds)      as unobserved_seconds,
        sum(h.engine_on_seconds)       as engine_on_seconds,
        sum(h.idling_engine_seconds)   as idling_engine_seconds,
        sum(h.observed_seconds)        as observed_seconds,
        max(h.max_speed_kph)           as max_speed_kph
    from {{ ref('fct_vehicle_hours') }} h
    join {{ ref('dim_vehicle') }}      v on v.vehicle_id = h.vehicle_id
    group by h.recorded_date, v.vehicle_type

),

alerts as (

    select
        a.started_date                                             as activity_date,
        a.vehicle_type,
        count(*)                                                   as alerts,
        count(*) filter (where a.severity = 'critical')            as critical_alerts,
        count(*) filter (where a.detection_type = 'idle')          as idle_alerts,
        count(*) filter (where a.detection_type = 'route_deviation') as deviation_alerts,
        count(*) filter (where a.detection_type = 'delay')         as delay_alerts,
        count(*) filter (where a.detection_type = 'geofence_breach') as breach_alerts,
        count(*) filter (where a.detection_type = 'gps_gap')       as gap_alerts
    from {{ ref('fct_alerts') }} a
    where a.vehicle_type is not null
    group by a.started_date, a.vehicle_type

),

trips as (

    select
        t.planned_date                                     as activity_date,
        t.vehicle_type,
        count(*)                                           as trips_planned,
        count(*) filter (where t.is_complete)              as trips_completed,
        count(*) filter (where t.is_on_time)               as trips_on_time,
        count(*) filter (where not t.has_telemetry)        as trips_without_telemetry,
        avg(t.finish_delay_minutes) filter (where t.is_complete)
                                                           as avg_finish_delay_minutes
    from {{ ref('fct_trips') }} t
    group by t.planned_date, t.vehicle_type

)

select
    coalesce(h.activity_date, a.activity_date, t.activity_date) as activity_date,
    coalesce(h.vehicle_type, a.vehicle_type, t.vehicle_type)    as vehicle_type,

    coalesce(h.active_vehicles, 0)                  as active_vehicles,
    coalesce(h.pings, 0)                            as pings,
    round(coalesce(h.distance_km, 0)::numeric, 2)   as distance_km,

    -- Precision matches the hourly fact it sums, so the additive identities
    -- survive the rollup rather than being re-rounded at each level.
    coalesce(h.moving_seconds, 0)::numeric(14,2)    as moving_seconds,
    coalesce(h.stopped_seconds, 0)::numeric(14,2)   as stopped_seconds,
    coalesce(h.unobserved_seconds, 0)::numeric(14,2) as unobserved_seconds,
    coalesce(h.idling_engine_seconds, 0)::numeric(14,2) as idling_engine_seconds,
    h.max_speed_kph,

    round(
        100.0 * h.moving_seconds / nullif(h.observed_seconds, 0)
    ::numeric, 1)                                   as utilisation_pct,
    round(
        100.0 * h.observed_seconds
        / nullif(h.observed_seconds + h.unobserved_seconds, 0)
    ::numeric, 1)                                   as coverage_pct,
    round(
        100.0 * h.idling_engine_seconds / nullif(h.engine_on_seconds, 0)
    ::numeric, 1)                                   as engine_idle_share_pct,

    coalesce(t.trips_planned, 0)                    as trips_planned,
    coalesce(t.trips_completed, 0)                  as trips_completed,
    coalesce(t.trips_on_time, 0)                    as trips_on_time,
    coalesce(t.trips_without_telemetry, 0)          as trips_without_telemetry,
    -- Denominator is completed trips, not planned ones. A trip still running
    -- is neither on time nor late yet, and counting it as late is how
    -- punctuality metrics end up being worst at the end of every shift.
    round(
        100.0 * t.trips_on_time / nullif(t.trips_completed, 0)
    ::numeric, 1)                                   as on_time_pct,
    round(t.avg_finish_delay_minutes::numeric, 1)   as avg_finish_delay_minutes,

    coalesce(a.alerts, 0)                           as alerts,
    coalesce(a.critical_alerts, 0)                  as critical_alerts,
    coalesce(a.idle_alerts, 0)                      as idle_alerts,
    coalesce(a.deviation_alerts, 0)                 as deviation_alerts,
    coalesce(a.delay_alerts, 0)                     as delay_alerts,
    coalesce(a.breach_alerts, 0)                    as breach_alerts,
    coalesce(a.gap_alerts, 0)                       as gap_alerts,

    -- Alerts per 100 km, so a busy day and a quiet day are comparable.
    round(
        100.0 * coalesce(a.alerts, 0) / nullif(h.distance_km, 0)
    ::numeric, 2)                                   as alerts_per_100km

from hours h
full outer join alerts a
    on a.activity_date = h.activity_date and a.vehicle_type = h.vehicle_type
full outer join trips t
    on t.activity_date = coalesce(h.activity_date, a.activity_date)
   and t.vehicle_type  = coalesce(h.vehicle_type, a.vehicle_type)
