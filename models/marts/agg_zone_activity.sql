{{ config(
    materialized='table',
    description='Daily geofence activity: visits, dwell time and breaches per zone.',
    indexes=['activity_date', 'zone_id', 'activity_date, zone_id']
) }}

-- Grain: one row per (activity_date, zone_id), for every zone, every day any
-- zone saw activity. A depot that received nothing on a day the rest of the
-- fleet was busy is a row of zeros here rather than a missing row -- that is
-- the shape a time series has to have to be plotted without gaps lying about
-- what happened.
--
-- Dwell statistics come only from *closed* visits. A vehicle currently
-- parked in a zone has an open episode with no duration; including it as a
-- zero-minute visit would drag every average down by however many vehicles
-- happen to be sitting somewhere at the moment the model ran.

with visits as (

    select
        started_date                      as activity_date,
        zone_id,
        count(*)                          as visits,
        count(*) filter (where not is_open) as closed_visits,
        count(distinct vehicle_id)        as vehicles,
        sum(duration_seconds) filter (where not is_open) as dwell_seconds,
        avg(duration_seconds) filter (where not is_open) as avg_dwell_seconds,
        max(duration_seconds) filter (where not is_open) as max_dwell_seconds
    from {{ ref('fct_alerts') }}
    where detection_type = 'zone_visit' and zone_id is not null
    group by started_date, zone_id

),

breaches as (

    select
        started_date               as activity_date,
        zone_id,
        count(*)                   as breaches,
        count(*) filter (where details ->> 'reason' = 'entered_restricted_zone')
                                   as entry_breaches,
        count(*) filter (where details ->> 'reason' = 'dwell_limit_exceeded')
                                   as dwell_breaches,
        max(magnitude)             as worst_dwell_minutes
    from {{ ref('fct_alerts') }}
    where detection_type = 'geofence_breach' and zone_id is not null
    group by started_date, zone_id

),

-- Cross the zones against every date that saw any activity, so the series is
-- dense per zone rather than only where something happened.
calendar as (

    select distinct activity_date from visits
    union
    select distinct activity_date from breaches

),

grid as (

    select c.activity_date, z.zone_id
    from calendar c
    cross join {{ ref('dim_zone') }} z

)

select
    g.activity_date,
    g.zone_id,
    z.name                                        as zone_name,
    z.zone_kind,
    z.max_dwell_minutes,
    z.area_km2,
    z.centre_lat,
    z.centre_lon,

    coalesce(v.visits, 0)                         as visits,
    coalesce(v.closed_visits, 0)                  as closed_visits,
    coalesce(v.vehicles, 0)                       as vehicles,
    coalesce(v.dwell_seconds, 0)::numeric(14,1)   as dwell_seconds,
    round((v.dwell_seconds / 3600.0)::numeric, 2) as dwell_hours,
    round((v.avg_dwell_seconds / 60.0)::numeric, 1) as avg_dwell_minutes,
    round((v.max_dwell_seconds / 60.0)::numeric, 1) as max_dwell_minutes_observed,

    coalesce(b.breaches, 0)                       as breaches,
    coalesce(b.entry_breaches, 0)                 as entry_breaches,
    coalesce(b.dwell_breaches, 0)                 as dwell_breaches,
    b.worst_dwell_minutes,

    -- Of the visits that ended, how many broke the zone's own dwell rule.
    -- Open visits are excluded from both sides so the ratio cannot exceed
    -- 100% as a vehicle sits there.
    round(
        100.0 * coalesce(b.dwell_breaches, 0) / nullif(v.closed_visits, 0)
    ::numeric, 1)                                 as dwell_breach_rate_pct

from grid g
join {{ ref('dim_zone') }} z on z.zone_id = g.zone_id
left join visits v   on v.activity_date = g.activity_date and v.zone_id = g.zone_id
left join breaches b on b.activity_date = g.activity_date and b.zone_id = g.zone_id
