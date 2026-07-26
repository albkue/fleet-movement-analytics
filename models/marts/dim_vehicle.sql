{{ config(
    materialized='table',
    description='One row per vehicle: its registration details and lifetime activity.',
    indexes=['vehicle_id', 'vehicle_type', 'home_depot_id']
) }}

-- Grain: one row per vehicle in ref.vehicles.
--
-- Every registered vehicle appears, including one that has never reported.
-- A left join rather than an inner one, because "which of my vehicles sent
-- nothing today" is a question about the fleet, and an inner join answers it
-- by deleting the evidence.

with activity as (

    select
        vehicle_id,
        count(*)                          as pings,
        count(distinct trip_id)            as trips,
        min(recorded_at)                   as first_seen_at,
        max(recorded_at)                   as last_seen_at,
        sum(travelled_m) / 1000.0          as distance_km,
        sum(moving_seconds)                as moving_seconds,
        sum(stopped_seconds)               as stopped_seconds,
        sum(engine_on_seconds)             as engine_on_seconds,
        max(speed_kph)                     as max_speed_kph
    from {{ ref('int_ping_segments') }}
    group by vehicle_id

)

select
    v.vehicle_id,
    v.plate,
    v.vehicle_type,
    v.capacity_kg,
    v.home_depot_id,
    z.name                                          as home_depot_name,

    coalesce(a.pings, 0)                            as pings,
    coalesce(a.trips, 0)                            as trips,
    a.first_seen_at,
    a.last_seen_at,
    round(coalesce(a.distance_km, 0)::numeric, 2)   as distance_km,
    coalesce(a.moving_seconds, 0)::numeric(14,2)    as moving_seconds,
    coalesce(a.stopped_seconds, 0)::numeric(14,2)   as stopped_seconds,
    coalesce(a.engine_on_seconds, 0)::numeric(14,2) as engine_on_seconds,
    a.max_speed_kph,

    -- Engine time spent not moving. This is the number a fleet manager acts
    -- on: it is fuel burned to sit still.
    greatest(
        coalesce(a.engine_on_seconds, 0) - coalesce(a.moving_seconds, 0), 0
    )::numeric(14,2)                                as idling_engine_seconds,

    a.pings is not null                             as has_reported

from {{ source('ref', 'vehicles') }} v
join {{ source('ref', 'zones') }} z on z.zone_id = v.home_depot_id
left join activity a on a.vehicle_id = v.vehicle_id
