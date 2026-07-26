{{ config(
    materialized='table',
    description='One row per ping paired with the vehicle previous ping: the movement between them.',
    indexes=[
        'vehicle_id, recorded_at',
        'trip_id',
        'recorded_hour',
        'recorded_date',
        'h3_r8'
    ]
) }}

-- The workhorse. Nearly every operational number in this project -- distance
-- driven, time moving, time stopped, utilisation, fleet speed -- is a sum
-- over this model, so the decisions here are the ones worth being careful
-- about.
--
-- **A segment belongs to the ping that ends it.** The interval between two
-- fixes is attributed to the later one, so summing by hour or by trip never
-- double counts and never leaves an orphan.
--
-- **Movement is measured, not reported.** `implied_speed_kph` comes from the
-- distance actually covered over the time actually elapsed. A tracker's own
-- speed field can be stale, can be fabricated from a lost fix, and on a
-- stationary vehicle wanders with GPS noise. Displacement over a real
-- interval cannot do any of those things: at rest, 7 m of noise across a
-- 15-second gap reads as roughly 1.7 kph, comfortably under the threshold.
--
-- **Time nobody observed is counted as neither.** A gap longer than
-- GPS_GAP_MINUTES is time the fleet was not reporting. It is not idle time
-- and it is not moving time; folding it into either would silently credit or
-- charge the vehicle for a period no one has evidence about. It gets its own
-- column instead, so utilisation can be stated against observed time and the
-- unobserved remainder stays visible.
--
-- Full rebuild by design. The measurement of a ping depends on the ping
-- before it, so a late arrival changes a row that was already written -- and
-- only a full pass sees that.

with ordered as (

    select
        ping_id,
        vehicle_id,
        trip_id,
        recorded_at,
        recorded_date,
        recorded_hour,
        lat,
        lon,
        h3_r8,
        h3_r9,
        speed_kph,
        heading_deg,
        ignition,
        odometer_km,
        -- ping_id breaks ties so two fixes sharing a timestamp order
        -- deterministically; without it the segment boundaries could differ
        -- between runs on identical data.
        lag(recorded_at) over w as prev_recorded_at,
        lag(location)    over w as prev_location,
        location
    from {{ ref('stg_pings') }}
    window w as (partition by vehicle_id order by recorded_at, ping_id)

),

measured as (

    select
        *,
        extract(epoch from (recorded_at - prev_recorded_at)) as gap_seconds,
        case
            when prev_location is null then null
            -- Both operands are geography, so this is metres on the
            -- spheroid, not degrees.
            else ST_Distance(location, prev_location)
        end as raw_distance_m
    from ordered

),

classified as (

    select
        *,
        case
            when gap_seconds is null or gap_seconds <= 0 then null
            else raw_distance_m / gap_seconds * 3.6
        end as implied_speed_kph,
        coalesce(
            gap_seconds > {{ var('gps_gap_minutes') }} * 60, false
        ) as is_after_gap
    from measured

)

select
    ping_id,
    vehicle_id,
    trip_id,
    recorded_at,
    recorded_date,
    recorded_hour,
    lat,
    lon,
    h3_r8,
    h3_r9,
    speed_kph,
    heading_deg,
    ignition,
    odometer_km,

    prev_recorded_at,
    gap_seconds,
    round(raw_distance_m::numeric, 2)     as raw_distance_m,
    round(implied_speed_kph::numeric, 2)  as implied_speed_kph,
    is_after_gap,

    -- The first ping of a vehicle has no interval before it, and a ping after
    -- a signal gap has an interval nobody watched. Both contribute nothing.
    (
        implied_speed_kph is not null
        and not is_after_gap
        and implied_speed_kph > {{ var('idle_speed_kph') }}
    ) as is_moving,

    case
        when gap_seconds is null or is_after_gap then 0
        else gap_seconds
    end::numeric(12,2) as observed_seconds,

    case
        when gap_seconds is null or is_after_gap then 0
        when implied_speed_kph > {{ var('idle_speed_kph') }} then gap_seconds
        else 0
    end::numeric(12,2) as moving_seconds,

    case
        when gap_seconds is null or is_after_gap then 0
        when implied_speed_kph > {{ var('idle_speed_kph') }} then 0
        else gap_seconds
    end::numeric(12,2) as stopped_seconds,

    case
        when gap_seconds is not null and is_after_gap then gap_seconds
        else 0
    end::numeric(12,2) as unobserved_seconds,

    -- Distance is credited only to segments that were genuinely moving.
    -- Otherwise a vehicle parked overnight would accumulate a kilometre of
    -- GPS jitter, which is how odometers derived from telemetry end up
    -- disagreeing with the dashboard by a few percent every single day.
    case
        when is_after_gap or implied_speed_kph is null then 0
        when implied_speed_kph > {{ var('idle_speed_kph') }} then raw_distance_m
        else 0
    end::numeric(12,2) as travelled_m,

    -- Engine time, which is not the same as moving time: the difference is
    -- exactly the idling this project exists to surface.
    case
        when gap_seconds is null or is_after_gap then 0
        when ignition then gap_seconds
        else 0
    end::numeric(12,2) as engine_on_seconds

from classified
