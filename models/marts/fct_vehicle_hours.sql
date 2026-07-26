{{ config(
    materialized='table',
    description='One row per vehicle per hour: distance, time split, and utilisation.',
    indexes=[
        'vehicle_id, recorded_hour',
        'recorded_hour',
        'recorded_date'
    ]
) }}

-- Grain: one row per (vehicle_id, recorded_hour), for hours the vehicle
-- actually reported in. Hours with no telemetry produce no row rather than a
-- row of zeros, because "reported nothing" and "reported standing still" are
-- different facts and only one of them is evidence about the vehicle.
--
-- **Utilisation is stated against observed time, not wall-clock time.** The
-- denominator is moving + stopped, which excludes any signal gap. Dividing
-- by 3600 instead would quietly report a vehicle as 0% utilised for an hour
-- it spent in an underground car park with no fix -- the same number it would
-- get for an hour spent parked in plain view, which is a different problem
-- with a different fix.

select
    vehicle_id,
    recorded_hour,
    recorded_date,
    (extract(hour from recorded_hour))::integer   as hour_of_day,

    count(*)                                      as pings,
    count(distinct trip_id)                       as trips,

    round((sum(travelled_m) / 1000.0)::numeric, 3) as distance_km,

    -- Two decimal places, matching int_ping_segments. moving + stopped must
    -- equal observed, and rounding the components to fewer places than their
    -- inputs carry breaks that identity: two halves that round up exceed a
    -- whole that rounds down.
    sum(moving_seconds)::numeric(12,2)            as moving_seconds,
    sum(stopped_seconds)::numeric(12,2)           as stopped_seconds,
    sum(unobserved_seconds)::numeric(12,2)        as unobserved_seconds,
    sum(engine_on_seconds)::numeric(12,2)         as engine_on_seconds,
    sum(observed_seconds)::numeric(12,2)          as observed_seconds,

    -- Engine running while not moving: the fuel cost of standing still.
    greatest(sum(engine_on_seconds) - sum(moving_seconds), 0)::numeric(12,2)
                                                  as idling_engine_seconds,

    round(max(speed_kph)::numeric, 1)             as max_speed_kph,
    round(avg(implied_speed_kph) filter (where is_moving)::numeric, 1)
                                                  as avg_moving_kph,

    round(
        100.0 * sum(moving_seconds) / nullif(sum(observed_seconds), 0)
    ::numeric, 1)                                 as utilisation_pct,

    -- How much of the hour we have evidence about at all. A low number here
    -- makes every other percentage on the row less trustworthy, so it is
    -- reported next to them rather than left to be inferred.
    round(
        100.0 * sum(observed_seconds)
        / nullif(sum(observed_seconds) + sum(unobserved_seconds), 0)
    ::numeric, 1)                                 as coverage_pct,

    count(*) filter (where is_after_gap)          as signal_gaps

from {{ ref('int_ping_segments') }}
group by vehicle_id, recorded_hour, recorded_date
