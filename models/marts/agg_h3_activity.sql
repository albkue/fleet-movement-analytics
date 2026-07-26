{{ config(
    materialized='table',
    description='Daily fleet density and speed per H3 cell: the heatmap grid.',
    indexes=['activity_date', 'h3_r8', 'activity_date, h3_r8']
) }}

-- Grain: one row per (activity_date, h3_r8).
--
-- **Why an H3 grid and not the geofences.** The zones answer "what happened
-- at the places we care about". This answers "where is the fleet actually
-- spending its time", which is a different question and cannot be asked of a
-- polygon table -- a zone can only tell you about the ground someone already
-- drew a box around. Congestion on a road nobody geofenced shows up here and
-- nowhere else.
--
-- **Why it is cheap.** The cell id was computed once per ping at ingest, so
-- this is a GROUP BY on a text column. The equivalent without H3 is a
-- spatial join of every ping against a grid table, per query, for ever.
--
-- Resolution 8 averages roughly 0.7 km² per cell, which is a few city
-- blocks: fine enough to see a corridor, coarse enough that a day of fleet
-- movement lands as a readable heatmap rather than a scatter of singletons.

with cells as (

    select
        recorded_date                            as activity_date,
        h3_r8,
        count(*)                                 as pings,
        count(distinct vehicle_id)               as vehicles,
        count(distinct trip_id)                  as trips,
        sum(travelled_m)                         as travelled_m,
        sum(moving_seconds)                      as moving_seconds,
        sum(stopped_seconds)                     as stopped_seconds,
        sum(observed_seconds)                    as observed_seconds,
        avg(implied_speed_kph) filter (where is_moving) as avg_moving_kph,
        min(implied_speed_kph) filter (where is_moving) as min_moving_kph,
        -- Representative point for drawing. This is the mean of the pings in
        -- the cell, not the cell's own centre: computing the true centre
        -- would need the H3 library, which lives in the ingest path, not in
        -- Postgres. For a heatmap marker the difference is invisible, and
        -- the name says which one it is.
        avg(lat)                                 as mean_lat,
        avg(lon)                                 as mean_lon
    from {{ ref('int_ping_segments') }}
    group by recorded_date, h3_r8

)

select
    activity_date,
    h3_r8,
    pings,
    vehicles,
    trips,

    round((travelled_m / 1000.0)::numeric, 3)     as distance_km,

    -- Two decimal places, matching int_ping_segments, and deliberately not
    -- one. These three must satisfy moving + stopped = observed, and
    -- rounding each of them independently breaks that: two halves that both
    -- round up exceed the whole that rounds down. Round for display, never
    -- for storing components that have to add up.
    moving_seconds::numeric(14,2)                 as moving_seconds,
    stopped_seconds::numeric(14,2)                as stopped_seconds,
    observed_seconds::numeric(14,2)               as observed_seconds,

    round(avg_moving_kph::numeric, 1)             as avg_moving_kph,
    round(min_moving_kph::numeric, 1)             as min_moving_kph,

    -- Share of observed time spent stopped in this cell. High values on a
    -- cell with many distinct vehicles is what congestion looks like from
    -- above; high values with one vehicle is just somewhere it parked.
    round(
        100.0 * stopped_seconds / nullif(observed_seconds, 0)
    ::numeric, 1)                                 as stopped_share_pct,

    round(mean_lat::numeric, 6)                   as mean_lat,
    round(mean_lon::numeric, 6)                   as mean_lon

from cells
