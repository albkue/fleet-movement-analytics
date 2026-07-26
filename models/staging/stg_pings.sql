{{ config(
    materialized='incremental',
    unique_key='ping_id',
    description='One typed row per validated telemetry ping.',
    indexes=[
        'vehicle_id, recorded_at',
        'recorded_at',
        'trip_id',
        'h3_r8',
        'ingested_at'
    ]
) }}

-- Flattens raw.pings into typed columns and carries the PostGIS geography
-- forward. Staging does renaming, casting and light derivation only -- no
-- business logic, no joins, no aggregation. Anything that encodes a decision
-- belongs downstream.

with source as (

    select *
    from {{ source('raw', 'pings') }}

    {% if is_incremental() %}
    -- Re-read a 15-minute overlap behind the high-water mark rather than
    -- taking strictly-newer rows. ingested_at is set by the consumer's
    -- transaction, and concurrent consumers commit out of order, so a row
    -- with an earlier ingested_at can become visible after a later one. The
    -- overlap re-selects that window and the unique_key merge makes the
    -- re-selection a no-op instead of a duplicate.
    where ingested_at >= (
        select coalesce(max(ingested_at), '-infinity'::timestamptz)
             - interval '15 minutes'
        from {{ this }}
    )
    {% endif %}

)

select
    ping_id,
    vehicle_id,
    trip_id,
    recorded_at,
    (recorded_at at time zone 'UTC')::date          as recorded_date,
    date_trunc('hour', recorded_at)                 as recorded_hour,

    lat,
    lon,
    -- The geography comes along rather than being rebuilt downstream, so
    -- there is one derivation of a ping's position in the whole project.
    location,
    h3_r8,
    h3_r9,

    speed_kph,
    heading_deg,
    ignition,
    odometer_km,
    fuel_pct,

    -- Reported by the tracker; useful for explaining a position that looks
    -- wrong rather than for filtering, which is why nothing downstream keys
    -- off it.
    (payload -> 'device' ->> 'hdop')::numeric(4,2)  as gps_hdop,
    (payload -> 'device' ->> 'satellites')::integer as gps_satellites,

    -- The tracker's own opinion of whether it is moving. Kept for
    -- comparison, but distance-based movement is what the segment model
    -- actually trusts -- a reported speed can be stale or fabricated by a
    -- device that has lost its fix.
    speed_kph <= {{ var('idle_speed_kph') }}        as is_reported_stopped,

    ingested_at

from source
