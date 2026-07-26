{{ config(
    materialized='incremental',
    unique_key='detection_key',
    description='One typed row per detection episode raised by the stream processor.',
    indexes=[
        'vehicle_id, started_at',
        'detection_type, started_at',
        'zone_id',
        'trip_id',
        'updated_at'
    ]
) }}

-- The stream processor's output, typed and made joinable.
--
-- The watermark is `updated_at`, not `detected_at`. An episode is written
-- once when it opens and again when it closes, and those can be hours apart:
-- a vehicle that started idling at 09:00 and moved at 11:00 has a row whose
-- detected_at is 09:00 and whose duration only became known at 11:00.
-- Watermarking on detected_at would leave the warehouse permanently holding
-- the open, duration-less version of every long episode.

with source as (

    select *
    from {{ source('stream', 'detections') }}

    {% if is_incremental() %}
    where updated_at >= (
        select coalesce(max(updated_at), '-infinity'::timestamptz)
             - interval '15 minutes'
        from {{ this }}
    )
    {% endif %}

)

select
    detection_key,
    detection_id,
    detection_type,
    severity,
    vehicle_id,
    trip_id,
    zone_id,

    started_at,
    ended_at,
    (started_at at time zone 'UTC')::date  as started_date,
    date_trunc('hour', started_at)         as started_hour,
    duration_seconds,
    round(duration_seconds / 60.0, 2)      as duration_minutes,

    lat,
    lon,
    magnitude,
    details,

    -- Still running as at the last batch processed. An open episode has no
    -- duration yet, so every duration-based aggregate downstream has to
    -- decide explicitly whether to include it -- which is easier to do
    -- correctly against a boolean than against `ended_at is null` scattered
    -- through a dozen models.
    ended_at is null                       as is_open,

    -- Ordinal severity, so "at least a warning" is a comparison rather than
    -- an IN list that has to be kept in step with the enum everywhere.
    case severity
        when 'critical' then 3
        when 'warning'  then 2
        else 1
    end                                    as severity_rank,

    batch_id,
    detected_at,
    updated_at

from source
