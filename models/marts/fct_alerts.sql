{{ config(
    materialized='table',
    description='One row per detection episode, enriched with vehicle, zone and route context.',
    indexes=[
        'started_at',
        'vehicle_id, started_at',
        'detection_type, severity_rank',
        'zone_id',
        'trip_id',
        'started_date'
    ]
) }}

-- Grain: one row per detection episode (not per ping, and not per alert
-- *notification*). An idle stop that lasted forty minutes is one row here
-- with a forty-minute duration, however many pings it spanned and however
-- many times a dashboard redrew it.
--
-- Open episodes are kept rather than filtered out. A vehicle that is
-- currently off route is the single most operationally interesting row in
-- this table, and dropping it because it has no end time yet would make the
-- fact table useless for exactly the case it should be best at.

select
    d.detection_key,
    d.detection_type,
    d.severity,
    d.severity_rank,
    d.is_open,

    d.vehicle_id,
    v.plate,
    v.vehicle_type,

    d.trip_id,
    t.route_id,
    rt.name                                  as route_name,

    d.zone_id,
    z.name                                   as zone_name,
    z.zone_kind,

    d.started_at,
    d.ended_at,
    d.started_date,
    d.started_hour,
    d.duration_seconds,
    d.duration_minutes,

    d.lat,
    d.lon,
    d.magnitude,

    -- One column that means "how bad", whatever the type, so a single
    -- ORDER BY produces a sensible worst-first list across a mixed feed.
    -- Types are not comparable in raw magnitude -- 300 metres off route and
    -- 300 minutes idle are not the same thing -- so each is expressed as a
    -- multiple of its own threshold.
    round(
        case d.detection_type
            when 'idle'            then d.magnitude / {{ var('idle_minutes') }}
            when 'route_deviation' then d.magnitude / {{ var('deviation_metres') }}
            when 'delay'           then d.magnitude / {{ var('delay_minutes') }}
            when 'gps_gap'         then d.magnitude / {{ var('gps_gap_minutes') }}
            when 'geofence_breach' then
                case
                    when z.max_dwell_minutes is null then 1.0
                    else d.magnitude / z.max_dwell_minutes
                end
            else null
        end, 2
    )                                        as threshold_multiple,

    d.details,
    d.detected_at,
    d.updated_at

from {{ ref('stg_detections') }} d
left join {{ source('ref', 'vehicles') }} v on v.vehicle_id = d.vehicle_id
left join {{ ref('dim_zone') }}          z on z.zone_id    = d.zone_id
left join {{ source('ref', 'trips') }}   t on t.trip_id    = d.trip_id
left join {{ source('ref', 'routes') }}  rt on rt.route_id = t.route_id
