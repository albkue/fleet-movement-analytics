{{ config(
    materialized='table',
    description='One row per geofence, with its measured shape and a GeoJSON boundary.',
    indexes=['zone_id', 'zone_kind']
) }}

-- Grain: one row per zone in ref.zones.
--
-- The shape measurements come from PostGIS on the geography type, so
-- `area_km2` is real area on the spheroid rather than the meaningless
-- product of two spans in degrees -- which at this latitude would be out by
-- about a factor of a hundred and would still look plausible.
--
-- `boundary_geojson` is carried so that anything drawing a map reads the
-- polygon from the warehouse rather than re-reading config/fleet.json and
-- risking a picture that disagrees with the numbers beside it.

select
    zone_id,
    name,
    zone_kind,
    max_dwell_minutes,

    round((ST_Area(boundary) / 1e6)::numeric, 5)      as area_km2,
    round((ST_Perimeter(boundary) / 1000)::numeric, 4) as perimeter_km,

    ST_Y(ST_Centroid(boundary::geometry))             as centre_lat,
    ST_X(ST_Centroid(boundary::geometry))             as centre_lon,
    ST_AsGeoJSON(boundary::geometry)                  as boundary_geojson,

    -- A restricted zone is breached by entry; every other kind only by
    -- overstaying. Stating it once here keeps the rule out of the reports.
    zone_kind = 'restricted'                          as breaches_on_entry,
    max_dwell_minutes is not null                     as has_dwell_limit,

    loaded_at

from {{ source('ref', 'zones') }}
