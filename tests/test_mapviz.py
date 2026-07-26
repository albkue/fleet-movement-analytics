"""Map rendering: projection, geometry parsing, and self-containment."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone

import pytest

from fleet.geo import h3_boundary, h3_cell, haversine_m
from fleet.mapviz import (
    HEIGHT,
    PADDING,
    WIDTH,
    MapData,
    _build_frame,
    _draw_scale_bar,
    _geojson_rings,
    _mercator_y,
    render,
)

SQUARE = [
    [104.920, 11.550],
    [104.930, 11.550],
    [104.930, 11.560],
    [104.920, 11.560],
    [104.920, 11.550],
]


def data(**overrides) -> MapData:
    base = dict(
        zones=[
            {
                "zone_id": "Z-A",
                "name": "Depot A",
                "zone_kind": "depot",
                "boundary_geojson": json.dumps(
                    {"type": "Polygon", "coordinates": [SQUARE]}
                ),
                "centre_lat": 11.555,
                "centre_lon": 104.925,
                "visits": 3,
                "breaches": 1,
            }
        ],
        routes=[
            {
                "route_id": "R-A",
                "name": "Route A",
                "path_geojson": json.dumps(
                    {
                        "type": "LineString",
                        "coordinates": [[104.921, 11.551], [104.929, 11.559]],
                    }
                ),
                "length_km": 1.2,
            }
        ],
        tracks=[
            {
                "vehicle_id": "VH-001",
                "plate": "PP-1201",
                "vehicle_type": "van",
                "lons": [104.922, 104.925, 104.928],
                "lats": [11.552, 11.555, 11.558],
            }
        ],
        alerts=[
            {
                "detection_type": "idle",
                "severity": "warning",
                "vehicle_id": "VH-001",
                "plate": "PP-1201",
                "zone_name": "Depot A",
                "started_at": datetime(2026, 7, 26, 9, 0, tzinfo=timezone.utc),
                "duration_minutes": 14.0,
                "magnitude": 14.0,
                "lat": 11.556,
                "lon": 104.926,
                "is_open": False,
            }
        ],
        cells=[
            {
                "h3_r8": h3_cell(104.925, 11.555, 8),
                "pings": 120,
                "vehicles": 4,
                "stopped_hours": 1.5,
                "avg_moving_kph": 18.2,
            }
        ],
        generated_at=datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return MapData(**base)


# ----------------------------------------------------------- projection ----


def test_mercator_northing_is_monotone_in_latitude():
    values = [_mercator_y(lat) for lat in range(-80, 81, 10)]

    assert values == sorted(values)


def test_mercator_clamps_at_the_poles():
    assert math.isfinite(_mercator_y(90.0))
    assert math.isfinite(_mercator_y(-90.0))


def test_north_is_up():
    """SVG y grows downwards, so a northern point must project to a smaller y."""
    frame = _build_frame([(104.92, 11.55), (104.93, 11.56)])

    _, south_y = frame.project((104.925, 11.551))
    _, north_y = frame.project((104.925, 11.559))

    assert north_y < south_y


def test_east_is_right():
    frame = _build_frame([(104.92, 11.55), (104.93, 11.56)])

    west_x, _ = frame.project((104.921, 11.555))
    east_x, _ = frame.project((104.929, 11.555))

    assert east_x > west_x


def test_every_projected_point_lands_inside_the_canvas():
    points = [(lon, lat) for lon in (104.90, 104.95) for lat in (11.50, 11.60)]
    frame = _build_frame(points)

    for point in points:
        x, y = frame.project(point)
        assert PADDING - 1 <= x <= WIDTH - PADDING + 1
        assert PADDING - 1 <= y <= HEIGHT - PADDING + 1


def test_aspect_ratio_is_preserved():
    """One scale for both axes, or every shape comes out stretched."""
    frame = _build_frame([(104.90, 11.50), (104.95, 11.60)])

    a = frame.project((104.90, 11.50))
    b = frame.project((104.95, 11.50))
    c = frame.project((104.90, 11.60))

    span_x_units = b[0] - a[0]
    span_y_units = a[1] - c[1]
    span_x_m = haversine_m((104.90, 11.50), (104.95, 11.50))
    span_y_m = haversine_m((104.90, 11.50), (104.90, 11.60))

    assert (span_x_units / span_x_m) == pytest.approx(
        span_y_units / span_y_m, rel=0.01
    )


def test_a_degenerate_extent_does_not_divide_by_zero():
    frame = _build_frame([(104.92, 11.55)])

    x, y = frame.project((104.92, 11.55))

    assert math.isfinite(x) and math.isfinite(y)


def test_frame_reports_a_plausible_ground_scale():
    frame = _build_frame([(104.90, 11.50), (104.95, 11.60)])
    a = frame.project((104.90, 11.55))
    b = frame.project((104.95, 11.55))

    measured_m = abs(b[0] - a[0]) * frame.metres_per_unit
    actual_m = haversine_m((104.90, 11.55), (104.95, 11.55))

    assert measured_m == pytest.approx(actual_m, rel=0.02)


def test_nothing_to_draw_is_an_error_not_a_blank_frame():
    with pytest.raises(ValueError, match="nothing to draw"):
        _build_frame([])


def test_scale_bar_is_rounded_to_a_human_number():
    frame = _build_frame([(104.90, 11.50), (104.95, 11.60)])

    bar = _draw_scale_bar(frame)

    assert "<text" in bar
    assert (" km<" in bar) or (" m<" in bar)


# ------------------------------------------------------------- geojson ----


def test_polygon_geojson_is_parsed_as_lon_lat():
    rings = _geojson_rings(json.dumps({"type": "Polygon", "coordinates": [SQUARE]}))

    assert len(rings) == 1
    assert rings[0][0] == (104.920, 11.550)


def test_linestring_geojson_is_parsed():
    rings = _geojson_rings(
        json.dumps({"type": "LineString", "coordinates": [[1.0, 2.0], [3.0, 4.0]]})
    )

    assert rings == [[(1.0, 2.0), (3.0, 4.0)]]


def test_unknown_geometry_types_are_ignored_rather_than_crashing():
    assert _geojson_rings(json.dumps({"type": "Point", "coordinates": [1, 2]})) == []


def test_missing_geojson_is_ignored():
    assert _geojson_rings(None) == []


# ------------------------------------------------------------ rendering ----


def test_render_produces_a_complete_document():
    page = render(data(), hours=6)

    assert page.startswith("<!doctype html>")
    assert "</html>" in page
    assert "<svg" in page


def test_render_makes_no_network_requests():
    """The whole point of the format: it opens offline, forever."""
    page = render(data(), hours=6).lower()
    # The SVG namespace is a URI, not a fetch -- it is the one permitted
    # occurrence of a scheme in the document.
    remaining = page.replace("http://www.w3.org/2000/svg", "")

    for token in ("http://", "https://", "//cdn", "<link", "src=", "@import", "url("):
        assert token not in remaining, token


def test_every_layer_is_drawn_and_toggleable():
    page = render(data(), hours=6)

    for layer in ("cells", "zones", "routes", "tracks", "alerts"):
        assert f'id="layer-{layer}"' in page
        assert f'data-layer="layer-{layer}"' in page


def test_hexagons_use_real_h3_geometry():
    """Drawn from the same cell id the aggregate is grouped on."""
    cell = h3_cell(104.925, 11.555, 8)
    page = render(data(), hours=6)
    frame = _build_frame(data().all_points())

    first_vertex = frame.project(h3_boundary(cell)[0])
    assert f"M{first_vertex[0]:.1f},{first_vertex[1]:.1f}" in page


def test_tooltips_carry_the_detail():
    page = render(data(), hours=6)

    assert "Depot A" in page
    assert "PP-1201" in page
    assert "Route A" in page


def test_content_is_escaped():
    hostile = data()
    hostile.zones[0]["name"] = '<script>alert("x")</script>'

    page = render(hostile, hours=6)

    assert "<script>alert" not in page
    assert "&lt;script&gt;" in page


def test_empty_data_renders_a_useful_message_not_a_traceback():
    page = render(
        MapData([], [], [], [], [], datetime.now(timezone.utc)), hours=6
    )

    assert "Nothing to draw" in page
    assert "fleet seed" in page


def test_all_points_spans_every_layer():
    points = data().all_points()

    assert len(points) >= len(SQUARE) + 2 + 3 + 1


def test_a_track_with_one_point_is_not_drawn_as_a_line():
    single = data(
        tracks=[
            {
                "vehicle_id": "VH-002",
                "plate": "PP-1202",
                "vehicle_type": "van",
                "lons": [104.925],
                "lats": [11.555],
            }
        ]
    )

    page = render(single, hours=6)

    assert 'class="track"' not in page


def test_the_page_states_that_it_has_no_basemap():
    page = render(data(), hours=6)

    assert "No basemap" in page
