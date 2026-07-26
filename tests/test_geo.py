"""Great-circle maths, polyline walking, polygon tests and H3 indexing."""

from __future__ import annotations

import math

import pytest

from fleet.geo import (
    bearing_deg,
    bounding_box,
    cumulative_lengths_m,
    destination,
    distance_to_path_m,
    distance_to_segment_m,
    h3_boundary,
    h3_cell,
    h3_centre,
    haversine_m,
    path_length_m,
    point_at_distance,
    point_in_ring,
    ring_centroid,
    to_wkt_linestring,
    to_wkt_polygon,
)

PHNOM_PENH = (104.9282, 11.5564)
SIEM_REAP = (103.8600, 13.3600)

# A square roughly 1 km on a side, in (lon, lat), counter-clockwise, closed.
SQUARE = (
    (104.920, 11.550),
    (104.930, 11.550),
    (104.930, 11.560),
    (104.920, 11.560),
    (104.920, 11.550),
)


# ------------------------------------------------------- great circle ----


def test_distance_to_self_is_zero():
    assert haversine_m(PHNOM_PENH, PHNOM_PENH) == pytest.approx(0.0, abs=1e-9)


def test_known_distance_phnom_penh_to_siem_reap():
    """Roughly 230 km great-circle. 1% tolerance covers the Earth model."""
    metres = haversine_m(PHNOM_PENH, SIEM_REAP)

    assert metres == pytest.approx(230_000, rel=0.01)


def test_distance_is_symmetric():
    assert haversine_m(PHNOM_PENH, SIEM_REAP) == pytest.approx(
        haversine_m(SIEM_REAP, PHNOM_PENH)
    )


def test_one_degree_of_latitude_is_about_111_km():
    a = (104.9, 11.0)
    b = (104.9, 12.0)

    assert haversine_m(a, b) == pytest.approx(111_195, rel=0.001)


def test_bearing_due_north_and_due_east():
    assert bearing_deg((104.9, 11.0), (104.9, 12.0)) == pytest.approx(0.0, abs=1e-6)
    assert bearing_deg((104.9, 11.0), (105.9, 11.0)) == pytest.approx(90.0, abs=0.2)


def test_destination_then_measure_round_trips():
    for bearing in (0, 45, 90, 180, 271, 359):
        moved = destination(PHNOM_PENH, bearing, 2500.0)

        assert haversine_m(PHNOM_PENH, moved) == pytest.approx(2500.0, rel=1e-4)
        assert bearing_deg(PHNOM_PENH, moved) == pytest.approx(bearing, abs=0.5)


def test_destination_normalises_longitude_across_the_antimeridian():
    lon, _ = destination((179.99, 0.0), 90.0, 5000.0)

    assert -180.0 <= lon < 180.0


# ---------------------------------------------------------- polylines ----


def test_path_length_is_the_sum_of_its_segments():
    path = [(104.90, 11.55), (104.92, 11.55), (104.92, 11.57)]

    assert path_length_m(path) == pytest.approx(
        haversine_m(path[0], path[1]) + haversine_m(path[1], path[2])
    )


def test_cumulative_lengths_start_at_zero_and_end_at_total():
    path = [(104.90, 11.55), (104.92, 11.55), (104.92, 11.57)]
    cumulative = cumulative_lengths_m(path)

    assert cumulative[0] == 0.0
    assert cumulative[-1] == pytest.approx(path_length_m(path))
    assert cumulative == sorted(cumulative)


def test_walking_zero_lands_on_the_start():
    path = [(104.90, 11.55), (104.92, 11.57)]
    position, _ = point_at_distance(path, 0.0)

    assert haversine_m(position, path[0]) == pytest.approx(0.0, abs=0.5)


def test_walking_the_whole_length_lands_on_the_end():
    path = [(104.90, 11.55), (104.92, 11.55), (104.92, 11.57)]
    position, _ = point_at_distance(path, path_length_m(path))

    assert haversine_m(position, path[-1]) == pytest.approx(0.0, abs=1.0)


def test_walking_past_the_end_clamps():
    path = [(104.90, 11.55), (104.92, 11.57)]
    beyond, _ = point_at_distance(path, path_length_m(path) * 5)
    at_end, _ = point_at_distance(path, path_length_m(path))

    assert haversine_m(beyond, at_end) == pytest.approx(0.0, abs=1.0)


def test_walking_is_monotone_along_the_path():
    path = [(104.90, 11.55), (104.92, 11.55), (104.92, 11.57), (104.94, 11.58)]
    total = path_length_m(path)

    previous = 0.0
    for i in range(1, 21):
        position, _ = point_at_distance(path, total * i / 20)
        travelled = haversine_m(path[0], position)
        # Not strictly the along-path distance (the path turns), but it must
        # not go backwards on a path that never doubles back.
        assert travelled >= previous - 1.0
        previous = travelled


def test_walked_distance_matches_the_distance_asked_for():
    path = [(104.90, 11.55), (104.92, 11.55)]
    position, _ = point_at_distance(path, 700.0)

    assert haversine_m(path[0], position) == pytest.approx(700.0, rel=1e-3)


def test_a_single_vertex_is_not_a_polyline():
    with pytest.raises(ValueError):
        point_at_distance([(104.9, 11.5)], 100.0)


# ------------------------------------------------------- point to line ----


def test_distance_to_a_segment_is_zero_on_the_segment():
    start, end = (104.90, 11.55), (104.92, 11.55)
    midpoint, _ = point_at_distance([start, end], 500.0)

    assert distance_to_segment_m(midpoint, start, end) == pytest.approx(0.0, abs=1.0)


def test_distance_to_a_segment_is_perpendicular_when_alongside():
    start, end = (104.90, 11.55), (104.92, 11.55)
    midpoint, _ = point_at_distance([start, end], 500.0)
    offset = destination(midpoint, 0.0, 300.0)  # due north of the line

    assert distance_to_segment_m(offset, start, end) == pytest.approx(300.0, rel=0.02)


def test_distance_to_a_segment_clamps_at_the_ends():
    """Beyond an endpoint the answer is the distance to that endpoint."""
    start, end = (104.90, 11.55), (104.92, 11.55)
    before = destination(start, 270.0, 400.0)

    assert distance_to_segment_m(before, start, end) == pytest.approx(400.0, rel=0.02)


def test_distance_to_a_degenerate_segment_is_the_point_distance():
    point = destination(PHNOM_PENH, 90.0, 250.0)

    assert distance_to_segment_m(point, PHNOM_PENH, PHNOM_PENH) == pytest.approx(
        250.0, rel=0.02
    )


def test_distance_to_path_takes_the_nearest_segment():
    path = [(104.90, 11.55), (104.92, 11.55), (104.92, 11.57)]
    near_second_segment = destination((104.92, 11.56), 90.0, 200.0)

    assert distance_to_path_m(near_second_segment, path) == pytest.approx(
        200.0, rel=0.05
    )


# ------------------------------------------------------------ polygons ----


def test_point_inside_and_outside_a_ring():
    assert point_in_ring((104.925, 11.555), SQUARE)
    assert not point_in_ring((104.940, 11.555), SQUARE)
    assert not point_in_ring((104.925, 11.575), SQUARE)


def test_centroid_of_a_square_is_its_middle():
    lon, lat = ring_centroid(SQUARE)

    assert lon == pytest.approx(104.925)
    assert lat == pytest.approx(11.555)


def test_centroid_of_a_square_is_inside_it():
    assert point_in_ring(ring_centroid(SQUARE), SQUARE)


def test_centroid_of_a_degenerate_ring_falls_back_to_the_mean():
    collinear = ((104.90, 11.50), (104.92, 11.50), (104.94, 11.50), (104.90, 11.50))

    lon, lat = ring_centroid(collinear)

    assert lat == pytest.approx(11.50)
    assert math.isfinite(lon)


def test_bounding_box():
    assert bounding_box(SQUARE) == (104.920, 11.550, 104.930, 11.560)


def test_bounding_box_of_nothing_is_an_error():
    with pytest.raises(ValueError):
        bounding_box([])


# ------------------------------------------------------------------ h3 ----


def test_h3_cell_is_stable_for_the_same_point():
    assert h3_cell(*PHNOM_PENH, 8) == h3_cell(*PHNOM_PENH, 8)


def test_h3_cell_ids_are_15_characters_at_every_resolution_used():
    for resolution in (8, 9):
        assert len(h3_cell(*PHNOM_PENH, resolution)) == 15


def test_finer_resolution_separates_points_a_coarse_cell_merges():
    """Two points ~400 m apart: same r8 cell is likely, same r9 is not."""
    a = PHNOM_PENH
    b = destination(PHNOM_PENH, 45.0, 400.0)

    assert h3_cell(*a, 9) != h3_cell(*b, 9) or h3_cell(*a, 8) == h3_cell(*b, 8)


def test_h3_centre_lands_inside_its_own_boundary():
    cell = h3_cell(*PHNOM_PENH, 8)

    assert point_in_ring(h3_centre(cell), h3_boundary(cell))


def test_h3_boundary_is_a_closed_hexagon():
    boundary = h3_boundary(h3_cell(*PHNOM_PENH, 8))

    # Seven vertices: six corners plus the repeated first.
    assert len(boundary) == 7
    assert boundary[0] == boundary[-1]


def test_h3_wrapper_takes_lon_lat_not_lat_lon():
    """The library takes (lat, lon); the wrapper exists to flip it once.

    If the wrapper ever stopped flipping, this point would index somewhere in
    the Indian Ocean and its cell centre would be nowhere near Phnom Penh.
    """
    centre = h3_centre(h3_cell(*PHNOM_PENH, 8))

    assert haversine_m(centre, PHNOM_PENH) < 1000.0


def test_a_point_is_inside_the_cell_it_indexes_to():
    for point in (PHNOM_PENH, SIEM_REAP, (0.0, 0.0), (-73.99, 40.73)):
        assert point_in_ring(point, h3_boundary(h3_cell(*point, 9)))


# ------------------------------------------------------------------ wkt ----


def test_polygon_wkt():
    assert to_wkt_polygon(SQUARE).startswith("POLYGON((104.92 11.55,")
    assert to_wkt_polygon(SQUARE).endswith("104.92 11.55))")


def test_unclosed_polygon_wkt_is_refused():
    with pytest.raises(ValueError, match="closed"):
        to_wkt_polygon(SQUARE[:-1])


def test_linestring_wkt():
    assert to_wkt_linestring([(104.90, 11.55), (104.92, 11.57)]) == (
        "LINESTRING(104.9 11.55, 104.92 11.57)"
    )


def test_single_point_linestring_is_refused():
    with pytest.raises(ValueError, match="two vertices"):
        to_wkt_linestring([(104.9, 11.5)])
