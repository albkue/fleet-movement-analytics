"""Pure-Python geometry: great-circle maths, polyline walking, H3 indexing.

Scope note, because the split matters. Nothing in this module is used to
*decide* anything about a real ping. Geofence membership, distance to route
and progress along route are all computed by PostGIS in fleet/enrich.py,
where the polygons live and where the work is set-based.

What lives here is:

  * the H3 index attached to a ping at ingest, which is a pure function of
    (lat, lon) and has no reason to make a database round trip;
  * the simulator's own geometry, which has to synthesise plausible movement
    before any database exists to ask;
  * a planar point-in-ring test used only by the simulator to decide where a
    stop is. PostGIS treats polygon edges as geodesics and this treats them
    as straight lines in lon/lat; over a city block the two differ by
    centimetres, but only the PostGIS answer is ever written down.

Coordinates are (lon, lat) throughout, matching GeoJSON and PostGIS argument
order. Distances are metres.
"""

from __future__ import annotations

import math
from typing import Iterable, Sequence

# Mean Earth radius (IUGG). Good to ~0.3% anywhere, which is far below the
# GPS error this pipeline models in the first place.
EARTH_RADIUS_M = 6_371_008.8

Coordinate = tuple[float, float]

# h3 4.x renamed most of its API; 3.x is still widely installed. The shim
# keeps one name for the rest of the project rather than sprinkling
# hasattr checks through the ingest path.
try:  # pragma: no cover - exercised by whichever version is installed
    import h3 as _h3

    if hasattr(_h3, "latlng_to_cell"):  # 4.x
        _latlng_to_cell = _h3.latlng_to_cell
        _cell_to_latlng = _h3.cell_to_latlng
        _cell_to_boundary = _h3.cell_to_boundary
    else:  # 3.x
        _latlng_to_cell = _h3.geo_to_h3
        _cell_to_latlng = _h3.h3_to_geo
        _cell_to_boundary = _h3.h3_to_geo_boundary
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "the 'h3' package is required; install it with "
        "`pip install -r requirements.txt`"
    ) from exc


def h3_cell(lon: float, lat: float, resolution: int) -> str:
    """Index a point into the H3 grid.

    Argument order is (lon, lat) for consistency with the rest of this
    module; the h3 library itself takes (lat, lon), which is exactly the kind
    of silent transposition this wrapper exists to prevent happening in
    twenty different call sites.
    """
    return str(_latlng_to_cell(lat, lon, resolution))


def h3_centre(cell: str) -> Coordinate:
    """Centre of an H3 cell as (lon, lat)."""
    lat, lon = _cell_to_latlng(cell)
    return float(lon), float(lat)


def h3_boundary(cell: str) -> tuple[Coordinate, ...]:
    """Hexagon vertices of an H3 cell as (lon, lat), closed."""
    ring = [(float(lon), float(lat)) for lat, lon in _cell_to_boundary(cell)]
    return tuple(ring + [ring[0]])


# ------------------------------------------------------- great circle ----


def haversine_m(a: Coordinate, b: Coordinate) -> float:
    """Great-circle distance between two (lon, lat) points, in metres."""
    lon1, lat1 = a
    lon2, lat2 = b
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dlambda = math.radians(lon2 - lon1)

    h = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(min(1.0, h)))


def bearing_deg(a: Coordinate, b: Coordinate) -> float:
    """Initial bearing from a to b, in degrees clockwise from north."""
    lon1, lat1 = a
    lon2, lat2 = b
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)

    y = math.sin(dlambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(
        dlambda
    )
    return math.degrees(math.atan2(y, x)) % 360.0


def destination(origin: Coordinate, bearing: float, distance_m: float) -> Coordinate:
    """Point reached by travelling `distance_m` from `origin` on `bearing`."""
    lon1, lat1 = origin
    phi1 = math.radians(lat1)
    lambda1 = math.radians(lon1)
    theta = math.radians(bearing)
    delta = distance_m / EARTH_RADIUS_M

    phi2 = math.asin(
        math.sin(phi1) * math.cos(delta)
        + math.cos(phi1) * math.sin(delta) * math.cos(theta)
    )
    lambda2 = lambda1 + math.atan2(
        math.sin(theta) * math.sin(delta) * math.cos(phi1),
        math.cos(delta) - math.sin(phi1) * math.sin(phi2),
    )
    # Normalise back into [-180, 180) so a track crossing the antimeridian
    # does not produce a longitude of 190.
    return (math.degrees(lambda2) + 540.0) % 360.0 - 180.0, math.degrees(phi2)


# ---------------------------------------------------------- polylines ----


def path_length_m(path: Sequence[Coordinate]) -> float:
    """Total length of a polyline."""
    return sum(haversine_m(path[i], path[i + 1]) for i in range(len(path) - 1))


def cumulative_lengths_m(path: Sequence[Coordinate]) -> list[float]:
    """Distance from the start of the polyline to each vertex."""
    out = [0.0]
    for i in range(len(path) - 1):
        out.append(out[-1] + haversine_m(path[i], path[i + 1]))
    return out


def point_at_distance(
    path: Sequence[Coordinate], distance_m: float
) -> tuple[Coordinate, float]:
    """Walk `distance_m` along a polyline.

    Returns the position and the bearing of the segment it landed on, so a
    caller simulating a vehicle gets a heading for free rather than having to
    re-derive it from two successive positions (which is noisy when the
    vehicle is barely moving).

    Distances beyond either end clamp to that end.
    """
    if len(path) < 2:
        raise ValueError("a polyline needs at least two vertices")

    cumulative = cumulative_lengths_m(path)
    total = cumulative[-1]
    target = max(0.0, min(distance_m, total))

    for i in range(len(path) - 1):
        if target <= cumulative[i + 1] or i == len(path) - 2:
            segment_length = cumulative[i + 1] - cumulative[i]
            heading = bearing_deg(path[i], path[i + 1])
            if segment_length <= 0:
                return path[i], heading
            along = target - cumulative[i]
            return destination(path[i], heading, along), heading

    return path[-1], bearing_deg(path[-2], path[-1])  # pragma: no cover


def distance_to_segment_m(
    point: Coordinate, start: Coordinate, end: Coordinate
) -> float:
    """Shortest distance from a point to a line segment.

    Projects onto a local tangent plane (metres east/north of the segment
    start) before doing the flat-plane projection. Over a segment of a few
    kilometres the curvature error is well under a metre.
    """
    lon0, lat0 = start
    scale_lat = math.pi * EARTH_RADIUS_M / 180.0
    scale_lon = scale_lat * math.cos(math.radians(lat0))

    def to_plane(c: Coordinate) -> tuple[float, float]:
        return (c[0] - lon0) * scale_lon, (c[1] - lat0) * scale_lat

    px, py = to_plane(point)
    ex, ey = to_plane(end)

    segment_sq = ex * ex + ey * ey
    if segment_sq == 0:
        return math.hypot(px, py)

    t = max(0.0, min(1.0, (px * ex + py * ey) / segment_sq))
    return math.hypot(px - t * ex, py - t * ey)


def distance_to_path_m(point: Coordinate, path: Sequence[Coordinate]) -> float:
    """Shortest distance from a point to a polyline."""
    return min(
        distance_to_segment_m(point, path[i], path[i + 1])
        for i in range(len(path) - 1)
    )


# ------------------------------------------------------------ polygons ----


def point_in_ring(point: Coordinate, ring: Sequence[Coordinate]) -> bool:
    """Planar ray-casting point-in-polygon test.

    Simulator-only; see the module docstring. A point exactly on an edge is
    not guaranteed either answer, which is acceptable because the simulator
    only asks about points it placed near a zone *centre*.
    """
    lon, lat = point
    inside = False
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        # Does the edge straddle the horizontal ray, and is the crossing to
        # the right of the point?
        if (y1 > lat) != (y2 > lat):
            x_at_lat = x1 + (lat - y1) * (x2 - x1) / (y2 - y1)
            if x_at_lat > lon:
                inside = not inside
    return inside


def ring_centroid(ring: Sequence[Coordinate]) -> Coordinate:
    """Area centroid of a closed ring, in (lon, lat)."""
    area = 0.0
    cx = 0.0
    cy = 0.0
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        cross = x1 * y2 - x2 * y1
        area += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross

    if area == 0:
        # Degenerate ring (collinear vertices): fall back to the mean vertex.
        xs = [c[0] for c in ring[:-1]]
        ys = [c[1] for c in ring[:-1]]
        return sum(xs) / len(xs), sum(ys) / len(ys)

    return cx / (3 * area), cy / (3 * area)


def bounding_box(points: Iterable[Coordinate]) -> tuple[float, float, float, float]:
    """(min_lon, min_lat, max_lon, max_lat) of an iterable of points."""
    lons: list[float] = []
    lats: list[float] = []
    for lon, lat in points:
        lons.append(lon)
        lats.append(lat)
    if not lons:
        raise ValueError("cannot take the bounding box of no points")
    return min(lons), min(lats), max(lons), max(lats)


def to_wkt_polygon(ring: Sequence[Coordinate]) -> str:
    """Closed ring -> WKT POLYGON, for handing a zone to PostGIS."""
    if ring[0] != ring[-1]:
        raise ValueError("polygon ring must be closed")
    body = ", ".join(f"{lon} {lat}" for lon, lat in ring)
    return f"POLYGON(({body}))"


def to_wkt_linestring(path: Sequence[Coordinate]) -> str:
    """Polyline -> WKT LINESTRING, for handing a route to PostGIS."""
    if len(path) < 2:
        raise ValueError("linestring needs at least two vertices")
    body = ", ".join(f"{lon} {lat}" for lon, lat in path)
    return f"LINESTRING({body})"
