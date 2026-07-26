"""Render the fleet picture as a single self-contained HTML file.

No tile server, no CDN, no JavaScript library. The output is one file that
opens offline and still works in five years, which a page that fetches
Leaflet and a basemap at load time does not.

That trade is deliberate. What a basemap would add is streets to recognise;
what it would cost is a network dependency, an API key, and a picture that
silently changes underneath the numbers it is drawn beside. The geofences,
the planned routes and the actual tracks are the things this project has
opinions about, and they are all drawn from the warehouse -- the same tables
the CLI reports read, so the map and the numbers cannot disagree.

The H3 hexagons are drawn with real cell geometry: `mart.agg_h3_activity`
stores the cell id, and the h3 library turns it back into a boundary here.
That is the same grid the aggregate is computed on, not an approximation of
it drawn as circles.
"""

from __future__ import annotations

import html
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Sequence

import psycopg

from . import report
from .geo import Coordinate, h3_boundary

WIDTH = 1180
HEIGHT = 820
PADDING = 46

ZONE_STYLE: dict[str, tuple[str, str]] = {
    # kind -> (fill, stroke)
    "depot": ("var(--depot-fill)", "var(--depot-line)"),
    "customer": ("var(--customer-fill)", "var(--customer-line)"),
    "restricted": ("var(--restricted-fill)", "var(--restricted-line)"),
    "congestion": ("var(--congestion-fill)", "var(--congestion-line)"),
}

SEVERITY_COLOUR = {
    "critical": "var(--sev-critical)",
    "warning": "var(--sev-warning)",
    "info": "var(--sev-info)",
}

TRACK_COLOURS = (
    "#3b82f6", "#8b5cf6", "#ec4899", "#14b8a6", "#f97316", "#22c55e",
    "#0ea5e9", "#a855f7", "#ef4444", "#84cc16", "#06b6d4", "#eab308",
)


@dataclass
class _Frame:
    """Maps (lon, lat) to SVG user units."""

    project: Callable[[Coordinate], tuple[float, float]]
    metres_per_unit: float


def _mercator_y(lat: float) -> float:
    """Web Mercator northing, in the same units as longitude degrees.

    Plain latitude would do at city scale, but it makes every shape a few
    percent too tall in a way that is invisible until someone measures a
    circle on the picture and finds it is an ellipse.
    """
    clamped = max(-85.05, min(85.05, lat))
    return math.degrees(
        math.log(math.tan(math.pi / 4 + math.radians(clamped) / 2))
    )


def _build_frame(points: Sequence[Coordinate]) -> _Frame:
    if not points:
        raise ValueError("nothing to draw")

    xs = [lon for lon, _ in points]
    ys = [_mercator_y(lat) for _, lat in points]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)

    # A degenerate extent (one point, or everything on one line) would divide
    # by zero below; give it an arbitrary but sane span instead.
    span_x = max(max_x - min_x, 1e-6)
    span_y = max(max_y - min_y, 1e-6)

    inner_w = WIDTH - 2 * PADDING
    inner_h = HEIGHT - 2 * PADDING
    # One scale for both axes, so the shapes keep their proportions.
    scale = min(inner_w / span_x, inner_h / span_y)

    offset_x = PADDING + (inner_w - span_x * scale) / 2
    offset_y = PADDING + (inner_h - span_y * scale) / 2

    def project(coordinate: Coordinate) -> tuple[float, float]:
        lon, lat = coordinate
        x = offset_x + (lon - min_x) * scale
        # SVG y grows downwards; north must be up.
        y = offset_y + (max_y - _mercator_y(lat)) * scale
        return x, y

    # Metres per SVG unit at the centre of the frame, for the scale bar.
    mid_lat = (min_y + max_y) / 2
    mid_lat_deg = math.degrees(2 * math.atan(math.exp(math.radians(mid_lat)))
                               - math.pi / 2)
    metres_per_degree_lon = 111_320.0 * math.cos(math.radians(mid_lat_deg))
    return _Frame(project=project, metres_per_unit=metres_per_degree_lon / scale)


def _path(frame: _Frame, ring: Iterable[Coordinate], close: bool) -> str:
    points = [frame.project(c) for c in ring]
    if not points:
        return ""
    body = " ".join(
        f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}"
        for i, (x, y) in enumerate(points)
    )
    return body + (" Z" if close else "")


def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _geojson_rings(raw: str | None) -> list[list[Coordinate]]:
    """Rings or lines out of a GeoJSON string, as lists of (lon, lat)."""
    if not raw:
        return []
    doc = json.loads(raw)
    kind = doc.get("type")
    coordinates = doc.get("coordinates") or []
    if kind == "Polygon":
        return [[(float(x), float(y)) for x, y in ring] for ring in coordinates]
    if kind == "LineString":
        return [[(float(x), float(y)) for x, y in coordinates]]
    return []


# ------------------------------------------------------------- gathering ----


@dataclass
class MapData:
    zones: list[dict]
    routes: list[dict]
    tracks: list[dict]
    alerts: list[dict]
    cells: list[dict]
    generated_at: datetime

    def all_points(self) -> list[Coordinate]:
        points: list[Coordinate] = []
        for zone in self.zones:
            for ring in _geojson_rings(zone["boundary_geojson"]):
                points.extend(ring)
        for route in self.routes:
            for line in _geojson_rings(route["path_geojson"]):
                points.extend(line)
        for track in self.tracks:
            points.extend(zip(track["lons"], track["lats"]))
        for alert in self.alerts:
            points.append((float(alert["lon"]), float(alert["lat"])))
        return points


def gather(conn: psycopg.Connection, *, hours: int = 6) -> MapData:
    return MapData(
        zones=report.map_zones(conn),
        routes=report.map_routes(conn),
        tracks=report.map_tracks(conn, hours=hours),
        alerts=report.map_alerts(conn, hours=hours),
        cells=report.map_cells(conn),
        generated_at=datetime.now(timezone.utc),
    )


# -------------------------------------------------------------- drawing ----


def _draw_cells(frame: _Frame, cells: list[dict]) -> str:
    if not cells:
        return ""
    busiest = max(int(c["pings"]) for c in cells) or 1
    parts: list[str] = []
    for cell in cells:
        try:
            boundary = h3_boundary(cell["h3_r8"])
        except (ValueError, TypeError):  # pragma: no cover - malformed cell id
            continue
        # Square-root so the quiet cells stay visible. A linear ramp against
        # a depot that dwarfs everything else leaves the rest of the city
        # rendered as blank paper.
        weight = math.sqrt(int(cell["pings"]) / busiest)
        opacity = 0.08 + 0.55 * weight
        stopped = cell["stopped_hours"] or 0
        parts.append(
            f'<path class="cell" d="{_path(frame, boundary, True)}" '
            f'fill-opacity="{opacity:.3f}">'
            f"<title>{_esc(cell['h3_r8'])}\n"
            f"{int(cell['pings']):,} pings, {int(cell['vehicles'])} vehicles\n"
            f"{float(stopped):.2f} h stopped, "
            f"avg {_esc(cell['avg_moving_kph'])} kph moving</title></path>"
        )
    return "\n".join(parts)


def _draw_zones(frame: _Frame, zones: list[dict]) -> str:
    parts: list[str] = []
    for zone in zones:
        fill, stroke = ZONE_STYLE.get(
            zone["zone_kind"], ("var(--zone-fill)", "var(--zone-line)")
        )
        for ring in _geojson_rings(zone["boundary_geojson"]):
            parts.append(
                f'<path class="zone" d="{_path(frame, ring, True)}" '
                f'fill="{fill}" stroke="{stroke}">'
                f"<title>{_esc(zone['name'])} ({_esc(zone['zone_kind'])})\n"
                f"{int(zone['visits'])} visits, "
                f"{int(zone['breaches'])} breach(es)</title></path>"
            )
        x, y = frame.project((float(zone["centre_lon"]), float(zone["centre_lat"])))
        parts.append(
            f'<text class="zone-label" x="{x:.1f}" y="{y:.1f}">'
            f"{_esc(zone['name'])}</text>"
        )
    return "\n".join(parts)


def _draw_routes(frame: _Frame, routes: list[dict]) -> str:
    parts: list[str] = []
    for route in routes:
        for line in _geojson_rings(route["path_geojson"]):
            parts.append(
                f'<path class="route" d="{_path(frame, line, False)}">'
                f"<title>{_esc(route['name'])} "
                f"({_esc(route['length_km'])} km planned)</title></path>"
            )
    return "\n".join(parts)


def _draw_tracks(frame: _Frame, tracks: list[dict]) -> str:
    parts: list[str] = []
    for i, track in enumerate(tracks):
        points = list(zip(track["lons"], track["lats"]))
        if len(points) < 2:
            continue
        colour = TRACK_COLOURS[i % len(TRACK_COLOURS)]
        parts.append(
            f'<path class="track" stroke="{colour}" '
            f'd="{_path(frame, points, False)}">'
            f"<title>{_esc(track['plate'])} "
            f"({_esc(track['vehicle_id'])}, {_esc(track['vehicle_type'])})\n"
            f"{len(points)} plotted positions</title></path>"
        )
        # Where the vehicle was last seen, which is the thing an operator
        # looks for first.
        x, y = frame.project(points[-1])
        parts.append(
            f'<circle class="vehicle" cx="{x:.1f}" cy="{y:.1f}" r="4.5" '
            f'fill="{colour}"><title>{_esc(track["plate"])} '
            f"(last plotted position)</title></circle>"
        )
    return "\n".join(parts)


def _draw_alerts(frame: _Frame, alerts: list[dict]) -> str:
    parts: list[str] = []
    for alert in alerts:
        x, y = frame.project((float(alert["lon"]), float(alert["lat"])))
        colour = SEVERITY_COLOUR.get(alert["severity"], "var(--sev-info)")
        started = alert["started_at"]
        detail = (
            f"{_esc(alert['detection_type'])} -- {_esc(alert['severity'])}\n"
            f"{_esc(alert['plate'] or alert['vehicle_id'])}"
            f"{' at ' + _esc(alert['zone_name']) if alert['zone_name'] else ''}\n"
            f"{started:%Y-%m-%d %H:%M} UTC"
            f"{'  (still open)' if alert['is_open'] else ''}\n"
            f"magnitude {_esc(alert['magnitude'])}"
        )
        # A ring rather than a filled dot: alerts cluster, and filled dots at
        # the same place become one opaque blob that hides how many there are.
        parts.append(
            f'<circle class="alert" cx="{x:.1f}" cy="{y:.1f}" r="6" '
            f'stroke="{colour}"><title>{detail}</title></circle>'
        )
    return "\n".join(parts)


def _draw_scale_bar(frame: _Frame) -> str:
    """A scale bar rounded to a human number of metres."""
    target_units = 170.0
    raw_metres = target_units * frame.metres_per_unit
    magnitude = 10 ** math.floor(math.log10(max(raw_metres, 1.0)))
    for step in (1, 2, 5, 10):
        nice = step * magnitude
        if nice >= raw_metres:
            break
    length = nice / frame.metres_per_unit
    x0 = PADDING
    y0 = HEIGHT - 22
    label = f"{nice / 1000:g} km" if nice >= 1000 else f"{nice:g} m"
    return (
        f'<g class="scale">'
        f'<line x1="{x0}" y1="{y0}" x2="{x0 + length:.1f}" y2="{y0}"/>'
        f'<line x1="{x0}" y1="{y0 - 5}" x2="{x0}" y2="{y0 + 5}"/>'
        f'<line x1="{x0 + length:.1f}" y1="{y0 - 5}" '
        f'x2="{x0 + length:.1f}" y2="{y0 + 5}"/>'
        f'<text x="{x0 + length / 2:.1f}" y="{y0 - 9}">{label}</text>'
        f"</g>"
    )


_CSS = """
:root {
  color-scheme: light dark;
  --bg: #f7f8fa; --panel: #ffffff; --ink: #14181f; --muted: #667085;
  --border: #dfe3ea; --map-bg: #eef1f6;
  --depot-fill: #64748b; --depot-line: #475569;
  --customer-fill: #22c55e; --customer-line: #15803d;
  --restricted-fill: #ef4444; --restricted-line: #b91c1c;
  --congestion-fill: #f59e0b; --congestion-line: #b45309;
  --zone-fill: #94a3b8; --zone-line: #64748b;
  --route: #1f2937; --cell: #6366f1;
  --sev-critical: #dc2626; --sev-warning: #f59e0b; --sev-info: #0ea5e9;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0d1117; --panel: #161b22; --ink: #e6edf3; --muted: #8b949e;
    --border: #30363d; --map-bg: #0b1020;
    --route: #cbd5e1; --cell: #818cf8;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 24px; background: var(--bg); color: var(--ink);
  font: 14px/1.5 ui-sans-serif, -apple-system, "Segoe UI", Roboto, sans-serif;
}
.wrap { max-width: 1240px; margin: 0 auto; }
h1 { font-size: 20px; margin: 0 0 4px; }
.sub { color: var(--muted); margin: 0 0 18px; font-size: 13px; }
.panel {
  background: var(--panel); border: 1px solid var(--border);
  border-radius: 12px; padding: 14px;
}
.controls {
  display: flex; flex-wrap: wrap; gap: 8px 18px; margin-bottom: 12px;
  font-size: 13px;
}
.controls label { display: flex; align-items: center; gap: 6px; cursor: pointer; }
.mapbox { overflow-x: auto; background: var(--map-bg); border-radius: 8px; }
svg { display: block; width: 100%; height: auto; min-width: 720px; }
.zone { stroke-width: 1.4; fill-opacity: 0.18; }
.zone-label {
  font-size: 10px; fill: var(--muted); text-anchor: middle;
  paint-order: stroke; stroke: var(--map-bg); stroke-width: 3px;
}
.route {
  fill: none; stroke: var(--route); stroke-width: 2.2; stroke-opacity: 0.55;
  stroke-dasharray: 7 5; stroke-linecap: round;
}
.track {
  fill: none; stroke-width: 1.9; stroke-opacity: 0.85;
  stroke-linejoin: round; stroke-linecap: round;
}
.vehicle { stroke: var(--panel); stroke-width: 1.5; }
.alert { fill: none; stroke-width: 2.4; }
.cell { fill: var(--cell); stroke: none; }
.scale line { stroke: var(--muted); stroke-width: 1.4; }
.scale text { fill: var(--muted); font-size: 11px; text-anchor: middle; }
.legend { display: flex; flex-wrap: wrap; gap: 6px 16px; margin-top: 12px;
          font-size: 12px; color: var(--muted); }
.legend span { display: flex; align-items: center; gap: 6px; }
.swatch { width: 13px; height: 13px; border-radius: 3px; display: inline-block; }
.stats {
  display: flex; flex-wrap: wrap; gap: 22px; margin-top: 16px;
  padding-top: 14px; border-top: 1px solid var(--border);
}
.stat b { display: block; font-size: 20px; font-weight: 600; }
.stat span { color: var(--muted); font-size: 12px; }
.hidden { display: none; }
footer { color: var(--muted); font-size: 12px; margin-top: 16px; }
"""

_JS = """
for (const box of document.querySelectorAll('input[data-layer]')) {
  box.addEventListener('change', () => {
    const layer = document.getElementById(box.dataset.layer);
    if (layer) layer.classList.toggle('hidden', !box.checked);
  });
}
"""


def _legend() -> str:
    entries = [
        ("var(--depot-fill)", "depot"),
        ("var(--customer-fill)", "customer site"),
        ("var(--restricted-fill)", "restricted"),
        ("var(--congestion-fill)", "congestion corridor"),
        ("var(--cell)", "H3 r8 density"),
        ("var(--sev-critical)", "critical alert"),
        ("var(--sev-warning)", "warning"),
        ("var(--sev-info)", "info"),
    ]
    swatches = "".join(
        f'<span><i class="swatch" style="background:{colour}"></i>{label}</span>'
        for colour, label in entries
    )
    return f'<div class="legend">{swatches}</div>'


def _stats(data: MapData) -> str:
    critical = sum(1 for a in data.alerts if a["severity"] == "critical")
    tiles = [
        (f"{len(data.tracks)}", "vehicles tracked"),
        (f"{len(data.zones)}", "geofences"),
        (f"{len(data.routes)}", "routes"),
        (f"{len(data.cells):,}", "H3 cells"),
        (f"{len(data.alerts):,}", "alerts plotted"),
        (f"{critical:,}", "critical"),
    ]
    body = "".join(
        f'<div class="stat"><b>{value}</b><span>{label}</span></div>'
        for value, label in tiles
    )
    return f'<div class="stats">{body}</div>'


def render(data: MapData, *, hours: int) -> str:
    points = data.all_points()
    if not points:
        return (
            "<!doctype html><meta charset='utf-8'>"
            "<title>Fleet map</title>"
            "<body style='font-family:sans-serif;padding:40px'>"
            "<h1>Nothing to draw</h1>"
            "<p>No geofences, routes or telemetry found. Run "
            "<code>python -m fleet seed</code> and "
            "<code>python -m fleet pipeline</code> first.</p>"
        )

    frame = _build_frame(points)
    layers = [
        ("cells", "H3 density", _draw_cells(frame, data.cells)),
        ("zones", "Geofences", _draw_zones(frame, data.zones)),
        ("routes", "Planned routes", _draw_routes(frame, data.routes)),
        ("tracks", "Vehicle tracks", _draw_tracks(frame, data.tracks)),
        ("alerts", "Alerts", _draw_alerts(frame, data.alerts)),
    ]
    groups = "\n".join(
        f'<g id="layer-{key}">\n{body}\n</g>' for key, _, body in layers
    )
    controls = "".join(
        f'<label><input type="checkbox" data-layer="layer-{key}" checked> '
        f"{label}</label>"
        for key, label, _ in layers
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fleet movement map</title>
<style>{_CSS}</style>
</head>
<body>
<div class="wrap">
  <h1>Fleet movement map</h1>
  <p class="sub">
    Last {hours} hour(s) of telemetry, drawn from the warehouse.
    Generated {data.generated_at:%Y-%m-%d %H:%M} UTC.
    Hover any shape for detail.
  </p>
  <div class="panel">
    <div class="controls">{controls}</div>
    <div class="mapbox">
      <svg viewBox="0 0 {WIDTH} {HEIGHT}" xmlns="http://www.w3.org/2000/svg"
           role="img" aria-label="Map of fleet geofences, routes and tracks">
        {groups}
        {_draw_scale_bar(frame)}
      </svg>
    </div>
    {_legend()}
    {_stats(data)}
  </div>
  <footer>
    No basemap and no network requests: every shape here comes from
    mart.dim_zone, mart.dim_route, mart.agg_h3_activity, mart.fct_alerts and
    stg.stg_pings. North is up; the scale bar is measured at the centre of
    the frame.
  </footer>
</div>
<script>{_JS}</script>
</body>
</html>
"""


def build_map(conn: psycopg.Connection, *, hours: int = 6) -> str:
    """Gather from the warehouse and render the HTML."""
    return render(gather(conn, hours=hours), hours=hours)
