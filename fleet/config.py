"""Configuration: environment settings and the fleet reference data.

Two separate things live here on purpose.

`Settings` is addresses and thresholds -- where Kafka is, how long a vehicle
has to sit still before it counts as idle. It comes from the environment.

`FleetConfig` is the world the vehicles move in -- the geofences, the routes,
the vehicles themselves. It comes from config/fleet.json, is validated hard on
load, and is seeded into ref.* so that SQL can join to it.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

ZONE_KINDS: frozenset[str] = frozenset(
    {"depot", "customer", "restricted", "congestion"}
)
VEHICLE_TYPES: frozenset[str] = frozenset({"van", "truck", "motorbike"})

# Every detection the stream processor can emit. This is the one place the
# vocabulary is declared: the detector writes these, the SQL check constraint
# mirrors them, and the reports rank severity from them.
#
# Note what is *not* here: there is no "geofence_exit". A zone visit is one
# episode with an entry, an exit and a dwell, so it is one row that gets
# closed -- not an enter row and an exit row that a reader has to pair back
# up by hand and that go wrong the moment one of them is missing.
DETECTION_TYPES: tuple[str, ...] = (
    "zone_visit",
    "geofence_breach",
    "idle",
    "route_deviation",
    "delay",
    "gps_gap",
)

SEVERITIES: tuple[str, ...] = ("info", "warning", "critical")


class ConfigError(ValueError):
    """The fleet reference data is malformed."""


# ------------------------------------------------------- reference data ----


@dataclass(frozen=True)
class Zone:
    zone_id: str
    name: str
    zone_kind: str
    max_dwell_minutes: int | None
    # Closed ring of (lon, lat) vertices; first == last.
    boundary: tuple[tuple[float, float], ...]

    @property
    def is_geofence_violation_on_entry(self) -> bool:
        """A restricted zone is a breach the moment it is entered.

        Every other kind is only a breach if the vehicle *lingers*, which is
        a different test with a different threshold.
        """
        return self.zone_kind == "restricted"


@dataclass(frozen=True)
class Route:
    route_id: str
    name: str
    start_zone_id: str
    end_zone_id: str
    planned_duration_minutes: int
    service_minutes: int
    stop_zone_ids: tuple[str, ...]
    path: tuple[tuple[float, float], ...]


@dataclass(frozen=True)
class Vehicle:
    vehicle_id: str
    plate: str
    vehicle_type: str
    capacity_kg: int
    home_depot_id: str


@dataclass(frozen=True)
class FleetConfig:
    zones: tuple[Zone, ...]
    routes: tuple[Route, ...]
    vehicles: tuple[Vehicle, ...]

    def zone(self, zone_id: str) -> Zone:
        for zone in self.zones:
            if zone.zone_id == zone_id:
                return zone
        raise KeyError(zone_id)

    def route(self, route_id: str) -> Route:
        for route in self.routes:
            if route.route_id == route_id:
                return route
        raise KeyError(route_id)

    def routes_from(self, depot_id: str) -> list[Route]:
        """Routes a vehicle homed at `depot_id` can actually start."""
        return [r for r in self.routes if r.start_zone_id == depot_id]

    def zones_of_kind(self, kind: str) -> list[Zone]:
        return [z for z in self.zones if z.zone_kind == kind]


# ------------------------------------------------------------ settings ----


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_database: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_consumer_group: str
    kafka_topic_partitions: int
    consumer_batch_size: int
    consumer_batch_timeout_seconds: float
    consumer_idle_timeout_seconds: float
    idle_speed_kph: float
    idle_minutes: int
    deviation_metres: float
    deviation_seconds: int
    delay_minutes: int
    delay_restep_minutes: int
    gps_gap_minutes: int
    h3_resolution_coarse: int
    h3_resolution_fine: int
    models_dir: Path
    fleet_config_file: Path
    ping_interval_seconds: int
    simulator_seed: int | None

    @property
    def dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} user={self.pg_user} "
            f"password={self.pg_password} dbname={self.pg_database}"
        )


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def _path_env(name: str, default: str) -> Path:
    path = Path(os.getenv(name) or default)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _positive(name: str, value: float) -> float:
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def load_settings() -> Settings:
    """Read .env (if present) plus the real environment into a Settings object."""
    load_dotenv(PROJECT_ROOT / ".env")

    seed_raw = os.getenv("SIMULATOR_SEED", "").strip()

    coarse = _int_env("H3_RESOLUTION_COARSE", 8)
    fine = _int_env("H3_RESOLUTION_FINE", 9)
    for name, res in (("H3_RESOLUTION_COARSE", coarse), ("H3_RESOLUTION_FINE", fine)):
        if not 0 <= res <= 15:
            raise ValueError(f"{name} must be between 0 and 15, got {res}")
    if fine <= coarse:
        # Two resolutions only earn their storage if one is genuinely finer;
        # equal values would just duplicate a column.
        raise ValueError(
            f"H3_RESOLUTION_FINE ({fine}) must be greater than "
            f"H3_RESOLUTION_COARSE ({coarse})"
        )

    return Settings(
        pg_host=os.getenv("POSTGRES_HOST", "localhost"),
        pg_port=_int_env("POSTGRES_PORT", 5436),
        pg_user=os.getenv("POSTGRES_USER", "fleet"),
        pg_password=os.getenv("POSTGRES_PASSWORD", "fleet"),
        pg_database=os.getenv("POSTGRES_DB", "fleet"),
        kafka_bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9096"
        ),
        kafka_topic=os.getenv("KAFKA_TOPIC", "fleet.telemetry"),
        kafka_consumer_group=os.getenv(
            "KAFKA_CONSUMER_GROUP", "fleet-stream-processor"
        ),
        kafka_topic_partitions=int(
            _positive("KAFKA_TOPIC_PARTITIONS", _int_env("KAFKA_TOPIC_PARTITIONS", 12))
        ),
        consumer_batch_size=int(
            _positive("CONSUMER_BATCH_SIZE", _int_env("CONSUMER_BATCH_SIZE", 500))
        ),
        consumer_batch_timeout_seconds=_float_env(
            "CONSUMER_BATCH_TIMEOUT_SECONDS", 5.0
        ),
        consumer_idle_timeout_seconds=_float_env(
            "CONSUMER_IDLE_TIMEOUT_SECONDS", 0.0
        ),
        idle_speed_kph=_float_env("IDLE_SPEED_KPH", 3.0),
        idle_minutes=int(_positive("IDLE_MINUTES", _int_env("IDLE_MINUTES", 5))),
        deviation_metres=_positive(
            "DEVIATION_METRES", _float_env("DEVIATION_METRES", 120.0)
        ),
        deviation_seconds=int(
            _positive("DEVIATION_SECONDS", _int_env("DEVIATION_SECONDS", 90))
        ),
        delay_minutes=int(_positive("DELAY_MINUTES", _int_env("DELAY_MINUTES", 10))),
        delay_restep_minutes=int(
            _positive(
                "DELAY_RESTEP_MINUTES", _int_env("DELAY_RESTEP_MINUTES", 10)
            )
        ),
        gps_gap_minutes=int(
            _positive("GPS_GAP_MINUTES", _int_env("GPS_GAP_MINUTES", 10))
        ),
        h3_resolution_coarse=coarse,
        h3_resolution_fine=fine,
        models_dir=_path_env("MODELS_DIR", "models"),
        fleet_config_file=_path_env("FLEET_CONFIG_FILE", "config/fleet.json"),
        ping_interval_seconds=int(
            _positive(
                "PING_INTERVAL_SECONDS", _int_env("PING_INTERVAL_SECONDS", 15)
            )
        ),
        simulator_seed=int(seed_raw) if seed_raw else None,
    )


# --------------------------------------------------------------- loader ----


def _require(entry: dict, keys: set[str], where: str) -> None:
    missing = keys - entry.keys()
    if missing:
        raise ConfigError(f"{where} is missing {sorted(missing)}")


def _coordinate(value: object, where: str) -> tuple[float, float]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ConfigError(f"{where}: expected a [lon, lat] pair, got {value!r}")
    lon, lat = float(value[0]), float(value[1])
    # The single most common way to break a geospatial project is to write
    # [lat, lon] where [lon, lat] is expected. Phnom Penh's latitude is a
    # legal longitude, so the mistake would not otherwise raise -- it would
    # just put the whole fleet in the Gulf of Guinea.
    if not -90 <= lat <= 90:
        raise ConfigError(f"{where}: latitude {lat} out of range (is it [lat, lon]?)")
    if not -180 <= lon <= 180:
        raise ConfigError(f"{where}: longitude {lon} out of range")
    return lon, lat


def _parse_zones(path: Path, entries: object) -> tuple[Zone, ...]:
    if not isinstance(entries, list) or not entries:
        raise ConfigError(f"{path} must contain a non-empty 'zones' array")

    zones: list[Zone] = []
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        where = f"{path.name} zones[{i}]"
        _require(entry, {"zone_id", "name", "zone_kind", "boundary"}, where)

        zone_id = entry["zone_id"]
        if zone_id in seen:
            raise ConfigError(f"{path.name} has duplicate zone_id {zone_id!r}")
        seen.add(zone_id)

        kind = entry["zone_kind"]
        if kind not in ZONE_KINDS:
            raise ConfigError(
                f"{where}: zone_kind must be one of {sorted(ZONE_KINDS)}, got {kind!r}"
            )

        ring = entry["boundary"]
        if not isinstance(ring, list) or len(ring) < 4:
            raise ConfigError(
                f"{where}: boundary needs at least 4 positions "
                f"(3 corners plus the repeated first)"
            )
        vertices = tuple(
            _coordinate(c, f"{where}.boundary[{j}]") for j, c in enumerate(ring)
        )
        if vertices[0] != vertices[-1]:
            # PostGIS rejects an unclosed ring anyway; failing here names the
            # file and the index instead of the SQL statement.
            raise ConfigError(f"{where}: boundary ring is not closed")

        dwell = entry.get("max_dwell_minutes")
        if dwell is not None and (not isinstance(dwell, int) or dwell <= 0):
            raise ConfigError(
                f"{where}: max_dwell_minutes must be a positive integer or null"
            )

        zones.append(
            Zone(
                zone_id=zone_id,
                name=entry["name"],
                zone_kind=kind,
                max_dwell_minutes=dwell,
                boundary=vertices,
            )
        )
    return tuple(zones)


def _parse_routes(
    path: Path, entries: object, zone_ids: set[str]
) -> tuple[Route, ...]:
    if not isinstance(entries, list) or not entries:
        raise ConfigError(f"{path} must contain a non-empty 'routes' array")

    routes: list[Route] = []
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        where = f"{path.name} routes[{i}]"
        _require(
            entry,
            {
                "route_id",
                "name",
                "start_zone_id",
                "end_zone_id",
                "planned_duration_minutes",
                "path",
            },
            where,
        )

        route_id = entry["route_id"]
        if route_id in seen:
            raise ConfigError(f"{path.name} has duplicate route_id {route_id!r}")
        seen.add(route_id)

        for key in ("start_zone_id", "end_zone_id"):
            if entry[key] not in zone_ids:
                raise ConfigError(f"{where}: {key} {entry[key]!r} is not a known zone")

        stops = tuple(entry.get("stop_zone_ids", []))
        for stop in stops:
            if stop not in zone_ids:
                raise ConfigError(f"{where}: stop zone {stop!r} is not a known zone")

        line = entry["path"]
        if not isinstance(line, list) or len(line) < 2:
            raise ConfigError(f"{where}: path needs at least 2 positions")
        vertices = tuple(
            _coordinate(c, f"{where}.path[{j}]") for j, c in enumerate(line)
        )

        duration = entry["planned_duration_minutes"]
        if not isinstance(duration, int) or duration <= 0:
            raise ConfigError(
                f"{where}: planned_duration_minutes must be a positive integer"
            )

        service = entry.get("service_minutes", 0)
        if not isinstance(service, int) or service < 0:
            raise ConfigError(f"{where}: service_minutes must be a non-negative integer")
        if service * len(stops) >= duration:
            # Otherwise the schedule is unmeetable by construction and every
            # trip would be "delayed" no matter how it was driven.
            raise ConfigError(
                f"{where}: {len(stops)} stops x {service} service minutes leaves no "
                f"time to drive within planned_duration_minutes={duration}"
            )

        routes.append(
            Route(
                route_id=route_id,
                name=entry["name"],
                start_zone_id=entry["start_zone_id"],
                end_zone_id=entry["end_zone_id"],
                planned_duration_minutes=duration,
                service_minutes=service,
                stop_zone_ids=stops,
                path=vertices,
            )
        )
    return tuple(routes)


def _parse_vehicles(
    path: Path, entries: object, depot_ids: set[str]
) -> tuple[Vehicle, ...]:
    if not isinstance(entries, list) or not entries:
        raise ConfigError(f"{path} must contain a non-empty 'vehicles' array")

    vehicles: list[Vehicle] = []
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        where = f"{path.name} vehicles[{i}]"
        _require(
            entry,
            {"vehicle_id", "plate", "vehicle_type", "capacity_kg", "home_depot_id"},
            where,
        )

        vehicle_id = entry["vehicle_id"]
        if vehicle_id in seen:
            raise ConfigError(f"{path.name} has duplicate vehicle_id {vehicle_id!r}")
        seen.add(vehicle_id)

        vehicle_type = entry["vehicle_type"]
        if vehicle_type not in VEHICLE_TYPES:
            raise ConfigError(
                f"{where}: vehicle_type must be one of {sorted(VEHICLE_TYPES)}, "
                f"got {vehicle_type!r}"
            )

        capacity = entry["capacity_kg"]
        if not isinstance(capacity, int) or capacity <= 0:
            raise ConfigError(f"{where}: capacity_kg must be a positive integer")

        depot = entry["home_depot_id"]
        if depot not in depot_ids:
            raise ConfigError(
                f"{where}: home_depot_id {depot!r} is not a zone of kind 'depot'"
            )

        vehicles.append(
            Vehicle(
                vehicle_id=vehicle_id,
                plate=entry["plate"],
                vehicle_type=vehicle_type,
                capacity_kg=capacity,
                home_depot_id=depot,
            )
        )
    return tuple(vehicles)


def load_fleet_config(path: Path) -> FleetConfig:
    """Parse and validate the fleet reference data.

    Validation is deliberately strict and structural: a zone that is not
    closed, a route that starts nowhere, a vehicle homed at a customer site.
    All of these would otherwise surface much later as an empty join or a
    detection that never fires, which is far harder to trace back.
    """
    if not path.exists():
        raise FileNotFoundError(f"Fleet config not found: {path}")

    doc = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise ConfigError(f"{path} must contain a JSON object")

    zones = _parse_zones(path, doc.get("zones"))
    zone_ids = {z.zone_id for z in zones}
    depot_ids = {z.zone_id for z in zones if z.zone_kind == "depot"}
    if not depot_ids:
        raise ConfigError(f"{path} must define at least one zone of kind 'depot'")

    routes = _parse_routes(path, doc.get("routes"), zone_ids)
    vehicles = _parse_vehicles(path, doc.get("vehicles"), depot_ids)

    # A vehicle with no route it can start would silently never appear in the
    # stream, so the fleet would look smaller than it is.
    startable = {r.start_zone_id for r in routes}
    stranded = sorted(
        {v.vehicle_id for v in vehicles if v.home_depot_id not in startable}
    )
    if stranded:
        raise ConfigError(
            f"{path.name}: no route starts at the home depot of {stranded}"
        )

    return FleetConfig(zones=zones, routes=routes, vehicles=vehicles)
