"""Synthetic fleet telemetry.

The simulator drives vehicles along their assigned routes and emits position
pings at a fixed cadence. Its job is not to be realistic for its own sake --
it is to produce a stream that the detector can be *judged* against. A
detector that is never given anything to detect proves nothing, so the
incidents it is supposed to find are injected deliberately and counted:

  * **service stops** at the customer geofences on the route, sometimes
    overrunning their allowance, which is what a dwell breach looks like;
  * **congestion**, modelled as a speed penalty inside the congestion
    corridor zone -- so the delay detections and the geofence data have a
    real causal link rather than being independent noise;
  * **route deviations**, a smooth lateral excursion off the route held long
    enough to clear the sustain threshold, aimed at the restricted precinct
    about half the time so that restricted-zone breaches actually occur;
  * **schedule slip**, a per-trip multiplier on driving time;
  * **signal loss**, a window of suppressed pings, which the detector has to
    tell apart from a vehicle standing still.

Everything is driven from one `random.Random`, so `--seed` reproduces a
stream exactly.

Positions are (lon, lat) throughout; see fleet/geo.py.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from random import Random
from typing import Any, Iterable
from uuid import UUID

from .config import FleetConfig, Route, Vehicle, Zone
from .geo import (
    Coordinate,
    bearing_deg,
    destination,
    haversine_m,
    path_length_m,
    point_at_distance,
    point_in_ring,
    ring_centroid,
)

log = logging.getLogger(__name__)

# GPS error, metres, one standard deviation. A consumer-grade tracker with a
# clear sky view sits around here; it is the reason IDLE_SPEED_KPH is not 0.
GPS_NOISE_M = 7.0

# Speed multiplier inside a congestion zone.
CONGESTION_FACTOR = 0.35

# Cadence multiplier for a vehicle parked between trips. Real trackers back
# off when the ignition is off; keeping some reporting is what makes the
# depot dwell visible at all.
PARKED_INTERVAL_MULTIPLIER = 5

# Litres per 100 km by vehicle type, used only to make fuel_pct move in a
# plausible direction. Nothing downstream depends on the exact figure.
FUEL_L_PER_100KM = {"van": 11.0, "truck": 26.0, "motorbike": 2.5}
TANK_LITRES = {"van": 60.0, "truck": 200.0, "motorbike": 12.0}


@dataclass(frozen=True)
class SimulatedTrip:
    trip_id: str
    vehicle_id: str
    route_id: str
    planned_start_at: datetime
    planned_end_at: datetime


@dataclass
class Simulation:
    trips: list[SimulatedTrip] = field(default_factory=list)
    pings: list[dict[str, Any]] = field(default_factory=list)
    # Incidents the simulator deliberately introduced. The point of counting
    # them is that the detector's output can be compared against a number
    # that was decided before any detection ran.
    injected: dict[str, int] = field(default_factory=dict)

    def bump(self, name: str, by: int = 1) -> None:
        self.injected[name] = self.injected.get(name, 0) + by


@dataclass
class _Deviation:
    """A planned excursion off the route."""

    trigger_m: float
    duration_seconds: float
    peak_m: float
    side: int  # +1 right of travel, -1 left
    started_at: datetime | None = None

    def offset_m(self, now: datetime) -> float:
        """Lateral offset at `now`, ramping up, holding, then ramping down.

        A plateau rather than a smooth bump on purpose: the detector only
        opens a deviation once the vehicle has been more than
        DEVIATION_METRES off route for DEVIATION_SECONDS, and a pure sine
        spends most of its span below its own peak. A plateau makes the
        excursion's duration mean what it says.
        """
        if self.started_at is None:
            return 0.0
        elapsed = (now - self.started_at).total_seconds()
        if elapsed < 0 or elapsed > self.duration_seconds:
            return 0.0
        ramp = self.duration_seconds * 0.15
        if elapsed < ramp:
            return self.peak_m * (elapsed / ramp)
        if elapsed > self.duration_seconds - ramp:
            return self.peak_m * ((self.duration_seconds - elapsed) / ramp)
        return self.peak_m

    def finished(self, now: datetime) -> bool:
        return (
            self.started_at is not None
            and (now - self.started_at).total_seconds() > self.duration_seconds
        )


def _uuid_from(rng: Random) -> UUID:
    """A UUIDv4 drawn from the simulator's own RNG.

    uuid.uuid4() reads the OS entropy pool, which would make a seeded run
    reproducible in every respect except its primary keys -- and the primary
    key is exactly what deduplication is tested on.
    """
    return UUID(int=rng.getrandbits(128), version=4)


def _stop_distances(route: Route, config: FleetConfig, total_m: float) -> list[
    tuple[float, Zone]
]:
    """Where along the route each served stop sits, in metres from the start."""
    out: list[tuple[float, Zone]] = []
    for zone_id in route.stop_zone_ids:
        zone = config.zone(zone_id)
        centre = ring_centroid(zone.boundary)
        best_d, best_gap = 0.0, math.inf
        # 50 m sampling: finer than the zones are wide, coarse enough to stay
        # cheap. The exact metre does not matter -- the vehicle dwells inside
        # the polygon either way.
        steps = max(2, int(total_m // 50))
        for i in range(steps + 1):
            d = total_m * i / steps
            gap = haversine_m(point_at_distance(route.path, d)[0], centre)
            if gap < best_gap:
                best_d, best_gap = d, gap
        out.append((best_d, zone))
    out.sort(key=lambda pair: pair[0])
    return out


def _plan_deviation(
    rng: Random, route: Route, config: FleetConfig, total_m: float
) -> _Deviation:
    """Pick where and how far a vehicle wanders off route.

    About half of deviations are aimed at a restricted zone the route passes
    near, because a restricted-zone breach that only ever happens by accident
    is a detection path that mostly goes untested.
    """
    restricted = config.zones_of_kind("restricted")
    target: tuple[float, Coordinate, float] | None = None

    if restricted and rng.random() < 0.5:
        zone = rng.choice(restricted)
        centre = ring_centroid(zone.boundary)
        best = (0.0, math.inf)
        steps = max(2, int(total_m // 50))
        for i in range(steps + 1):
            d = total_m * i / steps
            gap = haversine_m(point_at_distance(route.path, d)[0], centre)
            if gap < best[1]:
                best = (d, gap)
        if best[1] < 900.0:
            target = (best[0], centre, best[1])

    duration = rng.uniform(180.0, 420.0)

    if target is not None:
        trigger_m, centre, gap = target
        position, heading = point_at_distance(route.path, trigger_m)
        # Push far enough past the zone edge that GPS noise cannot argue.
        peak = min(600.0, gap + 150.0)
        side = _side_towards(position, heading, centre)
        return _Deviation(trigger_m, duration, peak, side)

    return _Deviation(
        trigger_m=rng.uniform(0.15 * total_m, 0.75 * total_m),
        duration_seconds=duration,
        peak_m=rng.uniform(160.0, 420.0),
        side=rng.choice([-1, 1]),
    )


def _side_towards(position: Coordinate, heading: float, target: Coordinate) -> int:
    """Which side of the direction of travel `target` lies on."""
    relative = (bearing_deg(position, target) - heading) % 360.0
    return 1 if relative < 180.0 else -1


def _delay_factor(rng: Random) -> float:
    """Per-trip multiplier on driving time.

    Most trips run roughly to plan; a long tail does not. Drawn once per trip
    rather than per ping so that a late trip is late all the way through,
    which is what makes a *sustained* schedule slip rather than jitter.
    """
    roll = rng.random()
    if roll < 0.68:
        return rng.uniform(0.95, 1.06)
    if roll < 0.90:
        return rng.uniform(1.10, 1.30)
    return rng.uniform(1.35, 1.85)


def _in_congestion(position: Coordinate, zones: Iterable[Zone]) -> bool:
    return any(point_in_ring(position, z.boundary) for z in zones)


def _jitter(rng: Random, position: Coordinate) -> Coordinate:
    """Displace a position by a draw from the GPS error distribution."""
    return destination(
        position, rng.uniform(0.0, 360.0), abs(rng.gauss(0.0, GPS_NOISE_M))
    )


class _VehicleClock:
    """Odometer and fuel carried across a vehicle's trips."""

    def __init__(self, rng: Random, vehicle: Vehicle) -> None:
        self.odometer_km = round(rng.uniform(18_000.0, 190_000.0), 1)
        self.fuel_pct = rng.uniform(55.0, 100.0)
        self.litres_per_km = FUEL_L_PER_100KM[vehicle.vehicle_type] / 100.0
        self.tank_litres = TANK_LITRES[vehicle.vehicle_type]

    def travel(self, metres: float) -> None:
        km = metres / 1000.0
        self.odometer_km += km
        burned_pct = 100.0 * (km * self.litres_per_km) / self.tank_litres
        self.fuel_pct -= burned_pct
        if self.fuel_pct < 8.0:
            # Refuelled. Modelled as an instant top-up because nothing
            # downstream reasons about refuelling stops.
            self.fuel_pct = 100.0


def _ping_doc(
    rng: Random,
    vehicle: Vehicle,
    trip_id: str | None,
    at: datetime,
    position: Coordinate,
    speed_kph: float,
    heading: float | None,
    ignition: bool,
    clock: _VehicleClock,
) -> dict[str, Any]:
    lon, lat = position
    return {
        "ping_id": str(_uuid_from(rng)),
        "vehicle_id": vehicle.vehicle_id,
        "trip_id": trip_id,
        "recorded_at": at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "location": {"lat": round(lat, 6), "lon": round(lon, 6)},
        "speed_kph": round(speed_kph, 2),
        "heading_deg": None if heading is None else round(heading % 360.0, 1),
        "ignition": ignition,
        "odometer_km": round(clock.odometer_km, 2),
        "fuel_pct": round(max(0.0, min(100.0, clock.fuel_pct)), 1),
        "device": {
            # Not used downstream; present because real telemetry carries it
            # and raw.pings keeps the whole payload.
            "hdop": round(rng.uniform(0.6, 1.8), 2),
            "satellites": rng.randint(6, 14),
            "firmware": "tracker-2.4.1",
        },
    }


def _drive_trip(
    rng: Random,
    sim: Simulation,
    config: FleetConfig,
    vehicle: Vehicle,
    route: Route,
    trip_id: str,
    start_at: datetime,
    window_end: datetime,
    clock: _VehicleClock,
    interval_seconds: int,
    incident_scale: float,
) -> datetime:
    """Drive one trip, appending pings. Returns the time the vehicle finished."""
    total_m = path_length_m(route.path)
    stops = _stop_distances(route, config, total_m)
    congestion = config.zones_of_kind("congestion")

    service_seconds = route.service_minutes * 60
    planned_drive_seconds = max(
        60.0, route.planned_duration_minutes * 60 - service_seconds * len(stops)
    )
    nominal_mps = total_m / planned_drive_seconds
    delay_factor = _delay_factor(rng)
    if delay_factor > 1.10:
        sim.bump("slow_trips")

    deviation = (
        _plan_deviation(rng, route, config, total_m)
        if rng.random() < 0.18 * incident_scale
        else None
    )
    if deviation is not None:
        sim.bump("deviations")

    gap: tuple[datetime, datetime] | None = None
    if rng.random() < 0.12 * incident_scale:
        gap_start = start_at + timedelta(
            seconds=rng.uniform(120.0, max(240.0, planned_drive_seconds * 0.8))
        )
        gap = (gap_start, gap_start + timedelta(minutes=rng.uniform(12.0, 35.0)))
        sim.bump("signal_gaps")

    now = start_at
    travelled = 0.0
    next_stop = 0
    dwell_until: datetime | None = None
    dwelling_at: Zone | None = None
    interval = timedelta(seconds=interval_seconds)

    # Hard stop: a trip cannot last more than four times its plan. Without it
    # a pathological speed draw could spin here forever.
    deadline = start_at + timedelta(minutes=route.planned_duration_minutes * 4)

    while travelled < total_m and now < deadline and now < window_end:
        if dwell_until is not None and now >= dwell_until:
            dwell_until = None
            dwelling_at = None

        moving = dwell_until is None
        position, heading = point_at_distance(route.path, travelled)

        if moving:
            speed_mps = nominal_mps / delay_factor * rng.uniform(0.82, 1.18)
            if _in_congestion(position, congestion):
                speed_mps *= CONGESTION_FACTOR
        else:
            speed_mps = 0.0

        if deviation is not None:
            if deviation.started_at is None and travelled >= deviation.trigger_m:
                deviation.started_at = now
            offset = deviation.offset_m(now)
            if offset > 0.0:
                position = destination(position, heading + 90.0 * deviation.side, offset)
            if deviation.finished(now):
                deviation = None

        # A dwelling vehicle still reports a metre or two of GPS wander, which
        # is exactly why "stopped" is a speed threshold and not equality to 0.
        reported_kph = (
            speed_mps * 3.6 if moving else rng.uniform(0.0, 2.2)
        )
        ignition = moving or (
            dwell_until is not None and (dwell_until - now) < timedelta(minutes=12)
        )

        if gap is None or not (gap[0] <= now <= gap[1]):
            sim.pings.append(
                _ping_doc(
                    rng,
                    vehicle,
                    trip_id,
                    now,
                    _jitter(rng, position),
                    reported_kph,
                    heading if moving else None,
                    ignition,
                    clock,
                )
            )

        step_m = speed_mps * interval_seconds
        travelled += step_m
        clock.travel(step_m)
        now += interval

        if moving and next_stop < len(stops) and travelled >= stops[next_stop][0]:
            travelled = stops[next_stop][0]
            dwelling_at = stops[next_stop][1]
            overrun = 1.0
            if rng.random() < 0.22 * incident_scale:
                # Overrun the allowance far enough to breach the zone's dwell
                # limit, not just to look slow.
                limit = dwelling_at.max_dwell_minutes or route.service_minutes
                overrun = (limit * 60.0 / max(1.0, service_seconds)) * rng.uniform(
                    1.15, 1.9
                )
                sim.bump("dwell_overruns")
            dwell_until = now + timedelta(
                seconds=service_seconds * overrun * rng.uniform(0.9, 1.15)
            )
            next_stop += 1

    return now


def _park(
    rng: Random,
    sim: Simulation,
    vehicle: Vehicle,
    depot: Zone,
    from_at: datetime,
    until: datetime,
    clock: _VehicleClock,
    interval_seconds: int,
) -> None:
    """Emit the low-cadence pings of a vehicle standing at its depot."""
    interval = timedelta(seconds=interval_seconds * PARKED_INTERVAL_MULTIPLIER)
    centre = ring_centroid(depot.boundary)
    # Park somewhere in the yard rather than exactly on the centroid, so
    # every vehicle does not report the identical position.
    bay = destination(centre, rng.uniform(0.0, 360.0), rng.uniform(10.0, 180.0))

    now = from_at
    while now < until:
        sim.pings.append(
            _ping_doc(
                rng,
                vehicle,
                None,
                now,
                _jitter(rng, bay),
                rng.uniform(0.0, 1.4),
                None,
                False,
                clock,
            )
        )
        now += interval


def simulate(
    config: FleetConfig,
    *,
    hours: int,
    ping_interval_seconds: int,
    window_end: datetime | None = None,
    seed: int | None = None,
    incident_scale: float = 1.0,
) -> Simulation:
    """Simulate `hours` of fleet movement ending at `window_end` (default now)."""
    if hours <= 0:
        raise ValueError(f"hours must be positive, got {hours}")
    if incident_scale < 0:
        raise ValueError(f"incident_scale must not be negative, got {incident_scale}")

    rng = Random(seed)
    end = (window_end or datetime.now(timezone.utc)).astimezone(timezone.utc)
    start = end - timedelta(hours=hours)

    sim = Simulation()

    for vehicle in config.vehicles:
        clock = _VehicleClock(rng, vehicle)
        depot_id = vehicle.home_depot_id
        # Stagger departures so the whole fleet does not leave on the same
        # second; a synchronised fleet makes every aggregate look periodic.
        cursor = start + timedelta(minutes=rng.uniform(0.0, 45.0))
        _park(
            rng, sim, vehicle, config.zone(depot_id), start, cursor, clock,
            ping_interval_seconds,
        )

        sequence = 0
        while cursor < end:
            options = config.routes_from(depot_id)
            if not options:
                break
            route = rng.choice(options)
            trip_id = f"TR-{vehicle.vehicle_id[-3:]}-{sequence:03d}"

            sim.trips.append(
                SimulatedTrip(
                    trip_id=trip_id,
                    vehicle_id=vehicle.vehicle_id,
                    route_id=route.route_id,
                    planned_start_at=cursor,
                    planned_end_at=cursor
                    + timedelta(minutes=route.planned_duration_minutes),
                )
            )

            finished = _drive_trip(
                rng, sim, config, vehicle, route, trip_id, cursor, end, clock,
                ping_interval_seconds, incident_scale,
            )

            depot_id = route.end_zone_id
            rest_until = min(
                end, finished + timedelta(minutes=rng.uniform(6.0, 28.0))
            )
            _park(
                rng, sim, vehicle, config.zone(depot_id), finished, rest_until,
                clock, ping_interval_seconds,
            )
            cursor = rest_until
            sequence += 1

    # Kafka orders per partition, and the partition is the vehicle, so only
    # per-vehicle order is guaranteed downstream. Sorting the whole list by
    # time makes the *published* stream look like a real fleet reporting
    # concurrently rather than one vehicle's entire day followed by the next.
    sim.pings.sort(key=lambda doc: (doc["recorded_at"], doc["vehicle_id"]))
    return sim


def corrupt_messages(count: int, seed: int | None = None) -> list[bytes]:
    """Malformed messages, for exercising the dead-letter path.

    Deliberately a mix of kinds: bytes that are not JSON at all, JSON that is
    not an object, a ping missing a required field, one carrying a physically
    impossible speed, and one whose coordinates are transposed into the
    ocean. Each of these fails at a different point in fleet/pings.py, and a
    dead-letter table that only ever sees one failure mode is not evidence
    that the others are handled.
    """
    rng = Random(seed)
    templates: list[bytes] = [
        b"not json at all",
        b"[1, 2, 3]",
        b'{"ping_id": "not-a-uuid", "vehicle_id": "VH-001"}',
        b'{"ping_id": "3f2504e0-4f89-41d3-9a0c-0305e82c3301", '
        b'"vehicle_id": "VH-001", "recorded_at": "2026-01-01T00:00:00Z", '
        b'"location": {"lat": 11.55, "lon": 104.92}, "speed_kph": 940.0, '
        b'"ignition": true}',
        b'{"ping_id": "3f2504e0-4f89-41d3-9a0c-0305e82c3302", '
        b'"vehicle_id": "VH-002", "recorded_at": "2026-01-01T00:00:00Z", '
        b'"location": {"lat": 104.92, "lon": 11.55}, "speed_kph": 30.0, '
        b'"ignition": true}',
        b'{"ping_id": "3f2504e0-4f89-41d3-9a0c-0305e82c3303", '
        b'"vehicle_id": "VH-003", "recorded_at": "yesterday", '
        b'"location": {"lat": 11.55, "lon": 104.92}, "speed_kph": 30.0, '
        b'"ignition": true}',
        b'{"ping_id": "3f2504e0-4f89-41d3-9a0c-0305e82c3304", '
        b'"vehicle_id": "VH-004", "recorded_at": "2026-01-01T00:00:00Z", '
        b'"location": {"lat": NaN, "lon": 104.92}, "speed_kph": 30.0, '
        b'"ignition": true}',
        b"\xff\xfe\x00binary garbage",
    ]
    return [rng.choice(templates) for _ in range(count)]
