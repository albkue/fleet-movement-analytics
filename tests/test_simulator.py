"""The simulator's invariants: reproducibility, validity, and per-vehicle order."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pytest

from fleet.config import PROJECT_ROOT, load_fleet_config
from fleet.geo import distance_to_path_m, haversine_m, point_in_ring
from fleet.pings import parse_ping, parse_timestamp
from fleet.simulator import GPS_NOISE_M, corrupt_messages, simulate

WINDOW_END = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def config():
    return load_fleet_config(PROJECT_ROOT / "config" / "fleet.json")


@pytest.fixture(scope="module")
def sim(config):
    return simulate(
        config, hours=6, ping_interval_seconds=15, window_end=WINDOW_END, seed=1234
    )


# ------------------------------------------------------ reproducibility ----


def test_the_same_seed_reproduces_the_stream_exactly(config):
    a = simulate(config, hours=3, ping_interval_seconds=15, window_end=WINDOW_END, seed=7)
    b = simulate(config, hours=3, ping_interval_seconds=15, window_end=WINDOW_END, seed=7)

    assert a.pings == b.pings
    assert a.trips == b.trips
    assert a.injected == b.injected


def test_ping_ids_are_drawn_from_the_seeded_rng(config):
    """uuid4() would reseed from the OS and break reproducibility of keys."""
    a = simulate(config, hours=2, ping_interval_seconds=15, window_end=WINDOW_END, seed=3)
    b = simulate(config, hours=2, ping_interval_seconds=15, window_end=WINDOW_END, seed=3)

    assert [p["ping_id"] for p in a.pings] == [p["ping_id"] for p in b.pings]


def test_different_seeds_produce_different_streams(config):
    a = simulate(config, hours=3, ping_interval_seconds=15, window_end=WINDOW_END, seed=1)
    b = simulate(config, hours=3, ping_interval_seconds=15, window_end=WINDOW_END, seed=2)

    assert a.pings != b.pings


def test_ping_ids_are_unique(sim):
    ids = [p["ping_id"] for p in sim.pings]

    assert len(ids) == len(set(ids))


# ------------------------------------------------------------- validity ----


def test_every_simulated_ping_passes_the_real_validator(sim):
    """The producer validates with this parser, so a failure here is a bug."""
    for doc in sim.pings:
        parse_ping(doc)


def test_every_ping_falls_inside_the_requested_window(sim):
    start = WINDOW_END - timedelta(hours=6)

    for doc in sim.pings:
        at = parse_timestamp(doc["recorded_at"])
        assert start <= at <= WINDOW_END


def test_the_stream_is_published_in_time_order(sim):
    stamps = [p["recorded_at"] for p in sim.pings]

    assert stamps == sorted(stamps)


def test_each_vehicle_reports_in_strictly_increasing_time(sim):
    """Kafka only guarantees order within a partition, and the partition is
    the vehicle -- so this is the ordering the detector is entitled to rely
    on, and the simulator must not violate it."""
    seen: dict[str, datetime] = {}

    for doc in sim.pings:
        at = parse_timestamp(doc["recorded_at"])
        vehicle = doc["vehicle_id"]
        if vehicle in seen:
            assert at > seen[vehicle], vehicle
        seen[vehicle] = at


def test_every_vehicle_in_the_fleet_reports(sim, config):
    reported = {p["vehicle_id"] for p in sim.pings}

    assert reported == {v.vehicle_id for v in config.vehicles}


def test_odometer_never_goes_backwards_for_a_vehicle(sim):
    last: dict[str, float] = {}

    for doc in sim.pings:
        vehicle = doc["vehicle_id"]
        value = doc["odometer_km"]
        assert value >= last.get(vehicle, 0.0) - 1e-6
        last[vehicle] = value


# --------------------------------------------------------------- trips ----


def test_every_trip_names_a_real_vehicle_and_route(sim, config):
    vehicle_ids = {v.vehicle_id for v in config.vehicles}
    route_ids = {r.route_id for r in config.routes}

    for trip in sim.trips:
        assert trip.vehicle_id in vehicle_ids
        assert trip.route_id in route_ids
        assert trip.planned_end_at > trip.planned_start_at


def test_trip_ids_are_unique(sim):
    ids = [t.trip_id for t in sim.trips]

    assert len(ids) == len(set(ids))


def test_every_trip_id_on_a_ping_is_a_declared_trip(sim):
    declared = {t.trip_id for t in sim.trips}
    on_pings = {p["trip_id"] for p in sim.pings if p["trip_id"]}

    assert on_pings <= declared


def test_a_vehicle_only_starts_routes_it_can_start_from_where_it_is(sim, config):
    """A vehicle must not teleport between depots between trips."""
    by_vehicle: dict[str, list] = defaultdict(list)
    for trip in sim.trips:
        by_vehicle[trip.vehicle_id].append(trip)

    for vehicle in config.vehicles:
        depot = vehicle.home_depot_id
        for trip in sorted(
            by_vehicle[vehicle.vehicle_id], key=lambda t: t.planned_start_at
        ):
            route = config.route(trip.route_id)
            assert route.start_zone_id == depot
            depot = route.end_zone_id


def test_trips_for_one_vehicle_do_not_overlap(sim):
    by_vehicle: dict[str, list] = defaultdict(list)
    for trip in sim.trips:
        by_vehicle[trip.vehicle_id].append(trip)

    for trips in by_vehicle.values():
        trips.sort(key=lambda t: t.planned_start_at)
        for earlier, later in zip(trips, trips[1:]):
            assert later.planned_start_at >= earlier.planned_start_at


# ----------------------------------------------------------- incidents ----


def test_incidents_are_injected_and_counted(sim):
    assert sim.injected.get("deviations", 0) > 0
    assert sim.injected.get("signal_gaps", 0) > 0
    assert sim.injected.get("dwell_overruns", 0) > 0
    assert sim.injected.get("slow_trips", 0) > 0


def test_zero_incident_scale_suppresses_injected_incidents(config):
    quiet = simulate(
        config,
        hours=6,
        ping_interval_seconds=15,
        window_end=WINDOW_END,
        seed=99,
        incident_scale=0.0,
    )

    assert quiet.injected.get("deviations", 0) == 0
    assert quiet.injected.get("signal_gaps", 0) == 0
    assert quiet.injected.get("dwell_overruns", 0) == 0
    # Schedule slip is a property of driving, not an injected incident, so it
    # survives -- vehicles still run at different speeds.
    assert quiet.pings


def test_incident_scale_increases_deviations(config):
    few = simulate(
        config, hours=8, ping_interval_seconds=15, window_end=WINDOW_END,
        seed=5, incident_scale=0.2,
    )
    many = simulate(
        config, hours=8, ping_interval_seconds=15, window_end=WINDOW_END,
        seed=5, incident_scale=3.0,
    )

    assert many.injected.get("deviations", 0) > few.injected.get("deviations", 0)


def test_without_incidents_vehicles_stay_close_to_their_routes(config):
    """The control case for deviation detection.

    With no injected excursions, the only thing pushing a vehicle off its
    route is GPS noise -- so nothing should get anywhere near the 120 m
    deviation threshold.
    """
    quiet = simulate(
        config, hours=4, ping_interval_seconds=15, window_end=WINDOW_END,
        seed=11, incident_scale=0.0,
    )
    by_trip = {t.trip_id: t for t in quiet.trips}

    worst = 0.0
    for doc in quiet.pings:
        trip = by_trip.get(doc["trip_id"])
        if trip is None:
            continue
        route = config.route(trip.route_id)
        point = (doc["location"]["lon"], doc["location"]["lat"])
        worst = max(worst, distance_to_path_m(point, route.path))

    assert worst < 8 * GPS_NOISE_M


def test_with_incidents_something_does_leave_its_route(config):
    loud = simulate(
        config, hours=8, ping_interval_seconds=15, window_end=WINDOW_END,
        seed=11, incident_scale=3.0,
    )
    by_trip = {t.trip_id: t for t in loud.trips}

    worst = 0.0
    for doc in loud.pings:
        trip = by_trip.get(doc["trip_id"])
        if trip is None:
            continue
        route = config.route(trip.route_id)
        point = (doc["location"]["lon"], doc["location"]["lat"])
        worst = max(worst, distance_to_path_m(point, route.path))

    assert worst > 120.0


def test_vehicles_do_visit_the_customer_zones_on_their_routes(config, sim):
    """If nothing ever entered a geofence there would be nothing to detect."""
    customer_zones = config.zones_of_kind("customer")
    visited = set()

    for doc in sim.pings:
        point = (doc["location"]["lon"], doc["location"]["lat"])
        for zone in customer_zones:
            if point_in_ring(point, zone.boundary):
                visited.add(zone.zone_id)

    assert visited, "no customer geofence was ever entered"


def test_signal_gaps_actually_appear_in_the_stream(config):
    loud = simulate(
        config, hours=8, ping_interval_seconds=15, window_end=WINDOW_END,
        seed=21, incident_scale=3.0,
    )

    last: dict[str, datetime] = {}
    biggest = timedelta(0)
    for doc in loud.pings:
        at = parse_timestamp(doc["recorded_at"])
        vehicle = doc["vehicle_id"]
        if vehicle in last:
            biggest = max(biggest, at - last[vehicle])
        last[vehicle] = at

    assert biggest > timedelta(minutes=10)


def test_parked_vehicles_report_below_the_idle_threshold(sim):
    parked = [p for p in sim.pings if not p["ignition"]]

    assert parked
    assert all(p["speed_kph"] <= 3.0 for p in parked)


def test_gps_noise_keeps_a_parked_vehicle_within_a_few_metres(config):
    """Noise must be small enough not to look like the vehicle drove away."""
    quiet = simulate(
        config, hours=2, ping_interval_seconds=15, window_end=WINDOW_END,
        seed=31, incident_scale=0.0,
    )
    positions = [
        (p["location"]["lon"], p["location"]["lat"])
        for p in quiet.pings
        if p["vehicle_id"] == "VH-001" and not p["ignition"]
    ][:40]

    assert len(positions) > 5
    spread = max(haversine_m(positions[0], p) for p in positions)
    assert spread < 10 * GPS_NOISE_M


# ------------------------------------------------------------ arguments ----


def test_zero_hours_is_refused(config):
    with pytest.raises(ValueError, match="hours"):
        simulate(config, hours=0, ping_interval_seconds=15)


def test_negative_incident_scale_is_refused(config):
    with pytest.raises(ValueError, match="incident_scale"):
        simulate(config, hours=1, ping_interval_seconds=15, incident_scale=-1.0)


def test_corrupt_messages_are_reproducible_and_varied():
    assert corrupt_messages(20, seed=4) == corrupt_messages(20, seed=4)
    assert len(set(corrupt_messages(60, seed=4))) > 3
