"""The stateful stream processor.

`detect()` is a pure fold, so every awkward case can be constructed directly:
a stop that spans two batches, an excursion interrupted by signal loss, a
tracker with a bad clock. These are the cases that are almost impossible to
provoke against a live stream and are exactly where this kind of processor
goes wrong.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from fleet.config import Settings
from fleet.detector import (
    IDLE_ANCHOR_RADIUS_M,
    VehicleState,
    ZoneMeta,
    detect,
    detection_key,
)
from fleet.enrich import EnrichedPing
from fleet.geo import destination

T0 = datetime(2026, 7, 26, 8, 0, tzinfo=timezone.utc)
DEPOT = (104.8900, 11.5900)

ZONES = {
    "Z-DEPOT-N": ZoneMeta("Z-DEPOT-N", "depot", 90),
    "Z-CUST-A": ZoneMeta("Z-CUST-A", "customer", 20),
    "Z-REST": ZoneMeta("Z-REST", "restricted", None),
    "Z-CONG": ZoneMeta("Z-CONG", "congestion", None),
}


def settings(**overrides) -> Settings:
    """A Settings with the thresholds this module reasons about."""
    base = dict(
        pg_host="localhost", pg_port=5436, pg_user="u", pg_password="p",
        pg_database="d", kafka_bootstrap_servers="localhost:9096",
        kafka_topic="t", kafka_consumer_group="g", kafka_topic_partitions=12,
        consumer_batch_size=500, consumer_batch_timeout_seconds=5.0,
        consumer_idle_timeout_seconds=0.0,
        idle_speed_kph=3.0, idle_minutes=5,
        deviation_metres=120.0, deviation_seconds=90,
        delay_minutes=10, delay_restep_minutes=10,
        gps_gap_minutes=10,
        h3_resolution_coarse=8, h3_resolution_fine=9,
        models_dir=".", fleet_config_file=".",
        ping_interval_seconds=15, simulator_seed=None,
    )
    base.update(overrides)
    return Settings(**base)


def ping(
    *,
    at: datetime,
    vehicle: str = "VH-001",
    position: tuple[float, float] = DEPOT,
    speed: float = 40.0,
    ignition: bool = True,
    zones: tuple[str, ...] = (),
    trip: str | None = "TR-001",
    route_distance: float | None = 0.0,
    route_fraction: float | None = 0.5,
    delay: float | None = None,
) -> EnrichedPing:
    lon, lat = position
    return EnrichedPing(
        ping_id=uuid4(),
        vehicle_id=vehicle,
        trip_id=trip,
        recorded_at=at,
        lat=lat,
        lon=lon,
        speed_kph=speed,
        ignition=ignition,
        odometer_km=1000.0,
        zone_ids=zones,
        route_id="R-1" if trip else None,
        route_distance_m=route_distance,
        route_fraction=route_fraction,
        delay_seconds=delay,
    )


def run(pings, states=None, config=None):
    states = states if states is not None else {}
    return detect(pings, states, config or settings(), ZONES), states


def of_type(result, detection_type):
    return [d for d in result.detections if d.detection_type == detection_type]


def closed(result, detection_type):
    return [d for d in of_type(result, detection_type) if not d.is_open]


# ------------------------------------------------------------------ key ----


def test_detection_key_depends_only_on_what_happened():
    a = detection_key("idle", "VH-001", "Z-CUST-A", T0)
    b = detection_key("idle", "VH-001", "Z-CUST-A", T0)

    assert a == b


def test_detection_key_is_timezone_independent():
    """The same instant expressed in another zone is the same episode."""
    other = T0.astimezone(timezone(timedelta(hours=7)))

    assert detection_key("idle", "VH-001", None, T0) == detection_key(
        "idle", "VH-001", None, other
    )


def test_detection_key_separates_types_vehicles_zones_and_starts():
    base = detection_key("idle", "VH-001", "Z-CUST-A", T0)

    assert base != detection_key("delay", "VH-001", "Z-CUST-A", T0)
    assert base != detection_key("idle", "VH-002", "Z-CUST-A", T0)
    assert base != detection_key("idle", "VH-001", "Z-DEPOT-N", T0)
    assert base != detection_key("idle", "VH-001", "Z-CUST-A", T0 + timedelta(seconds=1))


# ----------------------------------------------------------------- idle ----


def test_a_short_stop_is_not_idle():
    pings = [ping(at=T0, speed=0.5) for _ in range(1)]
    pings += [ping(at=T0 + timedelta(seconds=30 * i), speed=0.5) for i in range(1, 6)]
    pings.append(ping(at=T0 + timedelta(minutes=3), speed=40.0))

    result, _ = run(pings)

    assert of_type(result, "idle") == []


def test_a_long_stop_is_announced_then_closed_with_its_duration():
    pings = [
        ping(at=T0 + timedelta(seconds=30 * i), speed=0.4) for i in range(0, 30)
    ]
    pings.append(ping(at=T0 + timedelta(minutes=15), speed=40.0))

    result, _ = run(pings)
    episodes = of_type(result, "idle")

    assert len(episodes) == 2
    opened, ended = episodes
    assert opened.is_open and opened.is_first
    assert not ended.is_open and not ended.is_first
    # Both writes describe the same episode, so the upsert merges them.
    assert opened.detection_key == ended.detection_key
    assert ended.duration_seconds == pytest.approx(14 * 60 + 30, abs=31)
    assert ended.magnitude == pytest.approx(14.5, abs=0.6)


def test_the_stop_ends_at_the_last_stopped_ping_not_the_moving_one():
    """Attributing the moving ping's timestamp would inflate every stop."""
    stopped_until = T0 + timedelta(minutes=10)
    pings = [ping(at=T0 + timedelta(minutes=i), speed=0.3) for i in range(11)]
    pings.append(ping(at=T0 + timedelta(minutes=25), speed=45.0))

    result, _ = run(pings)

    assert closed(result, "idle")[0].ended_at == stopped_until


def test_idle_severity_reflects_whether_the_engine_was_running():
    running = [
        ping(at=T0 + timedelta(minutes=i), speed=0.3, ignition=True) for i in range(9)
    ] + [ping(at=T0 + timedelta(minutes=9), speed=40.0)]
    parked = [
        ping(at=T0 + timedelta(minutes=i), speed=0.3, ignition=False) for i in range(9)
    ] + [ping(at=T0 + timedelta(minutes=9), speed=40.0)]

    hot, _ = run(running)
    cold, _ = run(parked)

    assert closed(hot, "idle")[0].severity == "warning"
    assert closed(cold, "idle")[0].severity == "info"


def test_a_very_long_engine_idle_escalates_to_critical():
    pings = [
        ping(at=T0 + timedelta(minutes=i), speed=0.3, ignition=True) for i in range(20)
    ] + [ping(at=T0 + timedelta(minutes=20), speed=40.0)]

    result, _ = run(pings)

    assert closed(result, "idle")[0].severity == "critical"


def test_creeping_through_traffic_is_not_one_long_stop():
    """Reported speed alone would merge a crawl into a single idle episode.

    The anchor check splits it: the vehicle keeps reporting as stopped but is
    no longer where it stopped, so the run is closed and a new one begins.
    """
    pings = []
    position = DEPOT
    for i in range(24):
        if i and i % 6 == 0:
            position = destination(position, 90.0, IDLE_ANCHOR_RADIUS_M * 1.6)
        pings.append(ping(at=T0 + timedelta(minutes=i), position=position, speed=1.0))
    pings.append(ping(at=T0 + timedelta(minutes=30), speed=40.0, position=position))

    result, _ = run(pings)

    assert len(closed(result, "idle")) >= 3
    assert all(d.details["ended_because"] == "drifted" for d in closed(result, "idle")[:-1])


def test_gps_wander_within_the_anchor_does_not_split_a_stop():
    pings = []
    for i in range(20):
        wobble = destination(DEPOT, (i * 47) % 360, IDLE_ANCHOR_RADIUS_M * 0.2)
        pings.append(ping(at=T0 + timedelta(minutes=i), position=wobble, speed=0.8))
    pings.append(ping(at=T0 + timedelta(minutes=21), speed=40.0))

    result, _ = run(pings)

    assert len(closed(result, "idle")) == 1


def test_a_stop_spanning_two_batches_is_one_episode():
    """The whole point of checkpointing state between flushes."""
    states: dict[str, VehicleState] = {}
    first = [ping(at=T0 + timedelta(minutes=i), speed=0.3) for i in range(0, 4)]
    second = [ping(at=T0 + timedelta(minutes=i), speed=0.3) for i in range(4, 12)]
    second.append(ping(at=T0 + timedelta(minutes=12), speed=40.0))

    a, _ = run(first, states)
    b, _ = run(second, states)

    assert of_type(a, "idle") == []  # not yet over the threshold
    episodes = of_type(b, "idle")
    assert len(episodes) == 2
    assert episodes[-1].started_at == T0
    assert episodes[-1].duration_seconds == pytest.approx(11 * 60, abs=1)


def test_idle_is_attributed_to_a_customer_zone_over_a_congestion_corridor():
    """A vehicle stopped at a site inside a traffic corridor is *at the site*."""
    pings = [
        ping(at=T0 + timedelta(minutes=i), speed=0.3, zones=("Z-CONG", "Z-CUST-A"))
        for i in range(9)
    ] + [ping(at=T0 + timedelta(minutes=9), speed=40.0, zones=("Z-CONG", "Z-CUST-A"))]

    result, _ = run(pings)

    assert closed(result, "idle")[0].zone_id == "Z-CUST-A"


# ------------------------------------------------------------- geofences ----


def test_entering_and_leaving_a_zone_is_one_visit_with_a_dwell():
    pings = [
        ping(at=T0, zones=()),
        ping(at=T0 + timedelta(minutes=1), zones=("Z-CUST-A",)),
        ping(at=T0 + timedelta(minutes=6), zones=("Z-CUST-A",)),
        ping(at=T0 + timedelta(minutes=9), zones=()),
    ]

    result, _ = run(pings)
    visits = of_type(result, "zone_visit")

    assert len(visits) == 2
    assert visits[0].is_open and visits[0].is_first
    assert not visits[1].is_open and not visits[1].is_first
    assert visits[1].duration_seconds == 8 * 60
    assert visits[1].magnitude == pytest.approx(8.0)


def test_overlapping_zones_are_tracked_independently():
    pings = [
        ping(at=T0, zones=("Z-CONG",)),
        ping(at=T0 + timedelta(minutes=1), zones=("Z-CONG", "Z-CUST-A")),
        ping(at=T0 + timedelta(minutes=4), zones=("Z-CONG",)),
        ping(at=T0 + timedelta(minutes=6), zones=()),
    ]

    result, _ = run(pings)
    closed_visits = {d.zone_id: d for d in closed(result, "zone_visit")}

    assert closed_visits["Z-CUST-A"].duration_seconds == 3 * 60
    assert closed_visits["Z-CONG"].duration_seconds == 6 * 60


def test_entering_a_restricted_zone_is_an_immediate_critical_breach():
    pings = [
        ping(at=T0, zones=()),
        ping(at=T0 + timedelta(seconds=15), zones=("Z-REST",)),
    ]

    result, _ = run(pings)
    breaches = of_type(result, "geofence_breach")

    assert len(breaches) == 1
    assert breaches[0].severity == "critical"
    assert breaches[0].zone_id == "Z-REST"
    assert breaches[0].details["reason"] == "entered_restricted_zone"
    # Open, because the vehicle is still in there.
    assert breaches[0].is_open


def test_leaving_a_restricted_zone_records_how_long_it_stayed():
    pings = [
        ping(at=T0, zones=("Z-REST",)),
        ping(at=T0 + timedelta(minutes=4), zones=("Z-REST",)),
        ping(at=T0 + timedelta(minutes=6), zones=()),
    ]

    result, _ = run(pings)
    ended = closed(result, "geofence_breach")[0]

    assert ended.severity == "critical"
    assert ended.duration_seconds == 6 * 60
    assert ended.magnitude == pytest.approx(6.0)


def test_a_dwell_breach_records_the_overstay_not_just_the_threshold_crossing():
    """The breach opens the moment the allowance runs out.

    If it were never closed, its magnitude would be the limit plus one
    reporting interval for every overstay in the fleet -- so a vehicle two
    minutes over and one two hours over would score identically.
    """
    # Enters at minute 1, leaves at minute 80: a dwell of 79 minutes against
    # a 20-minute allowance.
    pings = [ping(at=T0, zones=())]
    pings += [
        ping(at=T0 + timedelta(minutes=i), zones=("Z-CUST-A",)) for i in range(1, 80)
    ]
    pings.append(ping(at=T0 + timedelta(minutes=80), zones=()))

    result, _ = run(pings)
    opened = [d for d in of_type(result, "geofence_breach") if d.is_open][0]
    ended = closed(result, "geofence_breach")[0]

    assert opened.detection_key == ended.detection_key
    # Announced just past the 20-minute allowance...
    assert opened.magnitude == pytest.approx(21.0, abs=1.5)
    # ...and closed with the time it actually stayed.
    assert ended.magnitude == pytest.approx(79.0, abs=0.1)
    assert ended.duration_seconds == 79 * 60
    # 79 minutes is almost four times the allowance.
    assert ended.severity == "critical"


def test_a_modest_overstay_stays_a_warning():
    """Severity has to separate "a bit late leaving" from "parked all day"."""
    pings = [ping(at=T0, zones=())]
    pings += [
        ping(at=T0 + timedelta(minutes=i), zones=("Z-CUST-A",)) for i in range(1, 31)
    ]
    pings.append(ping(at=T0 + timedelta(minutes=31), zones=()))

    result, _ = run(pings)

    assert closed(result, "geofence_breach")[0].severity == "warning"


def test_a_restricted_breach_is_raised_once_per_visit_not_once_per_ping():
    pings = [ping(at=T0, zones=())]
    pings += [
        ping(at=T0 + timedelta(minutes=i), zones=("Z-REST",)) for i in range(1, 20)
    ]

    result, _ = run(pings)

    assert len(of_type(result, "geofence_breach")) == 1


def test_leaving_and_re_entering_a_restricted_zone_raises_a_second_breach():
    pings = [
        ping(at=T0, zones=("Z-REST",)),
        ping(at=T0 + timedelta(minutes=1), zones=()),
        ping(at=T0 + timedelta(minutes=2), zones=("Z-REST",)),
    ]

    result, _ = run(pings)
    breaches = of_type(result, "geofence_breach")

    # Two distinct episodes: opened, closed, opened again.
    assert len({d.detection_key for d in breaches}) == 2
    assert result.by_type()["geofence_breach"] == 2


def test_overstaying_a_dwell_limit_raises_a_breach():
    pings = [ping(at=T0, zones=())]
    pings += [
        ping(at=T0 + timedelta(minutes=i), zones=("Z-CUST-A",)) for i in range(1, 40)
    ]

    result, _ = run(pings)
    breaches = of_type(result, "geofence_breach")

    assert len(breaches) == 1
    assert breaches[0].details["reason"] == "dwell_limit_exceeded"
    assert breaches[0].details["limit_minutes"] == 20
    assert breaches[0].magnitude > 20


def test_a_zone_with_no_dwell_limit_is_never_breached_by_dwelling():
    """Sitting in traffic is worth measuring; it is not a violation."""
    pings = [
        ping(at=T0 + timedelta(minutes=i), zones=("Z-CONG",)) for i in range(200)
    ]

    result, _ = run(pings)

    assert of_type(result, "geofence_breach") == []


def test_staying_within_the_dwell_limit_raises_nothing():
    pings = [ping(at=T0, zones=())]
    pings += [
        ping(at=T0 + timedelta(minutes=i), zones=("Z-CUST-A",)) for i in range(1, 15)
    ]
    pings.append(ping(at=T0 + timedelta(minutes=16), zones=()))

    result, _ = run(pings)

    assert of_type(result, "geofence_breach") == []


def test_a_visit_spanning_two_batches_keeps_its_start():
    states: dict[str, VehicleState] = {}
    run([ping(at=T0, zones=("Z-CUST-A",))], states)
    second, _ = run(
        [
            ping(at=T0 + timedelta(minutes=5), zones=("Z-CUST-A",)),
            ping(at=T0 + timedelta(minutes=7), zones=()),
        ],
        states,
    )

    visit = closed(second, "zone_visit")[0]
    assert visit.started_at == T0
    assert visit.duration_seconds == 7 * 60


# ------------------------------------------------------------ deviation ----


def test_a_single_ping_off_route_is_not_a_deviation():
    """This is the one that stops people ignoring the alerts."""
    pings = [
        ping(at=T0, route_distance=10.0),
        ping(at=T0 + timedelta(seconds=15), route_distance=400.0),
        ping(at=T0 + timedelta(seconds=30), route_distance=12.0),
    ]

    result, _ = run(pings)

    assert of_type(result, "route_deviation") == []


def test_a_sustained_excursion_is_announced_then_closed_with_its_peak():
    pings = [ping(at=T0, route_distance=10.0)]
    pings += [
        ping(at=T0 + timedelta(seconds=15 * i), route_distance=150.0 + 10 * i)
        for i in range(1, 20)
    ]
    pings.append(ping(at=T0 + timedelta(seconds=15 * 20), route_distance=8.0))

    result, _ = run(pings)
    episodes = of_type(result, "route_deviation")

    assert len(episodes) == 2
    assert episodes[0].is_open and episodes[0].is_first
    assert not episodes[1].is_open and not episodes[1].is_first
    assert episodes[1].magnitude == pytest.approx(340.0)
    assert episodes[1].details["ended_because"] == "rejoined"


def test_deviation_escalates_to_critical_when_far_off_route():
    pings = [ping(at=T0, route_distance=10.0)]
    pings += [
        ping(at=T0 + timedelta(seconds=15 * i), route_distance=500.0)
        for i in range(1, 12)
    ]

    result, _ = run(pings)

    assert of_type(result, "route_deviation")[0].severity == "critical"


def test_a_deviation_just_under_the_sustain_window_is_not_reported():
    pings = [ping(at=T0, route_distance=10.0)]
    pings += [
        ping(at=T0 + timedelta(seconds=15 * i), route_distance=300.0)
        for i in range(1, 6)  # 75 seconds, under the 90-second threshold
    ]
    pings.append(ping(at=T0 + timedelta(seconds=15 * 6), route_distance=10.0))

    result, _ = run(pings)

    assert of_type(result, "route_deviation") == []


def test_ending_a_trip_closes_an_open_deviation():
    pings = [ping(at=T0, route_distance=10.0)]
    pings += [
        ping(at=T0 + timedelta(seconds=15 * i), route_distance=400.0)
        for i in range(1, 12)
    ]
    pings.append(
        ping(
            at=T0 + timedelta(seconds=15 * 12),
            trip=None,
            route_distance=None,
            route_fraction=None,
        )
    )

    result, _ = run(pings)

    assert closed(result, "route_deviation")[0].details["ended_because"] == "trip_ended"


# ---------------------------------------------------------------- delay ----


def test_running_to_schedule_raises_nothing():
    pings = [
        ping(at=T0 + timedelta(minutes=i), delay=120.0) for i in range(20)
    ]

    result, _ = run(pings)

    assert of_type(result, "delay") == []


def test_running_ahead_of_schedule_raises_nothing():
    pings = [ping(at=T0 + timedelta(minutes=i), delay=-600.0) for i in range(20)]

    result, _ = run(pings)

    assert of_type(result, "delay") == []


def test_falling_behind_raises_one_alert_not_one_per_ping():
    pings = [ping(at=T0 + timedelta(minutes=i), delay=700.0) for i in range(30)]

    result, _ = run(pings)

    assert len(of_type(result, "delay")) == 1
    assert of_type(result, "delay")[0].magnitude == pytest.approx(700 / 60, abs=0.01)


def test_falling_a_further_step_behind_re_alerts():
    pings = [ping(at=T0 + timedelta(minutes=i), delay=700.0) for i in range(5)]
    pings += [
        ping(at=T0 + timedelta(minutes=5 + i), delay=1400.0) for i in range(5)
    ]

    result, _ = run(pings)

    assert len(of_type(result, "delay")) == 2


def test_a_new_trip_resets_the_delay_throttle():
    """Otherwise a badly late trip silences the first alert of the next one."""
    pings = [ping(at=T0 + timedelta(minutes=i), delay=2000.0) for i in range(5)]
    pings += [
        ping(at=T0 + timedelta(minutes=5 + i), trip="TR-002", delay=700.0)
        for i in range(5)
    ]

    result, _ = run(pings)
    alerts = of_type(result, "delay")

    assert len(alerts) == 2
    assert alerts[1].trip_id == "TR-002"


def test_delay_escalates_to_critical():
    pings = [ping(at=T0, delay=40 * 60.0)]

    result, _ = run(pings)

    assert of_type(result, "delay")[0].severity == "critical"


def test_no_schedule_means_no_delay_alert():
    pings = [ping(at=T0 + timedelta(minutes=i), delay=None) for i in range(20)]

    result, _ = run(pings)

    assert of_type(result, "delay") == []


# ------------------------------------------------------------------ gap ----


def test_a_long_silence_is_reported_as_a_gap():
    pings = [ping(at=T0), ping(at=T0 + timedelta(minutes=25))]

    result, _ = run(pings)
    gaps = of_type(result, "gps_gap")

    assert len(gaps) == 1
    assert gaps[0].started_at == T0
    assert gaps[0].duration_seconds == 25 * 60
    assert gaps[0].magnitude == pytest.approx(25.0)


def test_a_short_silence_is_not_a_gap():
    pings = [ping(at=T0), ping(at=T0 + timedelta(minutes=5))]

    result, _ = run(pings)

    assert of_type(result, "gps_gap") == []


def test_a_gap_does_not_become_idle_time():
    """The number that would otherwise absorb every tunnel in the city."""
    pings = [
        ping(at=T0, speed=0.2),
        ping(at=T0 + timedelta(minutes=6), speed=0.2),
        ping(at=T0 + timedelta(minutes=90), speed=0.2),
        ping(at=T0 + timedelta(minutes=95), speed=40.0),
    ]

    result, _ = run(pings)
    idles = closed(result, "idle")

    assert len(of_type(result, "gps_gap")) == 1
    assert len(idles) == 1
    # Six minutes of observed stopping, not eighty-four minutes of silence.
    assert idles[0].duration_seconds == 6 * 60


def test_a_gap_closes_an_open_zone_visit_at_the_last_sighting():
    pings = [
        ping(at=T0, zones=("Z-CUST-A",)),
        ping(at=T0 + timedelta(minutes=3), zones=("Z-CUST-A",)),
        ping(at=T0 + timedelta(minutes=60), zones=()),
    ]

    result, _ = run(pings)
    visit = closed(result, "zone_visit")[0]

    assert visit.ended_at == T0 + timedelta(minutes=3)
    assert visit.details["ended_because"] == "signal_lost"


def test_a_gap_closes_an_open_deviation():
    pings = [ping(at=T0, route_distance=10.0)]
    pings += [
        ping(at=T0 + timedelta(seconds=15 * i), route_distance=400.0)
        for i in range(1, 12)
    ]
    pings.append(ping(at=T0 + timedelta(minutes=60), route_distance=400.0))

    result, _ = run(pings)

    assert closed(result, "route_deviation")[0].details["ended_because"] == "signal_lost"


def test_gap_severity_escalates_with_length():
    short, _ = run([ping(at=T0), ping(at=T0 + timedelta(minutes=15))])
    long, _ = run([ping(at=T0), ping(at=T0 + timedelta(minutes=200))])

    assert of_type(short, "gps_gap")[0].severity == "info"
    assert of_type(long, "gps_gap")[0].severity == "warning"


# ------------------------------------------------------- state and order ----


def test_vehicles_do_not_interfere_with_each_other():
    pings = []
    for i in range(12):
        pings.append(ping(at=T0 + timedelta(minutes=i), vehicle="VH-001", speed=0.3))
        pings.append(ping(at=T0 + timedelta(minutes=i), vehicle="VH-002", speed=50.0))
    pings.append(ping(at=T0 + timedelta(minutes=12), vehicle="VH-001", speed=50.0))

    result, states = run(pings)

    assert {d.vehicle_id for d in closed(result, "idle")} == {"VH-001"}
    assert set(states) == {"VH-001", "VH-002"}


def test_an_out_of_order_ping_is_counted_and_skipped():
    """Keying by vehicle should make this unreachable, so it is counted."""
    pings = [
        ping(at=T0 + timedelta(minutes=10)),
        ping(at=T0),
    ]

    result, states = run(pings)

    assert states["VH-001"].pings_out_of_order == 1
    assert states["VH-001"].pings_seen == 1
    assert result.counts.get("out_of_order") == 1


def test_state_carries_the_last_observed_position_forward():
    last = destination(DEPOT, 45.0, 2000.0)
    _, states = run([ping(at=T0), ping(at=T0 + timedelta(minutes=1), position=last)])

    state = states["VH-001"]
    assert (state.lon_lat if hasattr(state, "lon_lat") else (state.last_lon, state.last_lat)) == (
        pytest.approx(last[0]),
        pytest.approx(last[1]),
    )
    assert state.last_ping_at == T0 + timedelta(minutes=1)
    assert state.pings_seen == 2


def test_episodes_left_open_stay_open_in_the_state():
    _, states = run(
        [
            ping(at=T0 + timedelta(minutes=i), speed=0.3, zones=("Z-CUST-A",))
            for i in range(9)
        ]
    )

    state = states["VH-001"]
    assert state.idle_since == T0
    assert state.idle_reported is True
    assert "Z-CUST-A" in state.open_zones


# ------------------------------------------------------------- idempotence ----


def test_reprocessing_the_same_pings_produces_the_same_keys():
    """What makes replay safe: the second pass collides rather than duplicates."""
    pings = [ping(at=T0 + timedelta(minutes=i), speed=0.3, zones=("Z-CUST-A",))
             for i in range(12)]
    pings.append(ping(at=T0 + timedelta(minutes=12), speed=50.0, zones=()))

    first, _ = run(pings, {})
    second, _ = run(pings, {})

    assert [d.detection_key for d in first.detections] == [
        d.detection_key for d in second.detections
    ]


def test_splitting_a_batch_does_not_change_the_episodes_found():
    """Batch boundaries are an implementation detail of the consumer.

    If a stop reported as one episode when the flush happened to fall
    elsewhere, every alert count would depend on CONSUMER_BATCH_SIZE.
    """
    pings = [ping(at=T0 + timedelta(minutes=i), speed=0.3, zones=("Z-CUST-A",))
             for i in range(30)]
    pings.append(ping(at=T0 + timedelta(minutes=30), speed=50.0, zones=()))

    whole, _ = run(pings, {})

    states: dict[str, VehicleState] = {}
    split_keys: list[str] = []
    for start in range(0, len(pings), 4):
        chunk, _ = run(pings[start:start + 4], states)
        split_keys.extend(d.detection_key for d in chunk.detections)

    assert set(d.detection_key for d in whole.detections) == set(split_keys)


def test_episode_counts_do_not_double_count_an_open_then_closed_episode():
    states: dict[str, VehicleState] = {}
    first = [ping(at=T0 + timedelta(minutes=i), speed=0.3) for i in range(9)]
    second = [ping(at=T0 + timedelta(minutes=9), speed=50.0)]

    a, _ = run(first, states)
    b, _ = run(second, states)

    assert a.by_type().get("idle") == 1
    assert b.by_type().get("idle") is None
