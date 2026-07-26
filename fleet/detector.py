"""The stateful part of the stream processor.

`detect()` is a pure fold: enriched pings in vehicle/time order, plus the
state each vehicle was left in, produce detections and the state each vehicle
is now in. It touches no database and no clock, which is what makes the
awkward cases -- a stop that spans two batches, a deviation interrupted by
signal loss -- testable without standing anything up.

Four things are detected, and one is detected in order to make the other
three honest:

  **idle**             stopped for longer than the threshold, where "stopped"
                       is a speed below IDLE_SPEED_KPH *and* not having
                       drifted from where the stop began. Speed alone would
                       count a vehicle crawling through traffic for an hour
                       as parked.

  **zone visits and
    geofence breaches** every entry and exit, as one episode with a dwell.
                       Entering a restricted zone is a breach immediately;
                       every other zone is a breach only by overstaying.

  **route deviation**  more than DEVIATION_METRES from the assigned route,
                       sustained for DEVIATION_SECONDS. The sustain window is
                       the whole point: a single ping off route is GPS error,
                       and alerting on it trains people to ignore alerts.

  **delay**            behind the planned schedule for the route. Re-reported
                       only when the vehicle falls a further step behind, so
                       one late trip is one alert and then an escalation, not
                       two hundred rows.

  **gps gap**          longer than GPS_GAP_MINUTES without a fix. This is the
                       honest one. A vehicle that vanishes for half an hour
                       looks exactly like a vehicle standing still, and
                       without this the idle numbers would quietly absorb
                       every tunnel and flat battery in the fleet. So a gap
                       *closes* whatever was open rather than extending it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .config import Settings
from .enrich import EnrichedPing
from .geo import haversine_m

log = logging.getLogger(__name__)

# How far a "stopped" vehicle may drift before it is treated as having moved
# on to a different stop. Comfortably above GPS noise, comfortably below the
# length of a city block.
IDLE_ANCHOR_RADIUS_M = 60.0

# Multiple of a threshold at which a detection escalates from warning to
# critical. One number, applied to idle duration, deviation distance and
# schedule slip alike, so severity means the same thing across types.
CRITICAL_MULTIPLE = 3.0


@dataclass(frozen=True)
class ZoneMeta:
    """The zone attributes the detector needs, loaded once per run."""

    zone_id: str
    zone_kind: str
    max_dwell_minutes: int | None


@dataclass(frozen=True)
class Detection:
    detection_key: str
    detection_type: str
    severity: str
    vehicle_id: str
    trip_id: str | None
    zone_id: str | None
    started_at: datetime
    ended_at: datetime | None
    duration_seconds: int | None
    lat: float | None
    lon: float | None
    magnitude: float | None
    details: dict[str, Any]
    # True when this write is the first the pipeline has heard of the
    # episode. An episode that opens in one batch and closes in the next is
    # written twice, and counting both would report roughly twice as many
    # alerts as there are -- with the inflation depending on batch size,
    # which is the sort of metric that is wrong in a different way every day.
    is_first: bool = True

    @property
    def is_open(self) -> bool:
        return self.ended_at is None


@dataclass
class VehicleState:
    """Everything needed to continue a partially-observed episode."""

    vehicle_id: str
    last_ping_id: Any = None
    last_ping_at: datetime | None = None
    last_lat: float | None = None
    last_lon: float | None = None
    last_odometer_km: float | None = None
    last_trip_id: str | None = None

    idle_since: datetime | None = None
    idle_lat: float | None = None
    idle_lon: float | None = None
    idle_pings: int = 0
    idle_ignition_pings: int = 0
    idle_zone_id: str | None = None
    idle_reported: bool = False

    open_zones: dict[str, datetime] = field(default_factory=dict)
    breached_zones: set[str] = field(default_factory=set)

    deviating_since: datetime | None = None
    deviation_peak_m: float | None = None
    deviation_reported: bool = False

    delay_reported_s: int | None = None

    pings_seen: int = 0
    pings_out_of_order: int = 0

    def reset_idle(self) -> None:
        self.idle_since = None
        self.idle_lat = None
        self.idle_lon = None
        self.idle_pings = 0
        self.idle_ignition_pings = 0
        self.idle_zone_id = None
        self.idle_reported = False

    def reset_deviation(self) -> None:
        self.deviating_since = None
        self.deviation_peak_m = None
        self.deviation_reported = False


@dataclass
class DetectorResult:
    detections: list[Detection] = field(default_factory=list)
    states: dict[str, VehicleState] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)

    def emit(self, detection: Detection) -> None:
        self.detections.append(detection)
        key = detection.detection_type + ("_open" if detection.is_open else "")
        self.counts[key] = self.counts.get(key, 0) + 1

    def bump(self, name: str, by: int = 1) -> None:
        self.counts[name] = self.counts.get(name, 0) + by

    def by_type(self) -> dict[str, int]:
        """How many *distinct episodes* this batch discovered.

        Counts only the first write of each episode, so the total matches the
        number of rows the batch adds to stream.detections rather than the
        number of statements it runs against it.
        """
        out: dict[str, int] = {}
        for detection in self.detections:
            if not detection.is_first:
                continue
            out[detection.detection_type] = out.get(detection.detection_type, 0) + 1
        return out


def detection_key(
    detection_type: str, vehicle_id: str, zone_id: str | None, started_at: datetime
) -> str:
    """A key that depends only on what happened, never on when it was seen.

    Two runs over the same pings must produce the same keys, otherwise
    reprocessing would duplicate every episode instead of merging it.
    """
    stamp = started_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S.%f")
    return f"{detection_type}|{vehicle_id}|{zone_id or '-'}|{stamp}"


def _escalate(value: float, threshold: float) -> str:
    return "critical" if value >= threshold * CRITICAL_MULTIPLE else "warning"


def _gap_severity(gap_seconds: float, threshold: float) -> str:
    """Signal loss caps at warning, and never reaches critical.

    A gap is an *absence of evidence*, not evidence of a problem: the vehicle
    may be in a basement car park or may have been stolen, and the telemetry
    cannot tell the difference. Letting it rank alongside a restricted-zone
    breach would mean the critical queue filled up with the alerts nobody can
    act on, which is how a critical queue stops being read.
    """
    return "warning" if gap_seconds >= threshold * CRITICAL_MULTIPLE else "info"


def _seconds(start: datetime, end: datetime) -> int:
    return max(0, int(round((end - start).total_seconds())))


# ----------------------------------------------------------------- idle ----


def _open_idle(state: VehicleState, ping: EnrichedPing, zones: Mapping[str, ZoneMeta]) -> None:
    state.idle_since = ping.recorded_at
    state.idle_lat = ping.lat
    state.idle_lon = ping.lon
    state.idle_pings = 1
    state.idle_ignition_pings = 1 if ping.ignition else 0
    state.idle_reported = False
    # Attribute the stop to somewhere a human would recognise. A congestion
    # corridor is where a vehicle is *stuck*, not where it is *visiting*, so
    # it loses to a depot or a customer site containing the same point.
    ranked = sorted(
        ping.zone_ids,
        key=lambda z: (
            {"depot": 0, "customer": 1, "restricted": 2}.get(
                zones[z].zone_kind if z in zones else "", 3
            ),
            z,
        ),
    )
    state.idle_zone_id = ranked[0] if ranked else None


def _idle_severity(duration_seconds: float, state: VehicleState, threshold: float) -> str:
    engine_share = state.idle_ignition_pings / max(1, state.idle_pings)
    if engine_share < 0.5:
        # Parked with the engine off. Worth measuring for utilisation, but it
        # is not a problem, and marking it as one would bury the ones that are.
        return "info"
    return _escalate(duration_seconds, threshold)


def _close_idle(
    result: DetectorResult,
    state: VehicleState,
    end: datetime | None,
    settings: Settings,
    reason: str = "moved",
) -> None:
    if state.idle_since is None:
        return

    threshold = settings.idle_minutes * 60
    if end is not None and end > state.idle_since:
        duration = (end - state.idle_since).total_seconds()
        if duration >= threshold:
            result.emit(
                Detection(
                    detection_key=detection_key(
                        "idle", state.vehicle_id, state.idle_zone_id, state.idle_since
                    ),
                    detection_type="idle",
                    severity=_idle_severity(duration, state, threshold),
                    vehicle_id=state.vehicle_id,
                    trip_id=state.last_trip_id,
                    zone_id=state.idle_zone_id,
                    started_at=state.idle_since,
                    ended_at=end,
                    duration_seconds=_seconds(state.idle_since, end),
                    lat=state.idle_lat,
                    lon=state.idle_lon,
                    magnitude=round(duration / 60.0, 2),
                    details={
                        "ended_because": reason,
                        "pings": state.idle_pings,
                        "engine_running_pings": state.idle_ignition_pings,
                    },
                    # A stop can cross the threshold and end between two
                    # pings, so it is announced and closed by the same write.
                    # That one is new; a close following an announcement is
                    # not.
                    is_first=not state.idle_reported,
                )
            )
    state.reset_idle()


def _update_idle(
    result: DetectorResult,
    state: VehicleState,
    ping: EnrichedPing,
    settings: Settings,
    zones: Mapping[str, ZoneMeta],
) -> None:
    threshold = settings.idle_minutes * 60

    if ping.speed_kph > settings.idle_speed_kph:
        _close_idle(result, state, state.last_ping_at, settings)
        return

    if state.idle_since is None:
        _open_idle(state, ping, zones)
        return

    drift = haversine_m((state.idle_lon, state.idle_lat), ping.position)
    if drift > IDLE_ANCHOR_RADIUS_M:
        # Reported as stopped, but it is not where it stopped. Creeping
        # through traffic is a different thing from standing still, and
        # merging them would inflate idle time by the entire duration of
        # every jam.
        _close_idle(result, state, state.last_ping_at, settings, reason="drifted")
        _open_idle(state, ping, zones)
        return

    state.idle_pings += 1
    if ping.ignition:
        state.idle_ignition_pings += 1

    elapsed = (ping.recorded_at - state.idle_since).total_seconds()
    if not state.idle_reported and elapsed >= threshold:
        state.idle_reported = True
        result.emit(
            Detection(
                detection_key=detection_key(
                    "idle", state.vehicle_id, state.idle_zone_id, state.idle_since
                ),
                detection_type="idle",
                severity=_idle_severity(elapsed, state, threshold),
                vehicle_id=state.vehicle_id,
                trip_id=ping.trip_id,
                zone_id=state.idle_zone_id,
                started_at=state.idle_since,
                ended_at=None,
                duration_seconds=None,
                lat=state.idle_lat,
                lon=state.idle_lon,
                magnitude=round(elapsed / 60.0, 2),
                details={"still_idle": True, "pings": state.idle_pings},
            )
        )


# ------------------------------------------------------------ geofences ----


def _breach_severity(zone: ZoneMeta | None, dwell_seconds: float) -> str:
    if zone is not None and zone.zone_kind == "restricted":
        return "critical"
    if zone is None or zone.max_dwell_minutes is None:
        return "warning"
    return _escalate(dwell_seconds, zone.max_dwell_minutes * 60)


def _close_zone_visit(
    result: DetectorResult,
    state: VehicleState,
    zone_id: str,
    entered_at: datetime,
    end: datetime,
    trip_id: str | None,
    reason: str,
    zones: Mapping[str, ZoneMeta],
) -> None:
    dwell = (end - entered_at).total_seconds()
    result.emit(
        Detection(
            detection_key=detection_key(
                "zone_visit", state.vehicle_id, zone_id, entered_at
            ),
            detection_type="zone_visit",
            severity="info",
            vehicle_id=state.vehicle_id,
            trip_id=trip_id,
            zone_id=zone_id,
            started_at=entered_at,
            ended_at=end,
            duration_seconds=_seconds(entered_at, end),
            lat=state.last_lat,
            lon=state.last_lon,
            magnitude=round(dwell / 60.0, 2),
            details={"ended_because": reason},
            # The entry was always announced, so this closes a known episode.
            is_first=False,
        )
    )

    if zone_id in state.breached_zones:
        # Close the breach with the same key it was opened under, so the
        # upsert fills in how long the vehicle actually stayed.
        #
        # Without this a breach would be frozen at the instant the limit was
        # crossed, and its magnitude would read as the limit plus one
        # reporting interval no matter what happened next -- every overstay
        # in the fleet would score an identical 1.01x and the column would
        # rank nothing.
        zone = zones.get(zone_id)
        restricted = zone is not None and zone.zone_kind == "restricted"
        result.emit(
            Detection(
                detection_key=detection_key(
                    "geofence_breach", state.vehicle_id, zone_id, entered_at
                ),
                detection_type="geofence_breach",
                severity=_breach_severity(zone, dwell),
                vehicle_id=state.vehicle_id,
                trip_id=trip_id,
                zone_id=zone_id,
                started_at=entered_at,
                ended_at=end,
                duration_seconds=_seconds(entered_at, end),
                lat=state.last_lat,
                lon=state.last_lon,
                magnitude=round(dwell / 60.0, 2),
                details={
                    "reason": (
                        "entered_restricted_zone"
                        if restricted
                        else "dwell_limit_exceeded"
                    ),
                    "limit_minutes": None if zone is None else zone.max_dwell_minutes,
                    "ended_because": reason,
                },
                is_first=False,
            )
        )

    state.breached_zones.discard(zone_id)


def _close_all_zone_visits(
    result: DetectorResult,
    state: VehicleState,
    end: datetime | None,
    reason: str,
    zones: Mapping[str, ZoneMeta],
) -> None:
    if end is None:
        state.open_zones.clear()
        state.breached_zones.clear()
        return
    for zone_id, entered_at in sorted(state.open_zones.items()):
        if end > entered_at:
            _close_zone_visit(
                result, state, zone_id, entered_at, end, state.last_trip_id,
                reason, zones,
            )
    state.open_zones.clear()
    state.breached_zones.clear()


def _update_zones(
    result: DetectorResult,
    state: VehicleState,
    ping: EnrichedPing,
    zones: Mapping[str, ZoneMeta],
) -> None:
    current = set(ping.zone_ids)
    known = set(state.open_zones)

    for zone_id in sorted(current - known):
        state.open_zones[zone_id] = ping.recorded_at
        meta = zones.get(zone_id)
        result.emit(
            Detection(
                detection_key=detection_key(
                    "zone_visit", state.vehicle_id, zone_id, ping.recorded_at
                ),
                detection_type="zone_visit",
                severity="info",
                vehicle_id=state.vehicle_id,
                trip_id=ping.trip_id,
                zone_id=zone_id,
                started_at=ping.recorded_at,
                ended_at=None,
                duration_seconds=None,
                lat=ping.lat,
                lon=ping.lon,
                magnitude=None,
                details={
                    "zone_kind": meta.zone_kind if meta else None,
                    "still_inside": True,
                },
            )
        )

        if meta is not None and meta.zone_kind == "restricted":
            # No dwell threshold applies: being here at all is the violation.
            # Opened rather than raised-and-forgotten, so that an open
            # critical breach means "this vehicle is in the restricted
            # precinct right now" -- and so that the exit fills in how long
            # it stayed.
            result.emit(
                Detection(
                    detection_key=detection_key(
                        "geofence_breach",
                        state.vehicle_id,
                        zone_id,
                        ping.recorded_at,
                    ),
                    detection_type="geofence_breach",
                    severity="critical",
                    vehicle_id=state.vehicle_id,
                    trip_id=ping.trip_id,
                    zone_id=zone_id,
                    started_at=ping.recorded_at,
                    ended_at=None,
                    duration_seconds=None,
                    lat=ping.lat,
                    lon=ping.lon,
                    magnitude=None,
                    details={
                        "reason": "entered_restricted_zone",
                        "still_inside": True,
                    },
                )
            )
            state.breached_zones.add(zone_id)

    for zone_id in sorted(known - current):
        entered_at = state.open_zones.pop(zone_id)
        _close_zone_visit(
            result, state, zone_id, entered_at, ping.recorded_at, ping.trip_id,
            "exited", zones,
        )

    for zone_id, entered_at in sorted(state.open_zones.items()):
        meta = zones.get(zone_id)
        if meta is None or meta.max_dwell_minutes is None:
            continue
        if zone_id in state.breached_zones:
            continue
        dwell = (ping.recorded_at - entered_at).total_seconds()
        if dwell <= meta.max_dwell_minutes * 60:
            continue
        state.breached_zones.add(zone_id)
        # Announced the moment the allowance runs out; closed on exit with
        # the dwell that actually happened.
        result.emit(
            Detection(
                detection_key=detection_key(
                    "geofence_breach", state.vehicle_id, zone_id, entered_at
                ),
                detection_type="geofence_breach",
                severity=_breach_severity(meta, dwell),
                vehicle_id=state.vehicle_id,
                trip_id=ping.trip_id,
                zone_id=zone_id,
                started_at=entered_at,
                ended_at=None,
                duration_seconds=None,
                lat=ping.lat,
                lon=ping.lon,
                magnitude=round(dwell / 60.0, 2),
                details={
                    "reason": "dwell_limit_exceeded",
                    "limit_minutes": meta.max_dwell_minutes,
                    "still_inside": True,
                },
            )
        )


# ------------------------------------------------------------ deviation ----


def _close_deviation(
    result: DetectorResult,
    state: VehicleState,
    end: datetime | None,
    settings: Settings,
    reason: str = "rejoined",
) -> None:
    if state.deviating_since is None:
        return
    if state.deviation_reported and end is not None and end > state.deviating_since:
        peak = state.deviation_peak_m or 0.0
        result.emit(
            Detection(
                detection_key=detection_key(
                    "route_deviation", state.vehicle_id, None, state.deviating_since
                ),
                detection_type="route_deviation",
                severity=_escalate(peak, settings.deviation_metres),
                vehicle_id=state.vehicle_id,
                trip_id=state.last_trip_id,
                zone_id=None,
                started_at=state.deviating_since,
                ended_at=end,
                duration_seconds=_seconds(state.deviating_since, end),
                lat=state.last_lat,
                lon=state.last_lon,
                magnitude=round(peak, 2),
                details={"ended_because": reason},
                # Guarded by deviation_reported above, so the excursion was
                # already announced when it crossed the sustain threshold.
                is_first=False,
            )
        )
    state.reset_deviation()


def _update_deviation(
    result: DetectorResult,
    state: VehicleState,
    ping: EnrichedPing,
    settings: Settings,
) -> None:
    distance = ping.route_distance_m
    if distance is None:
        # No assigned trip, so no route to be off. A vehicle resting at the
        # depot between trips is not deviating from anything.
        _close_deviation(result, state, state.last_ping_at, settings, "trip_ended")
        return

    if distance <= settings.deviation_metres:
        _close_deviation(result, state, state.last_ping_at, settings)
        return

    if state.deviating_since is None:
        state.deviating_since = ping.recorded_at
        state.deviation_peak_m = distance
        state.deviation_reported = False
    else:
        state.deviation_peak_m = max(state.deviation_peak_m or 0.0, distance)

    elapsed = (ping.recorded_at - state.deviating_since).total_seconds()
    if not state.deviation_reported and elapsed >= settings.deviation_seconds:
        state.deviation_reported = True
        peak = state.deviation_peak_m or distance
        result.emit(
            Detection(
                detection_key=detection_key(
                    "route_deviation", state.vehicle_id, None, state.deviating_since
                ),
                detection_type="route_deviation",
                severity=_escalate(peak, settings.deviation_metres),
                vehicle_id=state.vehicle_id,
                trip_id=ping.trip_id,
                zone_id=None,
                started_at=state.deviating_since,
                ended_at=None,
                duration_seconds=None,
                lat=ping.lat,
                lon=ping.lon,
                magnitude=round(peak, 2),
                details={"still_off_route": True, "route_id": ping.route_id},
            )
        )


# ---------------------------------------------------------------- delay ----


def _update_delay(
    result: DetectorResult,
    state: VehicleState,
    ping: EnrichedPing,
    settings: Settings,
) -> None:
    delay = ping.delay_seconds
    if delay is None:
        return

    threshold = settings.delay_minutes * 60
    if delay < threshold:
        return

    step = settings.delay_restep_minutes * 60
    if state.delay_reported_s is not None and delay < state.delay_reported_s + step:
        return

    state.delay_reported_s = int(delay)
    result.emit(
        Detection(
            detection_key=detection_key(
                "delay", state.vehicle_id, None, ping.recorded_at
            ),
            detection_type="delay",
            severity=_escalate(delay, threshold),
            vehicle_id=state.vehicle_id,
            trip_id=ping.trip_id,
            zone_id=None,
            started_at=ping.recorded_at,
            ended_at=ping.recorded_at,
            duration_seconds=0,
            lat=ping.lat,
            lon=ping.lon,
            magnitude=round(delay / 60.0, 2),
            details={
                "route_id": ping.route_id,
                "route_fraction": (
                    None
                    if ping.route_fraction is None
                    else round(ping.route_fraction, 4)
                ),
            },
        )
    )


# ------------------------------------------------------------------ gap ----


def _handle_gap(
    result: DetectorResult,
    state: VehicleState,
    ping: EnrichedPing,
    settings: Settings,
    zones: Mapping[str, ZoneMeta],
) -> bool:
    if state.last_ping_at is None:
        return False
    gap = (ping.recorded_at - state.last_ping_at).total_seconds()
    threshold = settings.gps_gap_minutes * 60
    if gap <= threshold:
        return False

    result.emit(
        Detection(
            detection_key=detection_key(
                "gps_gap", state.vehicle_id, None, state.last_ping_at
            ),
            detection_type="gps_gap",
            severity=_gap_severity(gap, threshold),
            vehicle_id=state.vehicle_id,
            trip_id=state.last_trip_id,
            zone_id=None,
            started_at=state.last_ping_at,
            ended_at=ping.recorded_at,
            duration_seconds=_seconds(state.last_ping_at, ping.recorded_at),
            lat=state.last_lat,
            lon=state.last_lon,
            magnitude=round(gap / 60.0, 2),
            details={"resumed_at_lat": ping.lat, "resumed_at_lon": ping.lon},
        )
    )

    # Everything open is closed at the last moment we actually observed. The
    # alternative -- letting the episode run through the gap -- would credit
    # the vehicle with idling, dwelling or deviating through a period nobody
    # saw, which is exactly the number that would then get quoted.
    _close_idle(result, state, state.last_ping_at, settings, reason="signal_lost")
    _close_deviation(result, state, state.last_ping_at, settings, "signal_lost")
    _close_all_zone_visits(result, state, state.last_ping_at, "signal_lost", zones)
    return True


# ----------------------------------------------------------------- fold ----


def detect(
    pings: Iterable[EnrichedPing],
    states: dict[str, VehicleState],
    settings: Settings,
    zones: Mapping[str, ZoneMeta],
) -> DetectorResult:
    """Fold a batch of enriched pings into detections and new vehicle state.

    `states` is mutated in place and also returned on the result, because the
    caller has to persist exactly the states this call touched -- writing all
    of them back would rewrite rows for vehicles that did not report.
    """
    result = DetectorResult()

    for ping in pings:
        state = states.get(ping.vehicle_id)
        if state is None:
            state = VehicleState(vehicle_id=ping.vehicle_id)
            states[ping.vehicle_id] = state
        result.states[ping.vehicle_id] = state

        # Out of order within a vehicle. Kafka guarantees order per partition
        # and pings are keyed by vehicle, so this should be unreachable --
        # which is exactly why it is counted rather than assumed away. A
        # tracker with a bad clock is the usual cause, and folding it in
        # would corrupt every duration in the episode it lands in.
        if state.last_ping_at is not None and ping.recorded_at < state.last_ping_at:
            state.pings_out_of_order += 1
            result.bump("out_of_order")
            continue

        _handle_gap(result, state, ping, settings, zones)

        if ping.trip_id != state.last_trip_id:
            # A new assignment resets how far behind we have already reported,
            # otherwise a badly delayed trip would suppress the first alert of
            # the trip after it.
            state.delay_reported_s = None

        _update_zones(result, state, ping, zones)
        _update_idle(result, state, ping, settings, zones)
        _update_deviation(result, state, ping, settings)
        _update_delay(result, state, ping, settings)

        state.last_ping_id = ping.ping_id
        state.last_ping_at = ping.recorded_at
        state.last_lat = ping.lat
        state.last_lon = ping.lon
        state.last_odometer_km = ping.odometer_km
        state.last_trip_id = ping.trip_id
        state.pings_seen += 1

    return result
