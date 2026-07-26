"""The telemetry contract shared by the producer and the stream processor.

A ping is one position report from one vehicle. The contract is deliberately
thin -- position, speed, ignition, and which trip it belongs to. Everything
interesting (is this inside a geofence, how far off route, how late) is
*derived*, because a tracker on a windscreen has no way to know any of it and
a field it cannot fill correctly is worse than a field that does not exist.

Validation is strict at the edge for one reason: a bad ping is not a bad row,
it is a bad *fact about physics*. A speed of 900 kph or a latitude of 200
does not become harmless downstream -- it silently poisons every average it
lands in. Rejected pings go to raw.pings_dead_letter with the reason.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from .geo import h3_cell

# Faster than this is a tracker fault, not a vehicle. The fastest thing in a
# delivery fleet is a motorbike on a highway; 250 kph leaves enormous room
# above that and still catches the classic GPS jump between two fixes.
MAX_SPEED_KPH = 250.0


class PingValidationError(ValueError):
    """A message was well-formed JSON but is not a usable ping."""


@dataclass(frozen=True)
class Ping:
    ping_id: UUID
    vehicle_id: str
    trip_id: str | None
    recorded_at: datetime
    lon: float
    lat: float
    speed_kph: float
    heading_deg: float | None
    ignition: bool
    odometer_km: float | None
    fuel_pct: float | None
    payload: dict[str, Any] = field(repr=False)

    @property
    def position(self) -> tuple[float, float]:
        return self.lon, self.lat

    def h3(self, resolution: int) -> str:
        return h3_cell(self.lon, self.lat, resolution)

    def key(self) -> bytes:
        """Kafka partition key.

        Keying by vehicle is the single load-bearing decision in the whole
        ingestion design. Every stateful detection -- idle runs, geofence
        enter/exit pairs, sustained route deviation -- is a fold over one
        vehicle's pings *in order*. Kafka guarantees order within a
        partition and nowhere else, so keying by vehicle is what turns
        "approximately when it stopped" into "when it stopped".
        """
        return self.vehicle_id.encode("utf-8")

    def value(self) -> bytes:
        return json.dumps(self.payload, separators=(",", ":")).encode("utf-8")


def _require(doc: dict[str, Any], key: str) -> Any:
    value = doc.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise PingValidationError(f"missing required field {key!r}")
    return value


def _number(value: Any, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PingValidationError(f"{where} must be a number, got {value!r}")
    number = float(value)
    if not math.isfinite(number):
        raise PingValidationError(f"{where} must be finite, got {value!r}")
    return number


def _bounded(value: Any, where: str, low: float, high: float) -> float:
    number = _number(value, where)
    if not low <= number <= high:
        raise PingValidationError(
            f"{where} must be between {low} and {high}, got {number}"
        )
    return number


def _optional_bounded(
    doc: dict[str, Any], key: str, low: float, high: float
) -> float | None:
    if doc.get(key) is None:
        return None
    return _bounded(doc[key], key, low, high)


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp, normalising to aware UTC.

    A naive timestamp is treated as UTC rather than rejected: trackers in the
    wild send both, and guessing the host's local zone would silently shift
    every ping -- which for a movement pipeline means shifting every duration
    and every schedule comparison too.
    """
    text = value.strip()
    # fromisoformat gained 'Z' support in 3.11; normalise anyway so the
    # accepted format does not depend on the interpreter version.
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise PingValidationError(f"recorded_at is not ISO-8601: {value!r}") from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_ping(doc: Any) -> Ping:
    """Validate a decoded JSON message into a Ping.

    Raises PingValidationError on anything the pipeline could not model. The
    consumer turns that into a dead-letter row rather than a crash.
    """
    if not isinstance(doc, dict):
        raise PingValidationError(f"expected a JSON object, got {type(doc).__name__}")

    raw_id = _require(doc, "ping_id")
    try:
        ping_id = UUID(str(raw_id))
    except ValueError as exc:
        raise PingValidationError(f"ping_id is not a UUID: {raw_id!r}") from exc

    vehicle_id = str(_require(doc, "vehicle_id"))
    recorded_at = parse_timestamp(str(_require(doc, "recorded_at")))

    location = doc.get("location")
    if not isinstance(location, dict):
        raise PingValidationError("location must be an object with lat and lon")
    if location.get("lat") is None or location.get("lon") is None:
        raise PingValidationError("location requires both lat and lon")
    lat = _bounded(location["lat"], "location.lat", -90.0, 90.0)
    lon = _bounded(location["lon"], "location.lon", -180.0, 180.0)

    if doc.get("speed_kph") is None:
        raise PingValidationError("missing required field 'speed_kph'")
    speed = _bounded(doc["speed_kph"], "speed_kph", 0.0, MAX_SPEED_KPH)

    heading = doc.get("heading_deg")
    if heading is not None:
        heading = _number(heading, "heading_deg")
        # 360 is the same direction as 0; accepting both would put two
        # spellings of north in the same column.
        if not 0.0 <= heading < 360.0:
            raise PingValidationError(
                f"heading_deg must be in [0, 360), got {heading}"
            )

    ignition = doc.get("ignition")
    if not isinstance(ignition, bool):
        raise PingValidationError(f"ignition must be true or false, got {ignition!r}")

    odometer = _optional_bounded(doc, "odometer_km", 0.0, 10_000_000.0)
    fuel = _optional_bounded(doc, "fuel_pct", 0.0, 100.0)

    trip_id = doc.get("trip_id")
    if trip_id is not None:
        trip_id = str(trip_id).strip() or None

    # A moving vehicle with the ignition off is a tow, a rolling start, or a
    # broken tracker. All three are worth catching here rather than letting
    # "engine hours" quietly disagree with "distance driven".
    if not ignition and speed > MAX_SPEED_KPH / 10:
        raise PingValidationError(
            f"ignition is off but speed_kph is {speed}"
        )

    return Ping(
        ping_id=ping_id,
        vehicle_id=vehicle_id,
        trip_id=trip_id,
        recorded_at=recorded_at,
        lon=lon,
        lat=lat,
        speed_kph=speed,
        heading_deg=heading,
        ignition=ignition,
        odometer_km=odometer,
        fuel_pct=fuel,
        payload=doc,
    )


def _reject_constant(name: str) -> Any:
    # json.loads accepts NaN, Infinity and -Infinity by default. They would
    # survive into a numeric column as NULL-ish poison, so they are refused
    # at the parser rather than caught by every downstream range check.
    raise PingValidationError(f"message contains the non-JSON constant {name}")


def decode_message(value: bytes | None) -> Ping:
    """Decode a Kafka message value into a Ping."""
    if value is None:
        raise PingValidationError("message value is empty (tombstone?)")
    try:
        doc = json.loads(value.decode("utf-8"), parse_constant=_reject_constant)
    except UnicodeDecodeError as exc:
        raise PingValidationError(f"message value is not UTF-8: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise PingValidationError(f"message value is not valid JSON: {exc}") from exc
    return parse_ping(doc)
