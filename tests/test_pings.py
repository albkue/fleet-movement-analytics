"""The telemetry contract: what is accepted, what is dead-lettered, and why."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

import pytest

from fleet.pings import (
    MAX_SPEED_KPH,
    PingValidationError,
    decode_message,
    parse_ping,
    parse_timestamp,
)

VALID = {
    "ping_id": "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
    "vehicle_id": "VH-001",
    "trip_id": "TR-001-000",
    "recorded_at": "2026-07-26T08:30:00Z",
    "location": {"lat": 11.5564, "lon": 104.9282},
    "speed_kph": 42.5,
    "heading_deg": 187.0,
    "ignition": True,
    "odometer_km": 84213.4,
    "fuel_pct": 62.0,
}


def ping(**overrides) -> dict:
    doc = json.loads(json.dumps(VALID))
    for key, value in overrides.items():
        if value is Ellipsis:
            doc.pop(key, None)
        else:
            doc[key] = value
    return doc


# ----------------------------------------------------------- happy path ----


def test_valid_ping_parses():
    parsed = parse_ping(ping())

    assert parsed.ping_id == UUID("3f2504e0-4f89-41d3-9a0c-0305e82c3301")
    assert parsed.vehicle_id == "VH-001"
    assert parsed.lat == pytest.approx(11.5564)
    assert parsed.lon == pytest.approx(104.9282)
    assert parsed.position == (parsed.lon, parsed.lat)


def test_partition_key_is_the_vehicle():
    """Per-vehicle ordering is what the whole state machine rests on."""
    assert parse_ping(ping()).key() == b"VH-001"


def test_value_round_trips_through_json():
    parsed = parse_ping(ping())

    assert json.loads(parsed.value())["ping_id"] == VALID["ping_id"]


def test_h3_is_derived_not_carried():
    parsed = parse_ping(ping())

    coarse = parsed.h3(8)
    fine = parsed.h3(9)

    assert len(coarse) == 15 and len(fine) == 15
    # A finer cell is a different cell, and both index the same point.
    assert coarse != fine


def test_optional_fields_may_be_absent():
    parsed = parse_ping(
        ping(trip_id=..., heading_deg=..., odometer_km=..., fuel_pct=...)
    )

    assert parsed.trip_id is None
    assert parsed.heading_deg is None
    assert parsed.odometer_km is None
    assert parsed.fuel_pct is None


# ------------------------------------------------------------ rejection ----


@pytest.mark.parametrize("field", ["ping_id", "vehicle_id", "recorded_at"])
def test_missing_required_field_is_rejected(field):
    with pytest.raises(PingValidationError, match=field):
        parse_ping(ping(**{field: ...}))


def test_missing_speed_is_rejected():
    with pytest.raises(PingValidationError, match="speed_kph"):
        parse_ping(ping(speed_kph=...))


def test_missing_ignition_is_rejected():
    with pytest.raises(PingValidationError, match="ignition"):
        parse_ping(ping(ignition=...))


def test_bad_uuid_is_rejected():
    with pytest.raises(PingValidationError, match="not a UUID"):
        parse_ping(ping(ping_id="VH-001-0001"))


def test_missing_location_is_rejected():
    with pytest.raises(PingValidationError, match="location"):
        parse_ping(ping(location=...))


def test_half_a_location_is_rejected():
    with pytest.raises(PingValidationError, match="both lat and lon"):
        parse_ping(ping(location={"lat": 11.5}))


def test_transposed_coordinates_are_rejected():
    """lat=104.9 is not a latitude, whatever the tracker thinks."""
    with pytest.raises(PingValidationError, match="location.lat"):
        parse_ping(ping(location={"lat": 104.9282, "lon": 11.5564}))


def test_impossible_speed_is_rejected():
    with pytest.raises(PingValidationError, match="speed_kph"):
        parse_ping(ping(speed_kph=MAX_SPEED_KPH + 1))


def test_negative_speed_is_rejected():
    with pytest.raises(PingValidationError, match="speed_kph"):
        parse_ping(ping(speed_kph=-1))


def test_heading_of_360_is_rejected_as_a_duplicate_spelling_of_north():
    with pytest.raises(PingValidationError, match="heading_deg"):
        parse_ping(ping(heading_deg=360.0))


def test_moving_with_the_ignition_off_is_rejected():
    with pytest.raises(PingValidationError, match="ignition is off"):
        parse_ping(ping(ignition=False, speed_kph=60.0))


def test_creeping_with_the_ignition_off_is_allowed():
    """A parked vehicle still reports a little GPS-driven speed."""
    assert parse_ping(ping(ignition=False, speed_kph=1.4)).speed_kph == 1.4


def test_boolean_speed_is_rejected():
    """bool is a subclass of int in Python; the parser must not be fooled."""
    with pytest.raises(PingValidationError, match="speed_kph"):
        parse_ping(ping(speed_kph=True))


def test_fuel_above_full_is_rejected():
    with pytest.raises(PingValidationError, match="fuel_pct"):
        parse_ping(ping(fuel_pct=140.0))


def test_non_object_message_is_rejected():
    with pytest.raises(PingValidationError, match="expected a JSON object"):
        parse_ping([1, 2, 3])


# ---------------------------------------------------------- timestamps ----


@pytest.mark.parametrize(
    "text",
    [
        "2026-07-26T08:30:00Z",
        "2026-07-26T08:30:00z",
        "2026-07-26T08:30:00+00:00",
        "2026-07-26T15:30:00+07:00",
        "2026-07-26T08:30:00",
    ],
)
def test_timestamp_forms_all_normalise_to_the_same_utc_instant(text):
    parsed = parse_timestamp(text)

    assert parsed.tzinfo is not None
    assert parsed == datetime(2026, 7, 26, 8, 30, tzinfo=timezone.utc)


def test_naive_timestamp_is_treated_as_utc_not_local():
    """Guessing the host's zone would silently shift every duration."""
    assert parse_timestamp("2026-07-26T08:30:00").utcoffset().total_seconds() == 0


def test_unparseable_timestamp_is_rejected():
    with pytest.raises(PingValidationError, match="ISO-8601"):
        parse_timestamp("yesterday")


# ------------------------------------------------------------- decoding ----


def test_decode_valid_message():
    assert decode_message(json.dumps(VALID).encode()).vehicle_id == "VH-001"


def test_decode_rejects_a_tombstone():
    with pytest.raises(PingValidationError, match="empty"):
        decode_message(None)


def test_decode_rejects_invalid_json():
    with pytest.raises(PingValidationError, match="not valid JSON"):
        decode_message(b"{not json")


def test_decode_rejects_invalid_utf8():
    with pytest.raises(PingValidationError, match="not UTF-8"):
        decode_message(b"\xff\xfe\x00garbage")


def test_decode_rejects_nan():
    """json.loads accepts NaN by default; a NaN coordinate must not survive."""
    payload = json.dumps(VALID).replace('"lat": 11.5564', '"lat": NaN').encode()

    with pytest.raises(PingValidationError, match="NaN"):
        decode_message(payload)


def test_decode_rejects_infinity():
    payload = (
        json.dumps(VALID).replace('"speed_kph": 42.5', '"speed_kph": Infinity').encode()
    )

    with pytest.raises(PingValidationError, match="Infinity"):
        decode_message(payload)


def test_every_corrupt_template_is_actually_rejected():
    """The dead-letter fixtures must all fail, or they prove nothing."""
    from fleet.simulator import corrupt_messages

    for payload in corrupt_messages(40, seed=7):
        with pytest.raises(PingValidationError):
            decode_message(payload)
