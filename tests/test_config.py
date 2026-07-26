"""Settings parsing and fleet reference-data validation."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from fleet.config import (
    ConfigError,
    PROJECT_ROOT,
    load_fleet_config,
    load_settings,
)


@pytest.fixture
def base_doc() -> dict:
    """The shipped config, as a mutable dict for tests to break on purpose."""
    return json.loads(
        (PROJECT_ROOT / "config" / "fleet.json").read_text(encoding="utf-8")
    )


def _write(tmp_path: Path, doc: dict) -> Path:
    path = tmp_path / "fleet.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


# ------------------------------------------------------------- settings ----


def test_settings_defaults_when_environment_is_empty(monkeypatch):
    for name in (
        "POSTGRES_PORT",
        "IDLE_MINUTES",
        "DEVIATION_METRES",
        "H3_RESOLUTION_COARSE",
        "H3_RESOLUTION_FINE",
        "SIMULATOR_SEED",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("fleet.config.load_dotenv", lambda *a, **k: None)

    settings = load_settings()

    assert settings.pg_port == 5436
    assert settings.idle_minutes == 5
    assert settings.deviation_metres == 120.0
    assert settings.h3_resolution_coarse == 8
    assert settings.h3_resolution_fine == 9
    assert settings.simulator_seed is None


def test_dsn_contains_every_connection_field(monkeypatch):
    monkeypatch.setattr("fleet.config.load_dotenv", lambda *a, **k: None)
    monkeypatch.setenv("POSTGRES_HOST", "db.example")
    monkeypatch.setenv("POSTGRES_PORT", "6000")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_DB", "d")

    dsn = load_settings().dsn

    assert "host=db.example" in dsn
    assert "port=6000" in dsn
    assert "dbname=d" in dsn


def test_non_numeric_setting_names_itself(monkeypatch):
    monkeypatch.setattr("fleet.config.load_dotenv", lambda *a, **k: None)
    monkeypatch.setenv("IDLE_MINUTES", "five")

    with pytest.raises(ValueError, match="IDLE_MINUTES"):
        load_settings()


def test_non_positive_threshold_is_refused(monkeypatch):
    monkeypatch.setattr("fleet.config.load_dotenv", lambda *a, **k: None)
    monkeypatch.setenv("DEVIATION_METRES", "0")

    with pytest.raises(ValueError, match="DEVIATION_METRES"):
        load_settings()


def test_fine_h3_resolution_must_be_finer_than_coarse(monkeypatch):
    monkeypatch.setattr("fleet.config.load_dotenv", lambda *a, **k: None)
    monkeypatch.setenv("H3_RESOLUTION_COARSE", "9")
    monkeypatch.setenv("H3_RESOLUTION_FINE", "9")

    with pytest.raises(ValueError, match="H3_RESOLUTION_FINE"):
        load_settings()


def test_h3_resolution_out_of_range_is_refused(monkeypatch):
    monkeypatch.setattr("fleet.config.load_dotenv", lambda *a, **k: None)
    monkeypatch.setenv("H3_RESOLUTION_COARSE", "20")

    with pytest.raises(ValueError, match="between 0 and 15"):
        load_settings()


# -------------------------------------------------------- shipped config ----


def test_shipped_config_loads():
    config = load_fleet_config(PROJECT_ROOT / "config" / "fleet.json")

    assert len(config.zones) >= 4
    assert len(config.routes) >= 2
    assert len(config.vehicles) >= 4


def test_every_shipped_vehicle_has_a_route_it_can_start():
    config = load_fleet_config(PROJECT_ROOT / "config" / "fleet.json")

    for vehicle in config.vehicles:
        assert config.routes_from(vehicle.home_depot_id), vehicle.vehicle_id


def test_every_shipped_route_stop_is_a_customer_or_depot():
    config = load_fleet_config(PROJECT_ROOT / "config" / "fleet.json")

    for route in config.routes:
        for zone_id in route.stop_zone_ids:
            assert config.zone(zone_id).zone_kind in {"customer", "depot"}


def test_shipped_zone_rings_are_closed_and_non_trivial():
    config = load_fleet_config(PROJECT_ROOT / "config" / "fleet.json")

    for zone in config.zones:
        assert zone.boundary[0] == zone.boundary[-1]
        assert len(zone.boundary) >= 5


# ----------------------------------------------------------- validation ----


def test_missing_file_is_reported_as_such(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_fleet_config(tmp_path / "nope.json")


def test_unclosed_ring_is_refused(tmp_path, base_doc):
    base_doc["zones"][0]["boundary"] = base_doc["zones"][0]["boundary"][:-1]

    with pytest.raises(ConfigError, match="not closed"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_transposed_coordinates_are_caught(tmp_path, base_doc):
    """[lat, lon] instead of [lon, lat] is the classic geospatial mistake.

    Phnom Penh's latitude (11.5) is a perfectly legal longitude, so only the
    out-of-range latitude gives the transposition away -- which is exactly
    why the check is on latitude.
    """
    ring = base_doc["zones"][0]["boundary"]
    base_doc["zones"][0]["boundary"] = [[lat, lon] for lon, lat in ring]

    with pytest.raises(ConfigError, match="latitude"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_duplicate_zone_id_is_refused(tmp_path, base_doc):
    base_doc["zones"].append(dict(base_doc["zones"][0]))

    with pytest.raises(ConfigError, match="duplicate zone_id"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_unknown_zone_kind_is_refused(tmp_path, base_doc):
    base_doc["zones"][0]["zone_kind"] = "warehouse"

    with pytest.raises(ConfigError, match="zone_kind"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_route_starting_at_an_unknown_zone_is_refused(tmp_path, base_doc):
    base_doc["routes"][0]["start_zone_id"] = "Z-NOWHERE"

    with pytest.raises(ConfigError, match="not a known zone"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_route_stop_at_an_unknown_zone_is_refused(tmp_path, base_doc):
    base_doc["routes"][0]["stop_zone_ids"] = ["Z-NOWHERE"]

    with pytest.raises(ConfigError, match="not a known zone"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_unmeetable_schedule_is_refused(tmp_path, base_doc):
    """Service time that consumes the whole plan leaves no time to drive."""
    route = base_doc["routes"][0]
    route["service_minutes"] = route["planned_duration_minutes"]

    with pytest.raises(ConfigError, match="no time to drive"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_vehicle_homed_at_a_non_depot_is_refused(tmp_path, base_doc):
    customer = next(
        z["zone_id"] for z in base_doc["zones"] if z["zone_kind"] == "customer"
    )
    base_doc["vehicles"][0]["home_depot_id"] = customer

    with pytest.raises(ConfigError, match="not a zone of kind 'depot'"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_vehicle_with_no_startable_route_is_refused(tmp_path, base_doc):
    """A stranded vehicle would silently never appear in the stream."""
    depot_ids = [z["zone_id"] for z in base_doc["zones"] if z["zone_kind"] == "depot"]
    orphan_depot = depot_ids[-1]
    base_doc["routes"] = [
        r for r in base_doc["routes"] if r["start_zone_id"] != orphan_depot
    ]
    for vehicle in base_doc["vehicles"]:
        vehicle["home_depot_id"] = orphan_depot

    with pytest.raises(ConfigError, match="no route starts at the home depot"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_config_with_no_depot_is_refused(tmp_path, base_doc):
    for zone in base_doc["zones"]:
        if zone["zone_kind"] == "depot":
            zone["zone_kind"] = "customer"

    with pytest.raises(ConfigError, match="at least one zone of kind 'depot'"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_negative_capacity_is_refused(tmp_path, base_doc):
    base_doc["vehicles"][0]["capacity_kg"] = -1

    with pytest.raises(ConfigError, match="capacity_kg"):
        load_fleet_config(_write(tmp_path, base_doc))


def test_restricted_zone_breaches_on_entry():
    config = load_fleet_config(PROJECT_ROOT / "config" / "fleet.json")

    for zone in config.zones:
        assert zone.is_geofence_violation_on_entry == (zone.zone_kind == "restricted")
