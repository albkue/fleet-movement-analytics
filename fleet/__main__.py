"""Command line entry point: python -m fleet <command>."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from . import report, seed as seed_module
from .config import load_fleet_config, load_settings
from .consumer import consume
from .db import apply_migrations, connect, postgis_version
from .mapviz import build_map
from .producer import ensure_topic, publish, publish_raw
from .simulator import corrupt_messages, simulate
from .transform import runner as transform_runner
from .transform import tests as transform_tests

EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_FAILED = 2

log = logging.getLogger("fleet")


# --------------------------------------------------------------- parser ----


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m fleet",
        description="Kafka fleet telemetry -> PostGIS -> warehouse -> alerts",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="show debug logging"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="create/refresh the raw, ref, stream and meta schemas")
    sub.add_parser("seed", help="load config/fleet.json into ref.* and build route schedules")

    sim = sub.add_parser(
        "simulate", help="simulate fleet movement and publish it to Kafka"
    )
    sim.add_argument(
        "--hours", type=int, default=8, help="hours of movement to simulate (default 8)"
    )
    sim.add_argument("--seed", type=int, help="seed for a reproducible stream")
    sim.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="pings per second; 0 (default) publishes as fast as possible",
    )
    sim.add_argument(
        "--incidents",
        type=float,
        default=1.0,
        help="scale the injected incident rates (default 1.0; 0 disables them)",
    )
    sim.add_argument(
        "--corrupt",
        type=int,
        default=0,
        help="also publish this many malformed messages, bypassing validation",
    )
    sim.add_argument(
        "--out", metavar="PATH", help="also write the pings to a JSON-lines file"
    )
    sim.add_argument(
        "--dry-run",
        action="store_true",
        help="simulate and summarise without touching Kafka or the database",
    )

    consume_cmd = sub.add_parser(
        "consume", help="load the topic into raw.pings and run the detector"
    )
    consume_cmd.add_argument(
        "--max-messages", type=int, help="stop after this many messages"
    )
    consume_cmd.add_argument(
        "--idle-timeout",
        type=float,
        help="stop after this many idle seconds (default from .env; 0 = never)",
    )

    transform = sub.add_parser("transform", help="build the dbt-style models")
    transform.add_argument(
        "--select",
        nargs="+",
        metavar="MODEL",
        help="build only these models and their ancestors",
    )
    transform.add_argument(
        "--full-refresh",
        action="store_true",
        help="rebuild incremental models from scratch",
    )

    test_cmd = sub.add_parser(
        "test", help="run the schema tests in models/**/schema.yml"
    )
    test_cmd.add_argument(
        "--select", nargs="+", metavar="MODEL", help="test only these models"
    )

    pipeline = sub.add_parser(
        "pipeline", help="consume, transform and test in one pass"
    )
    pipeline.add_argument(
        "--idle-timeout",
        type=float,
        default=10.0,
        help="seconds of stream silence before moving on to transform (default 10)",
    )
    pipeline.add_argument(
        "--full-refresh", action="store_true", help="rebuild incremental models"
    )

    alerts = sub.add_parser("alerts", help="show detections, worst first")
    alerts.add_argument("--type", dest="detection_type", help="filter by detection type")
    alerts.add_argument("--severity", choices=["info", "warning", "critical"])
    alerts.add_argument("--vehicle", dest="vehicle_id", help="filter by vehicle id")
    alerts.add_argument(
        "--open", action="store_true", help="only episodes that are still running"
    )
    alerts.add_argument("--limit", type=int, default=20, help="rows to show (default 20)")

    trips = sub.add_parser("trips", help="show trip performance against plan")
    trips.add_argument("--vehicle", dest="vehicle_id", help="filter by vehicle id")
    trips.add_argument("--limit", type=int, default=15, help="rows to show (default 15)")

    sub.add_parser("vehicles", help="show per-vehicle utilisation")

    zones = sub.add_parser("zones", help="show geofence activity and breaches")
    zones.add_argument("--days", type=int, default=7, help="days to include (default 7)")

    hotspots = sub.add_parser(
        "hotspots", help="show the H3 cells where the fleet sits still"
    )
    hotspots.add_argument(
        "--limit", type=int, default=12, help="cells to show (default 12)"
    )

    fleet_map = sub.add_parser("map", help="write a self-contained HTML map")
    fleet_map.add_argument(
        "--out", default="fleet-map.html", help="output path (default fleet-map.html)"
    )
    fleet_map.add_argument(
        "--hours", type=int, default=6, help="hours of track to draw (default 6)"
    )

    status = sub.add_parser("status", help="show pipeline state end to end")
    status.add_argument(
        "--limit", type=int, default=5, help="ingest batches to show (default 5)"
    )

    return parser


# -------------------------------------------------------------- helpers ----


def _fmt(value: object, width: int = 0, dash: str = "-") -> str:
    text = dash if value is None else str(value)
    return text.rjust(width) if width else text


def _bar(fraction: float | None, width: int = 18) -> str:
    if fraction is None:
        return " " * width
    filled = max(0, min(width, round(fraction * width)))
    return "#" * filled + "." * (width - filled)


def _flag(value: object) -> str:
    if value is None:
        return "-"
    return "yes" if value else "no"


def _hm(seconds: object) -> str:
    """Seconds as h:mm, which is how anyone reads a duration over an hour."""
    if seconds is None:
        return "-"
    total = int(float(seconds))
    return f"{total // 3600}:{(total % 3600) // 60:02d}"


# ------------------------------------------------------------- commands ----


def cmd_init_db(args: argparse.Namespace) -> int:
    settings = load_settings()
    applied = apply_migrations(settings)
    print(f"Applied {len(applied)} migration file(s): {', '.join(applied)}")

    version = postgis_version(settings)
    if version is None:
        # Everything geospatial would fail later with a confusing "function
        # does not exist", so say it plainly here instead.
        print("WARNING: the postgis extension is not installed", file=sys.stderr)
        return EXIT_PARTIAL
    print(f"PostGIS {version} available")
    return EXIT_OK


def cmd_seed(args: argparse.Namespace) -> int:
    settings = load_settings()
    config = load_fleet_config(settings.fleet_config_file)
    summary = seed_module.seed_reference(settings, config)

    print(f"Seeded from {settings.fleet_config_file.name}")
    print(f"  zones            {summary.zones}")
    print(f"  routes           {summary.routes}")
    print(f"  vehicles         {summary.vehicles}")
    print(f"  schedule points  {summary.schedule_points}")

    kinds: dict[str, int] = {}
    for zone in config.zones:
        kinds[zone.zone_kind] = kinds.get(zone.zone_kind, 0) + 1
    print("\nGeofences by kind")
    for kind in sorted(kinds):
        print(f"  {kind:<12} {kinds[kind]}")
    return EXIT_OK


def cmd_simulate(args: argparse.Namespace) -> int:
    settings = load_settings()
    config = load_fleet_config(settings.fleet_config_file)

    if args.hours <= 0:
        print("--hours must be positive", file=sys.stderr)
        return EXIT_FAILED

    seed_value = args.seed if args.seed is not None else settings.simulator_seed
    sim = simulate(
        config,
        hours=args.hours,
        ping_interval_seconds=settings.ping_interval_seconds,
        seed=seed_value,
        incident_scale=args.incidents,
    )

    print(
        f"Simulated {len(sim.pings):,} ping(s) across {len(sim.trips)} trip(s) "
        f"and {len(config.vehicles)} vehicle(s) over {args.hours}h"
    )
    if sim.pings:
        print(
            f"  window: {sim.pings[0]['recorded_at']} .. "
            f"{sim.pings[-1]['recorded_at']}"
        )
    print("  injected incidents (what the detector should find):")
    if sim.injected:
        for name in sorted(sim.injected):
            print(f"    {name:<16} {sim.injected[name]:>5}")
    else:
        print("    (none)")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            for doc in sim.pings:
                handle.write(json.dumps(doc, separators=(",", ":")) + "\n")
        print(f"  wrote {args.out}")

    if args.dry_run:
        print("\n(dry run: nothing published, no trips seeded)")
        return EXIT_OK

    # Trips are the plan, so they go into ref.* directly rather than through
    # the topic -- a dispatch system is not a sensor.
    seeded = seed_module.seed_trips(settings, sim.trips)
    print(f"\nSeeded {seeded} trip(s) into ref.trips")

    created = ensure_topic(settings)
    print(
        f"Topic {settings.kafka_topic} "
        f"({'created' if created else 'already exists'})"
    )

    summary = publish(settings, sim.pings, rate_per_second=args.rate)
    print(
        f"Published {summary.produced:,} ping(s) in "
        f"{summary.elapsed_seconds:.1f}s ({summary.rate:,.0f}/s)"
    )
    if summary.invalid:
        print(f"  {summary.invalid} simulated ping(s) failed validation")
    if summary.failed:
        print(f"  {summary.failed} delivery failure(s)")
    for error in summary.errors:
        print(f"    {error}")

    if args.corrupt > 0:
        bad = publish_raw(settings, corrupt_messages(args.corrupt, seed_value))
        print(
            f"Published {bad.produced} malformed message(s) "
            f"(these bypass validation and must dead-letter)"
        )

    return EXIT_OK if summary.ok else EXIT_PARTIAL


def cmd_consume(args: argparse.Namespace) -> int:
    settings = load_settings()
    print(
        f"Consuming {settings.kafka_topic} as group "
        f"{settings.kafka_consumer_group} (Ctrl-C to stop)"
    )
    summary = consume(
        settings,
        max_messages=args.max_messages,
        idle_timeout_seconds=args.idle_timeout,
    )
    print(
        f"\nRead {summary.messages_read:,} message(s) in {summary.batches} batch(es), "
        f"{summary.elapsed_seconds:.1f}s ({summary.rate:,.0f}/s) "
        f"-- stopped: {summary.stopped_because}"
    )
    print(f"  inserted   {summary.rows_inserted:,}")
    print(f"  duplicate  {summary.rows_duplicate:,}  (already in raw.pings)")
    print(f"  rejected   {summary.rows_rejected:,}  (see raw.pings_dead_letter)")

    print(f"\nDetections raised in this run: {summary.detections_total:,}")
    for name in sorted(summary.detections, key=lambda n: -summary.detections[n]):
        print(f"  {name:<18} {summary.detections[name]:>6}")

    return EXIT_PARTIAL if summary.rows_rejected else EXIT_OK


def cmd_transform(args: argparse.Namespace) -> int:
    settings = load_settings()
    summary = transform_runner.run(
        settings, select=args.select, full_refresh=args.full_refresh
    )

    print(f"\nTransform run {summary.run_id}: {summary.status.upper()}")
    print(f"{'model':<28} {'materialized':<13} {'rows':>9}  {'time':>7}  status")
    for result in summary.results:
        rows = "-" if result.rows is None else f"{result.rows:,}"
        print(
            f"{result.model.name:<28} {result.model.materialized:<13} {rows:>9}  "
            f"{result.duration_seconds:>6.2f}s  {result.status}"
        )
        if result.error:
            print(f"    {result.error}")

    counts = summary.counts()
    print(
        f"\n{counts['success']} succeeded, {counts['failed']} failed, "
        f"{counts['skipped']} skipped in {summary.elapsed_seconds:.1f}s"
    )
    return {
        "success": EXIT_OK,
        "partial": EXIT_PARTIAL,
        "failed": EXIT_FAILED,
    }[summary.status]


def cmd_test(args: argparse.Namespace) -> int:
    settings = load_settings()
    summary = transform_tests.run(settings, select=args.select)

    counts = summary.counts()
    total = sum(counts.values())
    print(f"\nSchema tests: {counts['pass']}/{total} passed")

    for result in summary.results:
        if result.status == "pass":
            continue
        target = f"{result.test.model_name}.{result.test.column_name or '*'}"
        detail = result.error or f"{result.failing_rows} failing row(s)"
        print(f"  {result.status.upper():<5} {target:<34} {result.test.test_name}")
        print(f"        {detail}")

    if counts["fail"] or counts["error"]:
        return EXIT_FAILED
    return EXIT_OK


def cmd_pipeline(args: argparse.Namespace) -> int:
    """Consume whatever is on the topic, then rebuild and test the warehouse."""
    consume_args = argparse.Namespace(
        max_messages=None, idle_timeout=args.idle_timeout
    )
    print("=" * 72)
    print("1/3  ingest + detect")
    print("=" * 72)
    ingest_code = cmd_consume(consume_args)

    print("\n" + "=" * 72)
    print("2/3  transform")
    print("=" * 72)
    transform_code = cmd_transform(
        argparse.Namespace(select=None, full_refresh=args.full_refresh)
    )
    if transform_code == EXIT_FAILED:
        return EXIT_FAILED

    print("\n" + "=" * 72)
    print("3/3  test")
    print("=" * 72)
    test_code = cmd_test(argparse.Namespace(select=None))

    return max(ingest_code, transform_code, test_code)


def cmd_alerts(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        summary = report.alert_summary(conn)
        if summary:
            print("Detections by type")
            print(
                f"  {'type':<17} {'episodes':>9} {'crit':>5} {'warn':>5} "
                f"{'info':>5} {'open':>5} {'avg':>8} {'max':>8} {'unit':<5} "
                f"{'worst x':>8} {'vehicles':>9}"
            )
            for row in summary:
                unit = report.MAGNITUDE_UNITS.get(row["detection_type"], "")
                print(
                    f"  {row['detection_type']:<17} {row['episodes']:>9,} "
                    f"{row['critical']:>5,} {row['warning']:>5,} "
                    f"{row['info']:>5,} {row['still_open']:>5,} "
                    f"{_fmt(row['avg_magnitude'], 8)} "
                    f"{_fmt(row['max_magnitude'], 8)} {unit:<5} "
                    f"{_fmt(row['worst_multiple'], 8)} {row['vehicles']:>9,}"
                )

        rows = report.alerts(
            conn,
            limit=args.limit,
            detection_type=args.detection_type,
            severity=args.severity,
            vehicle_id=args.vehicle_id,
            open_only=args.open,
        )
        if not rows:
            print("\nNo alerts match. Run: python -m fleet pipeline")
            return EXIT_OK

        print(f"\nWorst {len(rows)} alert(s)  (x = multiple of its own threshold)")
        print(
            f"  {'severity':<9} {'type':<16} {'vehicle':<9} {'where':<24} "
            f"{'started (UTC)':<17} {'size':>8} {'unit':<5} {'x':>6}  open"
        )
        for row in rows:
            where = row["zone_name"] or row["route_name"] or "-"
            # Magnitude, not duration: a delay and a restricted-zone entry are
            # instants, so a duration column reports the two most actionable
            # types as 0.00 and reads like they were nothing.
            unit = report.MAGNITUDE_UNITS.get(row["detection_type"], "")
            print(
                f"  {row['severity']:<9} {row['detection_type']:<16} "
                f"{row['plate'] or row['vehicle_id']:<9} {where[:24]:<24} "
                f"{row['started_at']:%Y-%m-%d %H:%M}  "
                f"{_fmt(row['magnitude'], 8)} {unit:<5} "
                f"{_fmt(row['threshold_multiple'], 6)}  "
                f"{_flag(row['is_open'])}"
            )
    return EXIT_OK


def cmd_trips(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        punctuality = report.trip_punctuality(conn)
        if punctuality:
            print("Punctuality by vehicle type")
            print(
                f"  {'type':<12} {'trips':>6} {'done':>6} {'on time':>8} "
                f"{'on-time %':>10} {'avg late min':>13} {'km':>9}"
            )
            for row in punctuality:
                print(
                    f"  {row['vehicle_type']:<12} {row['trips']:>6,} "
                    f"{row['completed']:>6,} {row['on_time']:>8,} "
                    f"{_fmt(row['on_time_pct'], 10)} "
                    f"{_fmt(row['avg_delay_minutes'], 13)} "
                    f"{_fmt(row['distance_km'], 9)}"
                )

        rows = report.trips(conn, limit=args.limit, vehicle_id=args.vehicle_id)
        if not rows:
            print("\nNo trips yet. Run: python -m fleet simulate")
            return EXIT_OK

        print(f"\nMost recent {len(rows)} trip(s)")
        print(
            f"  {'trip':<14} {'vehicle':<9} {'route':<16} "
            f"{'planned start':<17} {'km':>7} {'cover%':>7} {'late min':>9} "
            f"{'move%':>6} {'alerts':>7}  done  on-time"
        )
        for row in rows:
            print(
                f"  {row['trip_id']:<14} {row['plate']:<9} "
                f"{(row['route_name'] or '-')[:16]:<16} "
                f"{row['planned_start_at']:%Y-%m-%d %H:%M}  "
                f"{_fmt(row['distance_km'], 7)} "
                f"{_fmt(row['route_coverage_pct'], 7)} "
                f"{_fmt(row['finish_delay_minutes'], 9)} "
                f"{_fmt(row['moving_share_pct'], 6)} "
                f"{row['alerts']:>7,}  "
                f"{_flag(row['is_complete']):<5} {_flag(row['is_on_time'])}"
            )
    return EXIT_OK


def cmd_vehicles(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        rows = report.vehicles(conn)
        if not rows:
            print("No vehicles. Run: python -m fleet seed")
            return EXIT_OK

        print(f"Fleet utilisation ({len(rows)} vehicle(s))")
        print(
            f"  {'vehicle':<9} {'plate':<9} {'type':<10} {'depot':<20} "
            f"{'km':>9} {'moving':>7} {'stopped':>8} {'idling m':>9} "
            f"{'util %':>7} {'alerts':>7}  {'':<18}"
        )
        for row in rows:
            utilisation = row["utilisation_pct"]
            print(
                f"  {row['vehicle_id']:<9} {row['plate']:<9} "
                f"{row['vehicle_type']:<10} {row['home_depot_name'][:20]:<20} "
                f"{_fmt(row['distance_km'], 9)} "
                f"{_hm((row['moving_hours'] or 0) * 3600):>7} "
                f"{_hm((row['stopped_hours'] or 0) * 3600):>8} "
                f"{_fmt(row['idling_minutes'], 9)} "
                f"{_fmt(utilisation, 7)} {row['alerts']:>7,}  "
                f"{_bar(float(utilisation) / 100 if utilisation is not None else None)}"
            )
    return EXIT_OK


def cmd_zones(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        rows = report.zone_activity(conn, days=args.days)
        if not rows:
            print("No zone activity yet. Run: python -m fleet pipeline")
            return EXIT_OK

        print(f"Geofence activity (last {args.days} day(s))")
        print(
            f"  {'zone':<26} {'kind':<12} {'km2':>7} {'limit':>6} {'visits':>7} "
            f"{'dwell h':>8} {'avg min':>8} {'max min':>8} {'breach':>7} "
            f"{'entry':>6} {'dwell':>6}"
        )
        for row in rows:
            print(
                f"  {row['zone_name'][:26]:<26} {row['zone_kind']:<12} "
                f"{_fmt(row['area_km2'], 7)} "
                f"{_fmt(row['max_dwell_minutes'], 6)} "
                f"{row['visits']:>7,} {_fmt(row['dwell_hours'], 8)} "
                f"{_fmt(row['avg_dwell_minutes'], 8)} "
                f"{_fmt(row['max_dwell_observed'], 8)} "
                f"{row['breaches']:>7,} {row['entry_breaches']:>6,} "
                f"{row['dwell_breaches']:>6,}"
            )
    return EXIT_OK


def cmd_hotspots(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        rows = report.hotspots(conn, limit=args.limit)
        if not rows:
            print("No H3 activity yet. Run: python -m fleet pipeline")
            return EXIT_OK

        print(f"Top {len(rows)} H3 r8 cell(s) by time spent stopped")
        print(
            f"  {'cell':<17} {'pings':>8} {'vehicles':>9} {'stopped h':>10} "
            f"{'moving h':>9} {'avg kph':>8} {'stopped %':>10}  centre"
        )
        for row in rows:
            print(
                f"  {row['h3_r8']:<17} {row['pings']:>8,} {row['vehicles']:>9,} "
                f"{_fmt(row['stopped_hours'], 10)} {_fmt(row['moving_hours'], 9)} "
                f"{_fmt(row['avg_moving_kph'], 8)} "
                f"{_fmt(row['stopped_share_pct'], 10)}  "
                f"{row['mean_lat']:.4f}, {row['mean_lon']:.4f}"
            )
    return EXIT_OK


def cmd_map(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        document = build_map(conn, hours=args.hours)

    path = Path(args.out)
    path.write_text(document, encoding="utf-8")
    print(f"Wrote {path} ({len(document.encode('utf-8')) / 1024:.0f} KB)")
    print("Self-contained: no basemap tiles, no CDN, opens offline.")
    return EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        overview = report.ingest_overview(conn)
        print("Ingestion (raw.pings, times in UTC)")
        print(f"  pings             {overview.get('pings', 0):,}")
        print(f"  vehicles          {overview.get('vehicles', 0):,}")
        print(f"  trips seen        {overview.get('trips', 0):,}")
        if overview.get("first_ping_at"):
            print(
                f"  telemetry window  {overview['first_ping_at']:%Y-%m-%d %H:%M} "
                f".. {overview['last_ping_at']:%Y-%m-%d %H:%M}"
            )
            print(f"  last ingested     {overview['last_ingested_at']:%Y-%m-%d %H:%M:%S}")
        if overview.get("dead_letters"):
            print(
                f"  dead letters      {overview['dead_letters']:,}  "
                f"(raw.pings_dead_letter)"
            )
            for reason in report.dead_letter_reasons(conn):
                print(f"      {reason['messages']:>4}  {reason['reason'][:70]}")

        print("\nReference data")
        for table, count in seed_module.reference_counts(settings).items():
            print(f"  {table:<24} {count:>10,}")

        print(f"\nRecent ingest batches (last {args.limit})")
        batches = report.recent_batches(conn, args.limit)
        if not batches:
            print("  (none yet)")
        for batch in batches:
            print(
                f"  #{batch['batch_id']:<5} {batch['status']:<10} "
                f"{batch['started_at']:%Y-%m-%d %H:%M:%S}  "
                f"read {batch['messages_read']:>6}  "
                f"inserted {batch['rows_inserted']:>6}  "
                f"dup {batch['rows_duplicate']:>6}  "
                f"rej {batch['rows_rejected']:>4}  "
                f"det {batch['detections_found']:>5}"
            )
            if batch["error"]:
                print(f"        error: {batch['error'][:150]}")

        episodes = report.open_episodes(conn)
        print(f"\nEpisodes still open: {len(episodes)}")
        for episode in episodes[:10]:
            print(
                f"  {episode['detection_type']:<17} {episode['severity']:<9} "
                f"{episode['vehicle_id']:<9} "
                f"{episode['zone_id'] or '-':<18} since "
                f"{episode['started_at']:%Y-%m-%d %H:%M}"
            )
        if len(episodes) > 10:
            print(f"  ... and {len(episodes) - 10} more")

        print("\nWarehouse tables")
        for relation, count in report.table_counts(conn):
            value = "not built" if count is None else f"{count:,}"
            print(f"  {relation:<32} {value:>12}")

        last_run = report.last_transform_run(conn)
        print("\nLast transform run")
        if last_run is None:
            print("  (none yet) -- run: python -m fleet transform")
        else:
            print(f"  {last_run['run_id']}  {last_run['started_at']:%Y-%m-%d %H:%M:%S}")
            print(
                f"  {last_run['succeeded']}/{last_run['models']} models succeeded, "
                f"{last_run['failed']} failed, {last_run['skipped']} skipped"
            )

        last_test = report.last_test_run(conn)
        print("\nLast schema test run")
        if last_test is None:
            print("  (none yet) -- run: python -m fleet test")
        else:
            print(
                f"  {last_test['executed_at']:%Y-%m-%d %H:%M:%S}  "
                f"{last_test['passed']}/{last_test['tests']} passed, "
                f"{last_test['failed']} failed, {last_test['errored']} errored"
            )
            for failure in report.failing_tests(conn, str(last_test["run_id"])):
                target = f"{failure['model_name']}.{failure['column_name'] or '*'}"
                detail = failure["error"] or f"{failure['failing_rows']} row(s)"
                print(
                    f"    {failure['status'].upper():<5} {target:<32} "
                    f"{failure['test_name']}: {detail}"
                )

    return EXIT_OK


# ----------------------------------------------------------------- main ----


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    handlers = {
        "init-db": cmd_init_db,
        "seed": cmd_seed,
        "simulate": cmd_simulate,
        "consume": cmd_consume,
        "transform": cmd_transform,
        "test": cmd_test,
        "pipeline": cmd_pipeline,
        "alerts": cmd_alerts,
        "trips": cmd_trips,
        "vehicles": cmd_vehicles,
        "zones": cmd_zones,
        "hotspots": cmd_hotspots,
        "map": cmd_map,
        "status": cmd_status,
    }
    try:
        return handlers[args.command](args)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return EXIT_FAILED
    except Exception as exc:  # surface a readable message, not a raw traceback
        log.error("%s: %s", type(exc).__name__, exc)
        if args.verbose:
            raise
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
