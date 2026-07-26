"""Execute the model DAG against Postgres."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Iterable
from uuid import UUID, uuid4

import psycopg

from ..config import Settings
from .models import Model, compile_sql, discover_models, select_models

log = logging.getLogger(__name__)


@dataclass
class ModelResult:
    model: Model
    status: str  # success | failed | skipped
    rows: int | None = None
    duration_seconds: float = 0.0
    error: str | None = None
    full_refresh: bool = False


@dataclass
class TransformSummary:
    run_id: UUID
    results: list[ModelResult] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    @property
    def status(self) -> str:
        if any(r.status == "failed" for r in self.results):
            return "failed" if all(
                r.status != "success" for r in self.results
            ) else "partial"
        return "success"

    def counts(self) -> dict[str, int]:
        out = {"success": 0, "failed": 0, "skipped": 0}
        for result in self.results:
            out[result.status] += 1
        return out


def _identifier(name: str) -> str:
    """Guard against anything but a plain identifier reaching a DDL string.

    Model and column names come from files on disk rather than user input,
    but these values are interpolated into DDL that cannot be parameterised,
    so they are validated rather than trusted.
    """
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def _relation(model: Model) -> str:
    return f"{_identifier(model.schema)}.{_identifier(model.name)}"


def _relation_exists(cur: psycopg.Cursor, relation: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,))
    return bool(cur.fetchone()[0])


def _key_columns(model: Model) -> list[str]:
    assert model.unique_key is not None
    return [_identifier(part.strip()) for part in model.unique_key.split(",")]


def _create_indexes(cur: psycopg.Cursor, model: Model) -> None:
    relation = _relation(model)
    for i, columns in enumerate(model.indexes):
        safe = ", ".join(_identifier(c.strip()) for c in columns.split(","))
        index_name = _identifier(f"ix_{model.name}_{i}")
        cur.execute(f"CREATE INDEX {index_name} ON {relation} ({safe})")


def _build_view(cur: psycopg.Cursor, model: Model, sql: str) -> int | None:
    relation = _relation(model)
    # CASCADE because a downstream view may still be bound to the old
    # definition; a full run rebuilds those in dependency order right after.
    cur.execute(f"DROP VIEW IF EXISTS {relation} CASCADE")
    cur.execute(f"CREATE VIEW {relation} AS {sql}")
    cur.execute(f"SELECT count(*) FROM {relation}")
    return cur.fetchone()[0]


def _build_table(cur: psycopg.Cursor, model: Model, sql: str) -> int:
    """Build into a scratch name and swap, so the rebuild is atomic.

    Readers of the old table keep seeing it until this transaction commits,
    rather than watching it empty out and refill.
    """
    relation = _relation(model)
    schema = _identifier(model.schema)
    build_name = _identifier(f"{model.name}__build")
    build_relation = f"{schema}.{build_name}"

    cur.execute(f"DROP TABLE IF EXISTS {build_relation} CASCADE")
    cur.execute(f"CREATE TABLE {build_relation} AS {sql}")
    rows = cur.rowcount

    cur.execute(f"DROP TABLE IF EXISTS {relation} CASCADE")
    cur.execute(
        f"ALTER TABLE {build_relation} RENAME TO {_identifier(model.name)}"
    )
    _create_indexes(cur, model)
    return rows


def _build_incremental(
    cur: psycopg.Cursor, model: Model, sql: str, *, full_refresh: bool
) -> int:
    """Delete-and-insert merge on unique_key.

    Chosen over a plain append because the staging models re-read a short
    overlap window on every run (late-arriving rows), so the same ping can
    legitimately be selected twice and must update rather than duplicate.
    """
    relation = _relation(model)
    if full_refresh or not _relation_exists(cur, relation):
        return _build_table(cur, model, sql)

    stage = _identifier(f"_inc_{model.name}")
    cur.execute(f"CREATE TEMP TABLE {stage} ON COMMIT DROP AS {sql}")
    rows = cur.rowcount

    if rows:
        predicate = " AND ".join(
            f"target.{column} = stage.{column}" for column in _key_columns(model)
        )
        cur.execute(
            f"DELETE FROM {relation} AS target USING {stage} AS stage "
            f"WHERE {predicate}"
        )
        cur.execute(f"INSERT INTO {relation} SELECT * FROM {stage}")
    return rows


def build_variables(settings: Settings) -> dict[str, Any]:
    """Values models can read with {{ var('...') }}.

    The detection thresholds are passed in rather than repeated in SQL for
    the same reason they are not repeated in Python: a model that decides for
    itself what "idle" means will eventually disagree with the detector that
    raised the alert, and the two numbers will be defended by different
    people in the same meeting.
    """
    return {
        "idle_speed_kph": settings.idle_speed_kph,
        "idle_minutes": settings.idle_minutes,
        "deviation_metres": settings.deviation_metres,
        "delay_minutes": settings.delay_minutes,
        "gps_gap_minutes": settings.gps_gap_minutes,
        "h3_resolution_coarse": settings.h3_resolution_coarse,
        "h3_resolution_fine": settings.h3_resolution_fine,
    }


def run(
    settings: Settings,
    *,
    select: Iterable[str] | None = None,
    full_refresh: bool = False,
) -> TransformSummary:
    """Build the selected models (and their ancestors) in dependency order."""
    models = discover_models(settings.models_dir)
    plan = select_models(models, list(select) if select else None)
    variables = build_variables(settings)

    summary = TransformSummary(run_id=uuid4())
    started = time.monotonic()
    failed: set[str] = set()

    conn = psycopg.connect(settings.dsn, autocommit=True)
    try:
        for model in plan:
            # A model whose input never got rebuilt would silently produce
            # numbers from stale upstream data, which is worse than a gap.
            blocked = sorted(set(model.depends_on) & failed)
            if blocked:
                summary.results.append(
                    ModelResult(
                        model=model,
                        status="skipped",
                        error=f"upstream failed: {', '.join(blocked)}",
                    )
                )
                failed.add(model.name)
                _log_model_run(conn, summary.run_id, model, "skipped", None,
                               full_refresh, f"upstream failed: {', '.join(blocked)}")
                log.warning("skipping %s (%s)", model.name, blocked)
                continue

            model_started = time.monotonic()
            model_run_id = _log_model_run(
                conn, summary.run_id, model, "running", None, full_refresh, None
            )

            try:
                incremental = (
                    model.materialized == "incremental"
                    and not full_refresh
                    and _table_present(conn, _relation(model))
                )
                sql = compile_sql(
                    model,
                    models,
                    is_incremental=incremental,
                    variables=variables,
                )

                with conn.transaction():
                    with conn.cursor() as cur:
                        if model.materialized == "view":
                            rows = _build_view(cur, model, sql)
                        elif model.materialized == "table":
                            rows = _build_table(cur, model, sql)
                        else:
                            rows = _build_incremental(
                                cur, model, sql, full_refresh=full_refresh
                            )

                duration = time.monotonic() - model_started
                summary.results.append(
                    ModelResult(
                        model=model,
                        status="success",
                        rows=rows,
                        duration_seconds=duration,
                        full_refresh=full_refresh,
                    )
                )
                _finish_model_run(conn, model_run_id, "success", rows, None)
                log.info(
                    "%-26s %-11s %8s rows  %.2fs",
                    model.name,
                    "incremental" if incremental else model.materialized,
                    rows if rows is not None else "-",
                    duration,
                )

            except Exception as exc:
                duration = time.monotonic() - model_started
                message = f"{type(exc).__name__}: {exc}"
                summary.results.append(
                    ModelResult(
                        model=model,
                        status="failed",
                        duration_seconds=duration,
                        error=message,
                        full_refresh=full_refresh,
                    )
                )
                failed.add(model.name)
                _finish_model_run(conn, model_run_id, "failed", None, message)
                log.error("%s failed: %s", model.name, message)
    finally:
        conn.close()

    summary.elapsed_seconds = time.monotonic() - started
    return summary


def _table_present(conn: psycopg.Connection, relation: str) -> bool:
    with conn.cursor() as cur:
        return _relation_exists(cur, relation)


def _log_model_run(
    conn: psycopg.Connection,
    run_id: UUID,
    model: Model,
    status: str,
    rows: int | None,
    full_refresh: bool,
    error: str | None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.model_runs
                (run_id, model_name, materialized, status, rows_affected,
                 full_refresh, error, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    CASE WHEN %s = 'running' THEN NULL ELSE now() END)
            RETURNING model_run_id
            """,
            (
                str(run_id),
                model.name,
                model.materialized,
                status,
                rows,
                full_refresh,
                error,
                status,
            ),
        )
        return cur.fetchone()[0]


def _finish_model_run(
    conn: psycopg.Connection,
    model_run_id: int,
    status: str,
    rows: int | None,
    error: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE meta.model_runs
               SET status = %s, rows_affected = %s, error = %s, finished_at = now()
             WHERE model_run_id = %s
            """,
            (status, rows, error, model_run_id),
        )
