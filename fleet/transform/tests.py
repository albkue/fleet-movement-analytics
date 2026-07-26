"""dbt-style schema tests declared in models/**/schema.yml.

Every test compiles to a query that counts rows violating an assertion, so
zero is a pass. Supported:

    column tests   not_null, unique, accepted_values, relationships
    model tests    assert_expression   (a boolean every row must satisfy)
                   unique_combination  (a compound grain is unique)

Example:

    version: 2
    models:
      - name: fct_trips
        tests:
          - assert_expression: "actual_end_at >= actual_start_at"
          - unique_combination:
              columns: [vehicle_id, actual_start_at]
        columns:
          - name: trip_id
            tests: [not_null, unique]
          - name: vehicle_type
            tests:
              - accepted_values:
                  values: ['van', 'truck', 'motorbike']
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg
import yaml

from ..config import Settings
from .models import Model, ModelError, discover_models
from .runner import _identifier, _relation

log = logging.getLogger(__name__)

COLUMN_TESTS = frozenset(
    {"not_null", "unique", "accepted_values", "relationships"}
)
MODEL_TESTS = frozenset({"assert_expression", "unique_combination"})


@dataclass(frozen=True)
class SchemaTest:
    model_name: str
    column_name: str | None
    test_name: str
    args: dict[str, Any]
    source: Path


@dataclass
class TestResult:
    test: SchemaTest
    status: str  # pass | fail | error
    failing_rows: int | None = None
    error: str | None = None


@dataclass
class TestSummary:
    run_id: UUID
    results: list[TestResult] = field(default_factory=list)

    @property
    def status(self) -> str:
        if any(r.status == "error" for r in self.results):
            return "error"
        return "fail" if any(r.status == "fail" for r in self.results) else "pass"

    def counts(self) -> dict[str, int]:
        out = {"pass": 0, "fail": 0, "error": 0}
        for result in self.results:
            out[result.status] += 1
        return out


def _normalise(entry: Any, source: Path, model_name: str, column: str | None) -> SchemaTest:
    """Accept both `tests: [not_null]` and `tests: [{relationships: {...}}]`."""
    if isinstance(entry, str):
        return SchemaTest(model_name, column, entry, {}, source)
    if isinstance(entry, dict) and len(entry) == 1:
        name, args = next(iter(entry.items()))
        return SchemaTest(model_name, column, name, args or {}, source)
    raise ModelError(
        f"{source.name}: malformed test entry {entry!r} on "
        f"{model_name}.{column or '*'}"
    )


def discover_tests(models_dir: Path, models: dict[str, Model]) -> list[SchemaTest]:
    """Parse every schema.yml under the models directory."""
    tests: list[SchemaTest] = []

    for path in sorted(models_dir.rglob("schema.yml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for entry in doc.get("models", []):
            model_name = entry.get("name")
            if not model_name:
                raise ModelError(f"{path.name}: a models entry is missing 'name'")
            if model_name not in models:
                raise ModelError(
                    f"{path.name}: tests declared for unknown model {model_name!r}"
                )

            for raw in entry.get("tests", []) or []:
                test = _normalise(raw, path, model_name, None)
                if test.test_name not in MODEL_TESTS:
                    raise ModelError(
                        f"{path.name}: {test.test_name!r} is a column test; "
                        f"put it under a column, not the model"
                    )
                tests.append(test)

            for column in entry.get("columns", []) or []:
                column_name = column.get("name")
                if not column_name:
                    raise ModelError(
                        f"{path.name}: a column of {model_name} is missing 'name'"
                    )
                for raw in column.get("tests", []) or []:
                    test = _normalise(raw, path, model_name, column_name)
                    if test.test_name not in COLUMN_TESTS:
                        raise ModelError(
                            f"{path.name}: unknown test {test.test_name!r} on "
                            f"{model_name}.{column_name}"
                        )
                    tests.append(test)

    return tests


def build_query(test: SchemaTest, models: dict[str, Model]) -> tuple[str, list[Any]]:
    """Compile a test into a query returning the count of violating rows."""
    model = models[test.model_name]
    relation = _relation(model)
    column = _identifier(test.column_name) if test.column_name else None

    if test.test_name == "not_null":
        return f"SELECT count(*) FROM {relation} WHERE {column} IS NULL", []

    if test.test_name == "unique":
        # Counts the surplus rows, not the number of duplicated values, so a
        # value appearing 3 times reports 2 violations.
        return (
            f"SELECT coalesce(sum(n) - count(*), 0) FROM ("
            f"  SELECT count(*) AS n FROM {relation} "
            f"  WHERE {column} IS NOT NULL GROUP BY {column}"
            f") d",
            [],
        )

    if test.test_name == "accepted_values":
        values = test.args.get("values")
        if not isinstance(values, list) or not values:
            raise ModelError(
                f"{test.model_name}.{test.column_name}: accepted_values needs "
                f"a non-empty 'values' list"
            )
        # NULLs are the not_null test's business, so they are excluded here.
        return (
            f"SELECT count(*) FROM {relation} "
            f"WHERE {column} IS NOT NULL AND NOT ({column}::text = ANY(%s))",
            [[str(v) for v in values]],
        )

    if test.test_name == "relationships":
        to_model = test.args.get("to")
        field_name = test.args.get("field")
        if not to_model or not field_name:
            raise ModelError(
                f"{test.model_name}.{test.column_name}: relationships needs "
                f"'to' and 'field'"
            )
        if to_model not in models:
            raise ModelError(
                f"{test.model_name}.{test.column_name}: relationships 'to' "
                f"references unknown model {to_model!r}"
            )
        parent = _relation(models[to_model])
        parent_column = _identifier(field_name)
        return (
            f"SELECT count(*) FROM {relation} child "
            f"WHERE child.{column} IS NOT NULL AND NOT EXISTS ("
            f"  SELECT 1 FROM {parent} parent "
            f"  WHERE parent.{parent_column} = child.{column})",
            [],
        )

    if test.test_name == "unique_combination":
        columns = test.args.get("columns")
        if not isinstance(columns, list) or len(columns) < 2:
            raise ModelError(
                f"{test.model_name}: unique_combination needs a 'columns' list "
                f"of at least two column names"
            )
        grain = ", ".join(_identifier(str(c)) for c in columns)
        return (
            f"SELECT coalesce(sum(n) - count(*), 0) FROM ("
            f"  SELECT count(*) AS n FROM {relation} GROUP BY {grain}"
            f") d",
            [],
        )

    if test.test_name == "assert_expression":
        expression = test.args if isinstance(test.args, str) else test.args.get("expression")
        if not isinstance(expression, str) or not expression.strip():
            raise ModelError(
                f"{test.model_name}: assert_expression needs a SQL boolean"
            )
        # `IS NOT TRUE` rather than `NOT (...)` so a NULL result counts as a
        # violation instead of vanishing from both sides of the comparison.
        return f"SELECT count(*) FROM {relation} WHERE ({expression}) IS NOT TRUE", []

    raise ModelError(f"unknown test {test.test_name!r}")


def run(settings: Settings, *, select: list[str] | None = None) -> TestSummary:
    """Execute every schema test and record the outcome in meta.test_results."""
    models = discover_models(settings.models_dir)
    tests = discover_tests(settings.models_dir, models)
    if select:
        wanted = set(select)
        tests = [t for t in tests if t.model_name in wanted]

    summary = TestSummary(run_id=uuid4())
    conn = psycopg.connect(settings.dsn, autocommit=True)
    try:
        for test in tests:
            result = _execute(conn, test, models)
            summary.results.append(result)
            _record(conn, summary.run_id, result)

            label = f"{test.model_name}.{test.column_name or '*'}: {test.test_name}"
            if result.status == "pass":
                log.debug("PASS %s", label)
            else:
                log.warning(
                    "%s %s (%s)",
                    result.status.upper(),
                    label,
                    result.error or f"{result.failing_rows} failing row(s)",
                )
    finally:
        conn.close()

    return summary


def _execute(
    conn: psycopg.Connection, test: SchemaTest, models: dict[str, Model]
) -> TestResult:
    try:
        query, params = build_query(test, models)
        with conn.cursor() as cur:
            cur.execute(query, params or None)
            failing = int(cur.fetchone()[0])
    except Exception as exc:
        # An error is not a failure: a missing relation or a bad test
        # definition should not read as "the data is wrong".
        return TestResult(test=test, status="error", error=f"{type(exc).__name__}: {exc}")

    return TestResult(
        test=test,
        status="pass" if failing == 0 else "fail",
        failing_rows=failing,
    )


def _record(conn: psycopg.Connection, run_id: UUID, result: TestResult) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.test_results
                (run_id, model_name, column_name, test_name, status,
                 failing_rows, error)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(run_id),
                result.test.model_name,
                result.test.column_name,
                result.test.test_name,
                result.status,
                result.failing_rows,
                result.error,
            ),
        )
