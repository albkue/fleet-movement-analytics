"""Parse dbt-style SQL models and resolve their dependency graph.

A model is one .sql file holding one SELECT. It declares itself with a
config block and references other models through ref(), which is what lets
the runner derive build order instead of the author maintaining it by hand:

    {{ config(materialized='incremental', unique_key='ping_id') }}

    select ... from {{ ref('stg_pings') }}

Supported substitutions:
    {{ config(...) }}            declaration; removed from the compiled SQL
    {{ ref('model') }}           -> schema.model, and records an edge
    {{ source('raw', 'pings') }} -> raw.pings (a table this project does
                                    not build, i.e. a graph root)
    {{ this }}                   -> the model's own relation
    {{ is_incremental() }}       -> 'true'/'false' literal
    {{ var('name') }}            -> a value supplied by the runner

    {% if is_incremental() %} ... {% endif %}
        Kept only on an incremental build. This has to be a real conditional
        rather than a `where ... or false` trick, because the guarded SQL
        usually references {{ this }} -- and Postgres resolves relations at
        parse time, so on the very first build (when the table does not exist
        yet) even unreachable SQL would fail.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

# Which schema each model directory builds into. Staging and intermediate
# models are plumbing and share a schema; only marts are meant to be queried
# by anything outside this project.
DEFAULT_SCHEMAS: dict[str, str] = {
    "staging": "stg",
    "intermediate": "stg",
    "marts": "mart",
}

MATERIALIZATIONS = frozenset({"view", "table", "incremental"})

_CONFIG_RE = re.compile(r"\{\{\s*config\((?P<args>.*?)\)\s*\}\}", re.DOTALL)
_REF_RE = re.compile(r"\{\{\s*ref\(\s*['\"](?P<name>[\w.]+)['\"]\s*\)\s*\}\}")
_SOURCE_RE = re.compile(
    r"\{\{\s*source\(\s*['\"](?P<schema>\w+)['\"]\s*,\s*['\"](?P<table>\w+)['\"]\s*\)\s*\}\}"
)
_THIS_RE = re.compile(r"\{\{\s*this\s*\}\}")
_IS_INCREMENTAL_RE = re.compile(r"\{\{\s*is_incremental\(\)\s*\}\}")
_VAR_RE = re.compile(r"\{\{\s*var\(\s*['\"](?P<name>\w+)['\"]\s*\)\s*\}\}")
_IF_INCREMENTAL_BLOCK_RE = re.compile(
    r"\{%\s*if\s+is_incremental\(\)\s*%\}(?P<body>.*?)\{%\s*endif\s*%\}",
    re.DOTALL,
)
_LEFTOVER_RE = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.DOTALL)


class ModelError(ValueError):
    """A model file is malformed or the graph does not resolve."""


@dataclass(frozen=True)
class Model:
    name: str
    path: Path
    schema: str
    materialized: str
    unique_key: str | None
    indexes: tuple[str, ...]
    description: str
    raw_sql: str
    depends_on: tuple[str, ...]

    @property
    def relation(self) -> str:
        return f"{self.schema}.{self.name}"


def _parse_config(sql: str, path: Path) -> dict[str, Any]:
    match = _CONFIG_RE.search(sql)
    if match is None:
        return {}

    args = match.group("args").strip()
    if not args:
        return {}
    try:
        # Parsing as a call expression rather than splitting on commas keeps
        # list values like indexes=['a', 'b, c'] intact.
        node = ast.parse(f"_f({args})", mode="eval")
        call = node.body
        if not isinstance(call, ast.Call) or call.args:
            raise ModelError(f"{path.name}: config() takes keyword arguments only")
        return {kw.arg: ast.literal_eval(kw.value) for kw in call.keywords}
    except ModelError:
        raise
    except (SyntaxError, ValueError) as exc:
        raise ModelError(f"{path.name}: could not parse config(): {exc}") from exc


def load_model(path: Path, *, default_schema: str) -> Model:
    """Read one .sql file into a Model."""
    raw_sql = path.read_text(encoding="utf-8")
    config = _parse_config(raw_sql, path)

    unknown = config.keys() - {
        "materialized",
        "unique_key",
        "schema",
        "indexes",
        "description",
    }
    if unknown:
        raise ModelError(f"{path.name}: unknown config key(s) {sorted(unknown)}")

    materialized = config.get("materialized", "view")
    if materialized not in MATERIALIZATIONS:
        raise ModelError(
            f"{path.name}: materialized must be one of {sorted(MATERIALIZATIONS)}, "
            f"got {materialized!r}"
        )

    unique_key = config.get("unique_key")
    if materialized == "incremental" and not unique_key:
        # Without a key there is no way to replace a previously-loaded row,
        # so an incremental rebuild would duplicate instead of update.
        raise ModelError(
            f"{path.name}: materialized='incremental' requires a unique_key"
        )

    indexes = config.get("indexes", [])
    if not isinstance(indexes, list) or not all(isinstance(i, str) for i in indexes):
        raise ModelError(f"{path.name}: indexes must be a list of strings")
    if indexes and materialized == "view":
        raise ModelError(f"{path.name}: a view cannot have indexes")

    depends_on = tuple(dict.fromkeys(_REF_RE.findall(raw_sql)))

    return Model(
        name=path.stem,
        path=path,
        schema=config.get("schema", default_schema),
        materialized=materialized,
        unique_key=unique_key,
        indexes=tuple(indexes),
        description=config.get("description", ""),
        raw_sql=raw_sql,
        depends_on=depends_on,
    )


def discover_models(models_dir: Path) -> dict[str, Model]:
    """Load every models/**/*.sql file, keyed by model name."""
    if not models_dir.exists():
        raise FileNotFoundError(f"Models directory not found: {models_dir}")

    models: dict[str, Model] = {}
    for path in sorted(models_dir.rglob("*.sql")):
        # The immediate parent directory picks the default schema.
        folder = path.parent.name
        default_schema = DEFAULT_SCHEMAS.get(folder)
        if default_schema is None:
            raise ModelError(
                f"{path}: models must live in one of "
                f"{sorted(DEFAULT_SCHEMAS)}, found {folder!r}"
            )

        model = load_model(path, default_schema=default_schema)
        if model.name in models:
            raise ModelError(
                f"duplicate model name {model.name!r}: "
                f"{models[model.name].path} and {path}"
            )
        models[model.name] = model

    if not models:
        raise ModelError(f"No .sql models found under {models_dir}")
    return models


def resolve_order(models: dict[str, Model]) -> list[Model]:
    """Return models in a valid build order (Kahn's algorithm).

    Ties are broken alphabetically so the order is stable between runs --
    a build that reorders itself run to run is miserable to debug.
    """
    for model in models.values():
        for dependency in model.depends_on:
            if dependency not in models:
                raise ModelError(
                    f"{model.name} refs {dependency!r}, which is not a model. "
                    f"Use source() for tables this project does not build."
                )

    remaining = {name: set(m.depends_on) for name, m in models.items()}
    ordered: list[Model] = []

    while remaining:
        ready = sorted(name for name, deps in remaining.items() if not deps)
        if not ready:
            cycle = " -> ".join(sorted(remaining))
            raise ModelError(f"dependency cycle among models: {cycle}")

        for name in ready:
            ordered.append(models[name])
            del remaining[name]
        for deps in remaining.values():
            deps.difference_update(ready)

    return ordered


def select_models(
    models: dict[str, Model], names: Iterable[str] | None
) -> list[Model]:
    """Build order for `names` plus everything they depend on.

    Selecting a model without its upstreams would run it against whatever
    stale state happened to be there, so ancestors are always pulled in.
    """
    if not names:
        return resolve_order(models)

    wanted: set[str] = set()
    stack = list(names)
    while stack:
        name = stack.pop()
        if name in wanted:
            continue
        if name not in models:
            raise ModelError(
                f"unknown model {name!r}; known models: {', '.join(sorted(models))}"
            )
        wanted.add(name)
        stack.extend(models[name].depends_on)

    return [m for m in resolve_order(models) if m.name in wanted]


def compile_sql(
    model: Model,
    models: dict[str, Model],
    *,
    is_incremental: bool,
    variables: dict[str, Any],
) -> str:
    """Render a model's raw SQL into executable SQL."""
    sql = _CONFIG_RE.sub("", model.raw_sql)
    # Resolved first: on a non-incremental build the guarded SQL is deleted
    # outright, so anything inside it is never compiled or parsed.
    sql = _IF_INCREMENTAL_BLOCK_RE.sub(
        (lambda m: m.group("body")) if is_incremental else (lambda m: ""), sql
    )
    sql = _REF_RE.sub(lambda m: models[m.group("name")].relation, sql)
    sql = _SOURCE_RE.sub(lambda m: f"{m.group('schema')}.{m.group('table')}", sql)
    sql = _THIS_RE.sub(model.relation, sql)
    sql = _IS_INCREMENTAL_RE.sub("true" if is_incremental else "false", sql)

    def _var(match: re.Match[str]) -> str:
        name = match.group("name")
        if name not in variables:
            raise ModelError(
                f"{model.name}: var({name!r}) is not defined by the runner"
            )
        value = variables[name]
        # Numbers and booleans inline bare; anything else is quoted as a
        # string literal so a stray value cannot alter the statement.
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    sql = _VAR_RE.sub(_var, sql)

    leftover = _LEFTOVER_RE.search(sql)
    if leftover:
        # Better to fail than to send `{{ ... }}` to Postgres and get a
        # confusing syntax error pointing at the wrong thing.
        raise ModelError(
            f"{model.name}: unsupported template expression {leftover.group(0)!r}"
        )

    return sql.strip().rstrip(";").strip()
