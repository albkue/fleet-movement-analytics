"""The dbt-style model layer: parsing, the DAG, and SQL compilation."""

from __future__ import annotations

import pytest

from fleet.config import PROJECT_ROOT, Settings
from fleet.transform.models import (
    ModelError,
    compile_sql,
    discover_models,
    load_model,
    resolve_order,
    select_models,
)
from fleet.transform.runner import build_variables
from fleet.transform.tests import build_query, discover_tests

MODELS_DIR = PROJECT_ROOT / "models"


def write(tmp_path, folder: str, name: str, sql: str):
    directory = tmp_path / folder
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.sql"
    path.write_text(sql, encoding="utf-8")
    return path


@pytest.fixture(scope="module")
def models():
    return discover_models(MODELS_DIR)


# --------------------------------------------------------------- parsing ----


def test_config_block_is_parsed(tmp_path):
    path = write(
        tmp_path,
        "staging",
        "stg_thing",
        "{{ config(materialized='incremental', unique_key='id',\n"
        "          indexes=['a', 'b, c'], description='hi') }}\nselect 1",
    )

    model = load_model(path, default_schema="stg")

    assert model.materialized == "incremental"
    assert model.unique_key == "id"
    # A compound index must survive as one entry, not be split on its comma.
    assert model.indexes == ("a", "b, c")
    assert model.description == "hi"


def test_default_materialization_is_a_view(tmp_path):
    path = write(tmp_path, "marts", "m", "select 1")

    assert load_model(path, default_schema="mart").materialized == "view"


def test_unknown_config_key_is_refused(tmp_path):
    path = write(tmp_path, "marts", "m", "{{ config(clustered_by='x') }}\nselect 1")

    with pytest.raises(ModelError, match="unknown config key"):
        load_model(path, default_schema="mart")


def test_unknown_materialization_is_refused(tmp_path):
    path = write(tmp_path, "marts", "m", "{{ config(materialized='cube') }}\nselect 1")

    with pytest.raises(ModelError, match="materialized must be one of"):
        load_model(path, default_schema="mart")


def test_incremental_without_a_unique_key_is_refused(tmp_path):
    """Without a key, an incremental rebuild duplicates instead of updating."""
    path = write(
        tmp_path, "marts", "m", "{{ config(materialized='incremental') }}\nselect 1"
    )

    with pytest.raises(ModelError, match="requires a unique_key"):
        load_model(path, default_schema="mart")


def test_a_view_cannot_have_indexes(tmp_path):
    path = write(tmp_path, "marts", "m", "{{ config(indexes=['a']) }}\nselect 1")

    with pytest.raises(ModelError, match="view cannot have indexes"):
        load_model(path, default_schema="mart")


def test_model_in_an_unknown_folder_is_refused(tmp_path):
    write(tmp_path, "scratch", "m", "select 1")

    with pytest.raises(ModelError, match="models must live in one of"):
        discover_models(tmp_path)


def test_duplicate_model_names_across_folders_are_refused(tmp_path):
    write(tmp_path, "staging", "same", "select 1")
    write(tmp_path, "marts", "same", "select 1")

    with pytest.raises(ModelError, match="duplicate model name"):
        discover_models(tmp_path)


def test_an_empty_models_directory_is_refused(tmp_path):
    (tmp_path / "staging").mkdir(parents=True)

    with pytest.raises(ModelError, match="No .sql models found"):
        discover_models(tmp_path)


def test_missing_models_directory_is_reported(tmp_path):
    with pytest.raises(FileNotFoundError):
        discover_models(tmp_path / "nope")


# ------------------------------------------------------------------- DAG ----


def test_refs_become_edges(tmp_path):
    write(tmp_path, "staging", "a", "select 1")
    write(tmp_path, "marts", "b", "select * from {{ ref('a') }}")

    models = discover_models(tmp_path)

    assert models["b"].depends_on == ("a",)
    assert models["a"].depends_on == ()


def test_build_order_puts_parents_first(tmp_path):
    write(tmp_path, "staging", "a", "select 1")
    write(tmp_path, "intermediate", "b", "select * from {{ ref('a') }}")
    write(tmp_path, "marts", "c", "select * from {{ ref('b') }}")

    order = [m.name for m in resolve_order(discover_models(tmp_path))]

    assert order.index("a") < order.index("b") < order.index("c")


def test_build_order_is_stable_between_runs(tmp_path):
    for name in ("a", "b", "c", "d"):
        write(tmp_path, "marts", name, "select 1")

    models = discover_models(tmp_path)

    assert [m.name for m in resolve_order(models)] == [
        m.name for m in resolve_order(models)
    ]


def test_a_cycle_is_reported_not_hung_on(tmp_path):
    write(tmp_path, "marts", "a", "select * from {{ ref('b') }}")
    write(tmp_path, "marts", "b", "select * from {{ ref('a') }}")

    with pytest.raises(ModelError, match="dependency cycle"):
        resolve_order(discover_models(tmp_path))


def test_ref_to_a_non_model_is_refused(tmp_path):
    write(tmp_path, "marts", "a", "select * from {{ ref('raw_pings') }}")

    with pytest.raises(ModelError, match="Use source"):
        resolve_order(discover_models(tmp_path))


def test_selecting_a_model_pulls_in_its_ancestors(tmp_path):
    write(tmp_path, "staging", "a", "select 1")
    write(tmp_path, "intermediate", "b", "select * from {{ ref('a') }}")
    write(tmp_path, "marts", "c", "select * from {{ ref('b') }}")
    write(tmp_path, "marts", "unrelated", "select 1")

    selected = [m.name for m in select_models(discover_models(tmp_path), ["c"])]

    assert selected == ["a", "b", "c"]


def test_selecting_an_unknown_model_lists_what_exists(tmp_path):
    write(tmp_path, "marts", "a", "select 1")

    with pytest.raises(ModelError, match="unknown model"):
        select_models(discover_models(tmp_path), ["nope"])


# ----------------------------------------------------------- compilation ----


def test_ref_compiles_to_a_schema_qualified_relation(tmp_path):
    write(tmp_path, "staging", "a", "select 1")
    write(tmp_path, "marts", "b", "select * from {{ ref('a') }}")
    models = discover_models(tmp_path)

    sql = compile_sql(models["b"], models, is_incremental=False, variables={})

    assert "stg.a" in sql


def test_source_compiles_to_the_named_relation(tmp_path):
    write(tmp_path, "staging", "a", "select * from {{ source('raw', 'pings') }}")
    models = discover_models(tmp_path)

    sql = compile_sql(models["a"], models, is_incremental=False, variables={})

    assert "raw.pings" in sql


def test_this_compiles_to_the_models_own_relation(tmp_path):
    write(tmp_path, "staging", "a", "select * from {{ this }}")
    models = discover_models(tmp_path)

    assert "stg.a" in compile_sql(
        models["a"], models, is_incremental=False, variables={}
    )


def test_incremental_block_is_deleted_on_a_full_build(tmp_path):
    """It must be deleted, not disabled.

    The guarded SQL usually references {{ this }}, and Postgres resolves
    relations at parse time -- so on the very first build even unreachable
    SQL would fail if it were left in place.
    """
    write(
        tmp_path,
        "staging",
        "a",
        "select 1 {% if is_incremental() %} where x > (select max(x) "
        "from {{ this }}) {% endif %}",
    )
    models = discover_models(tmp_path)

    full = compile_sql(models["a"], models, is_incremental=False, variables={})
    incremental = compile_sql(models["a"], models, is_incremental=True, variables={})

    assert "max(x)" not in full
    assert "max(x)" in incremental
    assert "stg.a" in incremental


def test_variables_are_substituted_with_their_types(tmp_path):
    write(
        tmp_path,
        "staging",
        "a",
        "select {{ var('n') }}, {{ var('flag') }}, {{ var('name') }}",
    )
    models = discover_models(tmp_path)

    sql = compile_sql(
        models["a"],
        models,
        is_incremental=False,
        variables={"n": 5, "flag": True, "name": "abc"},
    )

    assert "select 5, true, 'abc'" in sql


def test_a_string_variable_cannot_break_out_of_its_literal(tmp_path):
    write(tmp_path, "staging", "a", "select {{ var('name') }}")
    models = discover_models(tmp_path)

    sql = compile_sql(
        models["a"], models, is_incremental=False, variables={"name": "o'brien"}
    )

    assert "'o''brien'" in sql


def test_an_undeclared_variable_is_refused(tmp_path):
    write(tmp_path, "staging", "a", "select {{ var('nope') }}")
    models = discover_models(tmp_path)

    with pytest.raises(ModelError, match="is not defined by the runner"):
        compile_sql(models["a"], models, is_incremental=False, variables={})


def test_an_unsupported_template_expression_is_refused(tmp_path):
    """Better than sending {{ ... }} to Postgres and getting a syntax error
    that points at the wrong thing."""
    write(tmp_path, "staging", "a", "select {{ magic('x') }}")
    models = discover_models(tmp_path)

    with pytest.raises(ModelError, match="unsupported template expression"):
        compile_sql(models["a"], models, is_incremental=False, variables={})


# ------------------------------------------------------ the real project ----


def test_the_project_dag_resolves(models):
    order = [m.name for m in resolve_order(models)]

    assert "stg_pings" in order
    assert order.index("stg_pings") < order.index("int_ping_segments")
    assert order.index("int_ping_segments") < order.index("fct_trips")
    assert order.index("fct_alerts") < order.index("fct_trips")
    assert order.index("fct_trips") < order.index("agg_daily_fleet")


def test_every_project_model_compiles(models):
    settings = Settings(
        pg_host="h", pg_port=1, pg_user="u", pg_password="p", pg_database="d",
        kafka_bootstrap_servers="b", kafka_topic="t", kafka_consumer_group="g",
        kafka_topic_partitions=12, consumer_batch_size=500,
        consumer_batch_timeout_seconds=5.0, consumer_idle_timeout_seconds=0.0,
        idle_speed_kph=3.0, idle_minutes=5, deviation_metres=120.0,
        deviation_seconds=90, delay_minutes=10, delay_restep_minutes=10,
        gps_gap_minutes=10, h3_resolution_coarse=8, h3_resolution_fine=9,
        models_dir=MODELS_DIR, fleet_config_file=".", ping_interval_seconds=15,
        simulator_seed=None,
    )
    variables = build_variables(settings)

    for model in models.values():
        for incremental in (False, True):
            sql = compile_sql(
                model, models, is_incremental=incremental, variables=variables
            )
            # Nothing template-shaped may reach Postgres.
            assert "{{" not in sql and "{%" not in sql, model.name
            # Models open with a comment explaining their grain, so the first
            # statement keyword is whatever survives stripping those.
            body = "\n".join(
                line for line in sql.splitlines() if not line.strip().startswith("--")
            ).strip()
            assert body.lower().startswith(("with", "select")), model.name


def test_staging_and_intermediate_build_into_stg_and_marts_into_mart(models):
    assert models["stg_pings"].schema == "stg"
    assert models["int_ping_segments"].schema == "stg"
    assert models["fct_trips"].schema == "mart"


def test_incremental_models_declare_a_unique_key(models):
    for model in models.values():
        if model.materialized == "incremental":
            assert model.unique_key, model.name


def test_every_model_has_a_description(models):
    """A model nobody can describe in a sentence is usually two models."""
    for model in models.values():
        assert model.description, model.name


# ----------------------------------------------------------- schema tests ----


def test_project_schema_tests_parse(models):
    tests = discover_tests(MODELS_DIR, models)

    assert len(tests) > 50
    assert {t.model_name for t in tests} <= set(models)


def test_every_model_has_at_least_one_declared_test(models):
    tested = {t.model_name for t in discover_tests(MODELS_DIR, models)}

    assert set(models) - tested == set()


def test_every_project_test_compiles_to_a_counting_query(models):
    for test in discover_tests(MODELS_DIR, models):
        query, _ = build_query(test, models)
        assert query.lower().startswith("select ")
        assert "count(" in query.lower() or "sum(" in query.lower()


def test_a_test_on_an_unknown_model_is_refused(tmp_path):
    write(tmp_path, "marts", "a", "select 1")
    (tmp_path / "marts" / "schema.yml").write_text(
        "version: 2\nmodels:\n  - name: ghost\n    columns:\n"
        "      - name: x\n        tests: [not_null]\n",
        encoding="utf-8",
    )

    with pytest.raises(ModelError, match="unknown model"):
        discover_tests(tmp_path, discover_models(tmp_path))


def test_a_model_level_test_under_a_column_is_refused(tmp_path):
    write(tmp_path, "marts", "a", "select 1")
    (tmp_path / "marts" / "schema.yml").write_text(
        "version: 2\nmodels:\n  - name: a\n    columns:\n"
        "      - name: x\n        tests: [assert_expression]\n",
        encoding="utf-8",
    )

    with pytest.raises(ModelError, match="unknown test"):
        discover_tests(tmp_path, discover_models(tmp_path))


def test_unique_test_counts_surplus_rows_not_duplicated_values(tmp_path):
    write(tmp_path, "marts", "a", "select 1")
    (tmp_path / "marts" / "schema.yml").write_text(
        "version: 2\nmodels:\n  - name: a\n    columns:\n"
        "      - name: x\n        tests: [unique]\n",
        encoding="utf-8",
    )
    models = discover_models(tmp_path)
    test = discover_tests(tmp_path, models)[0]

    query, _ = build_query(test, models)

    assert "sum(n) - count(*)" in query


def test_assert_expression_treats_null_as_a_violation(tmp_path):
    """`NOT (...)` would let a NULL result vanish from both sides."""
    write(tmp_path, "marts", "a", "select 1")
    (tmp_path / "marts" / "schema.yml").write_text(
        "version: 2\nmodels:\n  - name: a\n    tests:\n"
        "      - assert_expression: \"x > 0\"\n",
        encoding="utf-8",
    )
    models = discover_models(tmp_path)
    test = discover_tests(tmp_path, models)[0]

    query, _ = build_query(test, models)

    assert "IS NOT TRUE" in query
