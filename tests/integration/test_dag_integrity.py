"""
Integration test: Airflow DAG import and structure validation.

Verifies that:
  1. All DAGs in airflow/dags/ parse without import errors
  2. The expected DAGs are present
  3. Each DAG has the correct number of tasks
  4. Task dependencies form the expected linear / parallel structure

This test uses a lightweight SQLite Airflow DB (no Docker needed).
"""

import os

import pytest

pytest.importorskip("airflow", reason="apache-airflow not installed")


@pytest.fixture(scope="module", autouse=True)
def _airflow_home(tmp_path_factory, monkeymodule):
    """Configure Airflow to use a temporary SQLite database for DAG loading."""
    airflow_home = tmp_path_factory.mktemp("airflow_home")
    monkeymodule.setenv("AIRFLOW_HOME", str(airflow_home))
    monkeymodule.setenv(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        f"sqlite:///{airflow_home}/airflow.db",
    )
    monkeymodule.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    monkeymodule.setenv("AIRFLOW__CORE__EXECUTOR", "SequentialExecutor")
    monkeymodule.setenv(
        "PYTHONPATH", os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    )


@pytest.fixture(scope="module")
def dag_bag():
    from airflow.models import DagBag

    return DagBag(
        dag_folder=os.path.join(os.path.dirname(__file__), "../../airflow/dags"),
        include_examples=False,
    )


class TestDagImport:
    def test_no_import_errors(self, dag_bag):
        assert dag_bag.import_errors == {}, "DAG import errors found:\n" + "\n".join(
            f"  {k}: {v}" for k, v in dag_bag.import_errors.items()
        )

    def test_expected_dags_present(self, dag_bag):
        expected = {"defi_ingest", "defi_transform"}
        actual = set(dag_bag.dags.keys())
        assert expected.issubset(actual), f"Missing DAGs: {expected - actual}"

    def test_dag_count(self, dag_bag):
        assert len(dag_bag.dags) >= 2


class TestDefiIngestDag:
    def test_task_count(self, dag_bag):
        dag = dag_bag.get_dag("defi_ingest")
        assert dag is not None
        assert len(dag.tasks) == 5  # 3 fetch + validate + trigger

    def test_expected_tasks_present(self, dag_bag):
        dag = dag_bag.get_dag("defi_ingest")
        task_ids = {t.task_id for t in dag.tasks}
        expected = {
            "fetch_aave_positions",
            "fetch_compound_positions",
            "fetch_maker_vaults",
            "validate_raw_counts",
            "trigger_defi_transform",
        }
        assert expected == task_ids

    def test_fetch_tasks_are_parallel(self, dag_bag):
        """The three fetch tasks should have no dependencies on each other."""
        dag = dag_bag.get_dag("defi_ingest")
        aave_task = dag.get_task("fetch_aave_positions")
        compound_task = dag.get_task("fetch_compound_positions")
        assert "fetch_compound_positions" not in {t.task_id for t in aave_task.downstream_list}
        assert "fetch_aave_positions" not in {t.task_id for t in compound_task.downstream_list}

    def test_validate_depends_on_all_fetch_tasks(self, dag_bag):
        dag = dag_bag.get_dag("defi_ingest")
        validate_task = dag.get_task("validate_raw_counts")
        upstream_ids = {t.task_id for t in validate_task.upstream_list}
        assert "fetch_aave_positions" in upstream_ids
        assert "fetch_compound_positions" in upstream_ids
        assert "fetch_maker_vaults" in upstream_ids

    def test_schedule_is_6_hourly(self, dag_bag):
        dag = dag_bag.get_dag("defi_ingest")
        assert dag.schedule_interval == "0 */6 * * *"


class TestDefiTransformDag:
    def test_task_count(self, dag_bag):
        dag = dag_bag.get_dag("defi_transform")
        assert dag is not None
        assert len(dag.tasks) == 5  # bronze, silver, dbt_run, dbt_test, done

    def test_expected_tasks_present(self, dag_bag):
        dag = dag_bag.get_dag("defi_transform")
        task_ids = {t.task_id for t in dag.tasks}
        expected = {"spark_bronze", "spark_silver", "dbt_run", "dbt_test", "notify_done"}
        assert expected == task_ids

    def test_linear_dependency_chain(self, dag_bag):
        """bronze → silver → dbt_run → dbt_test → done"""
        dag = dag_bag.get_dag("defi_transform")

        def downstream_ids(task_id):
            return {t.task_id for t in dag.get_task(task_id).downstream_list}

        assert "spark_silver" in downstream_ids("spark_bronze")
        assert "dbt_run" in downstream_ids("spark_silver")
        assert "dbt_test" in downstream_ids("dbt_run")
        assert "notify_done" in downstream_ids("dbt_test")

    def test_no_independent_schedule(self, dag_bag):
        dag = dag_bag.get_dag("defi_transform")
        assert dag.schedule_interval is None
