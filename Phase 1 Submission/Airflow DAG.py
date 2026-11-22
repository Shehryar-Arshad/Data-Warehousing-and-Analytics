"""
Airflow DAG: Phase 1 SDI - Pharma Sales & Hospital Demand
CSV files -> staging (stg) -> dimensions + facts (dwh, fact constellation) -> data-quality gate -> query layer (mart)

Trigger manually from the Airflow UI ("Trigger DAG"); it is a full refresh, so it can be re-run safely.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

try:                                   # Airflow 3
    from airflow.sdk import dag, task, task_group
except ImportError:                    # Airflow 2.x (docker-compose in this project uses 2.10.5)
    from airflow.decorators import dag, task, task_group

sys.path.insert(0, os.path.dirname(__file__))
from pharma_sdi import etl  # noqa: E402

DEFAULT_ARGS = {
    "owner": "sdi-group",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="pharma_sdi_phase1",
    description="Phase 1 SDI: CSV -> staging -> constellation DWH -> DQ gate -> query layer",
    schedule=None,                                     # manual trigger (source files are static in Phase 1)
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Karachi"),
    catchup=False,
    max_active_runs=1,                                 # a full refresh must never overlap itself
    default_args=DEFAULT_ARGS,
    params={"date_start": "2023-01-01", "date_end": "2025-12-31"},   # DimDate range, editable at trigger time
    tags=["sdi", "pharma", "phase1"],
    doc_md=__doc__,
)
def pharma_sdi_phase1():

    @task
    def check_sources(run_id=None):
        return etl.check_sources(run_id)

    @task
    def create_schema(run_id=None):
        etl.create_schema(run_id)

    @task_group(group_id="staging")
    def staging():
        @task
        def stage_master(run_id=None):
            return etl.load_staging(run_id, "master_data")

        @task
        def stage_sales(run_id=None):
            return etl.load_staging(run_id, "pharmacy_sales")

        @task
        def stage_demand(run_id=None):
            return etl.load_staging(run_id, "hospital_demand")

        stage_master()
        stage_sales()
        stage_demand()

    @task_group(group_id="dimensions")
    def dimensions():
        @task
        def dim_date(run_id=None, params=None):
            return etl.load_dimension(run_id, "dim_date", params["date_start"], params["date_end"])

        @task
        def dim_location(run_id=None):
            return etl.load_dimension(run_id, "dim_location")

        @task
        def dim_medicine(run_id=None):
            return etl.load_dimension(run_id, "dim_medicine")

        @task
        def dim_pharmacy(run_id=None):
            return etl.load_dimension(run_id, "dim_pharmacy")

        @task
        def dim_channel(run_id=None):
            return etl.load_dimension(run_id, "dim_channel")

        @task
        def dim_hospital(run_id=None):
            return etl.load_dimension(run_id, "dim_hospital")

        dim_date()
        dim_location()
        dim_medicine()
        dim_pharmacy()
        dim_channel()
        dim_hospital()

    @task_group(group_id="facts")
    def facts():
        @task
        def fact_sales(run_id=None):
            return etl.load_fact(run_id, "fact_sales")

        @task
        def fact_demand(run_id=None):
            return etl.load_fact(run_id, "fact_demand")

        fact_sales()
        fact_demand()

    @task
    def dq_checks(run_id=None):
        return etl.run_dq_checks(run_id)

    @task
    def create_query_layer(run_id=None):
        return etl.create_query_layer(run_id)

    @task
    def run_queries(run_id=None):
        return etl.run_queries(run_id)

    @task
    def pipeline_summary(run_id=None):
        return etl.pipeline_summary(run_id)

    s = check_sources()
    c = create_schema()
    stg = staging()
    dims = dimensions()
    fct = facts()
    dq = dq_checks()
    ql = create_query_layer()
    q = run_queries()
    summ = pipeline_summary()

    s >> c >> stg >> dims >> fct >> dq >> ql >> q >> summ


pharma_sdi_phase1()
