from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.models import Variable

default_args  = {
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "owner" : "airflow",
    "start_date": datetime(2024, 1, 1),
    "execution_timeout" : timedelta(hours=3)
}

@dag(
    dag_id="adsb_nightly_batch",
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args
)
def adsb_nightly_batch():


    upload_bronze = SimpleHttpOperator(
        task_id="upload_bronze",
        http_conn_id="fastapi_default",
        endpoint="/upload-bronze",
        method="POST",
        response_check=lambda response: response.status_code == 200,
        extra_options={"timeout": 1200}  # 20 min — upload can take a while still needs testing
    )

    @task
    def validate_bronze():
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
        from datetime import datetime, timezone
        import logging

        log = logging.getLogger(__name__)
        #threshold is set to 0 for testing 10k for prod
        ROW_COUNT_THRESHOLD = 0 #10000

        BRONZE_PATH = "/Volumes/workspace/default/adsb_data/bronze"
        query = f'''SELECT count(*) as rows_count 
                    FROM delta.`{BRONZE_PATH}/live_states` 
                    WHERE ingest_date = current_date() - INTERVAL 1 DAY'''
        try:
            hook = DatabricksSqlHook(databricks_conn_id='databricks_sql_default')
            result = hook.get_records(query)

            if not result or result[0][0] is None:
                raise ValueError("No data found for today's partition")

            row_count = result[0][0]
        except Exception as e:
            log.error(f"System failed {e}")
            raise

        if row_count < ROW_COUNT_THRESHOLD:
            raise ValueError(f"row count is less than {ROW_COUNT_THRESHOLD}")
        return {"row_count":row_count}
    

    run_silver = DatabricksRunNowOperator(
        task_id='run_silver',
        databricks_conn_id='databricks_default',
        job_id=Variable.get('databricks_job_silver_flights')
    )

    run_anomaly_detection = DatabricksRunNowOperator(
        task_id='run_anomaly_detection',
        databricks_conn_id='databricks_default',
        job_id=Variable.get('databricks_job_anomaly_detect')
    )

    run_gold = DatabricksRunNowOperator(
        task_id='run_gold',
        databricks_conn_id='databricks_default',
        job_id=Variable.get('databricks_job_gold_aggregates')
    )

    RUN_DBT_TIMEOUT = 300 #5 min change if more tasks are added to dbt logic

    run_dbt = SimpleHttpOperator(
        task_id="run_dbt",
        http_conn_id="fastapi_default",
        endpoint="/run-dbt",
        method="POST",
        response_check=lambda response: response.status_code == 200,
        extra_options={"timeout": RUN_DBT_TIMEOUT}
    )

    test_dbt = SimpleHttpOperator(
        task_id="test_dbt",
        http_conn_id="fastapi_default",
        endpoint="/test-dbt",
        method="POST",
        response_check=lambda response: response.status_code == 200,
        extra_options={"timeout": RUN_DBT_TIMEOUT}
    )


    @task(trigger_rule='all_done')
    def vacuum_tables():
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
        import logging

        log = logging.getLogger(__name__)

        BRONZE_PATH = "/Volumes/workspace/default/adsb_data/bronze"
        SILVER_PATH = "/Volumes/workspace/default/adsb_data/silver"
        tables = [
                f"{BRONZE_PATH}/live_states",
                f"{SILVER_PATH}/flights",
                f"{SILVER_PATH}/anomalies"
            ]
        RETENTION_HOURS = 168

        try:
            hook = DatabricksSqlHook(databricks_conn_id='databricks_sql_default')
            for table in tables:
                query = F'''VACUUM delta.`{table}` RETAIN {RETENTION_HOURS} HOURS'''
                hook.run(query)
        except Exception as e:
            log.error(f"Vacuum failed: {e}")
    

    upload_bronze >> validate_bronze() >> run_silver >> run_anomaly_detection >> run_gold >> run_dbt >> test_dbt >> vacuum_tables()

        

adsb_nightly_batch()