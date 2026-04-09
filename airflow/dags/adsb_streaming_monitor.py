from airflow.decorators import dag, task
from datetime import datetime, timedelta

# NOTE: Using SQL warehouse for simplicity. For production, consider using the
# Databricks Files API to check Volume file timestamps directly — faster, cheaper,
# and does not require a running SQL warehouse. The Files API approach is more
# robust for a high-frequency health check running every 10 minutes.

default_args  = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "owner" : "airflow",
    "start_date": datetime(2024, 1, 1)
}


@dag(
    dag_id="adsb_streaming_monitor",
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args
)
def adsb_streaming_monitor():

    @task
    def check_stream_health():
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
        from datetime import datetime, timezone
        import logging

        log = logging.getLogger(__name__)

        BRONZE_PATH = "/Volumes/workspace/default/adsb_data/bronze/live_states"
        TASK_TIMEOUT = 999999999 #300
        query = f"SELECT MAX(ingested_at) as last_ingested FROM delta.`{BRONZE_PATH}`"
        try:

            hook = DatabricksSqlHook(databricks_conn_id='databricks_sql_default')
            result = hook.get_records(query)

            if not result or result[0][0] is None:
                return {"healthy": False, "age_seconds": -1}

            last_ingested_dt = datetime.strptime(result[0][0], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
            age_seconds = int((datetime.now(timezone.utc) - last_ingested_dt).total_seconds())


            return {"healthy": age_seconds < TASK_TIMEOUT, "age_seconds": age_seconds}
        except Exception as e:
            log.error("check_stream_health failed: %s", str(e))
            return {"healthy": False, "age_seconds": -1}
    
    @task
    def stream_ok(health:dict):
        import logging

        log = logging.getLogger(__name__)

        log.info(f"Stream is healthy — last record ingested {health['age_seconds']} seconds ago")

    @task
    def restart_stream():
        import logging

        log = logging.getLogger(__name__)
        log.info(f"Stream is not healthy -- manual restart required. See airflow/NOTES.md for details.")

    @task.branch
    def decide_action(health: dict):
        if health["healthy"]:
            return "stream_ok"
        return "restart_stream"
    
    @task(trigger_rule='none_failed_min_one_success')
    def notify(health):
        import logging

        log = logging.getLogger(__name__)
        if health['healthy']:
            log.info(f"Pipeline status: stream healthy — last record was {health['age_seconds']} seconds ago")
        else:
            log.info(f"Pipeline status: stream not healthy — last record was {health['age_seconds']} seconds ago")
      


    health = check_stream_health()
    branch = decide_action(health)
    branch >> [stream_ok(health), restart_stream()] >> notify(health)
    


adsb_streaming_monitor()