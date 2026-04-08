from airflow.decorators import dag, task
from datetime import datetime, timedelta


default_args  = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "owner" : "airflow"
}


@dag(
    dag_id="adsb_streaming_monitor",
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args
)
def adbs_streaming_monitor():
    @task
    def check_stream_health():
        pass

    pass

adbs_streaming_monitor()