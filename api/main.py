# api logic to connect docker containers with local machine (as advised in airflow/NOTES.md)
from fastapi import FastAPI, HTTPException
import subprocess
import psutil
import logging


log = logging.getLogger(__name__)
app = FastAPI()

@app.get("/stream-health")
def stream_health():
    import os
    from datetime import datetime, timezone
    
    BRONZE_PATH = "data/delta/bronze/live_states"
    THRESHOLD_SECONDS = 300  # 5 minutes
    
    # find the most recently modified parquet file
    latest_time = None
    for root, dirs, files in os.walk(BRONZE_PATH):
        for file in files:
            if file.endswith(".parquet"):
                mtime = os.path.getmtime(os.path.join(root, file))
                if latest_time is None or mtime > latest_time:
                    latest_time = mtime
    
    if latest_time is None:
        return {"healthy": False, "age_seconds": -1}
    
    age_seconds = int(datetime.now(timezone.utc).timestamp() - latest_time)
    return {"healthy": age_seconds < THRESHOLD_SECONDS, "age_seconds": age_seconds}

@app.get("/health")
def check_health():
    return {
        "status": "ok"
    }


@app.post("/run-dbt")
def run_dbt():
    dbt_run_result = subprocess.run(
        ["dbt", "run", "--select", "tag:adsb"], 
        cwd="dbt/",  
        capture_output=True,  
        text=True 
    )
    if dbt_run_result.returncode != 0:
        raise HTTPException(status_code=500, detail={
            "stdout": dbt_run_result.stdout,
            "stderr": dbt_run_result.stderr,
            "exit_code": dbt_run_result.returncode
        })
    
    return {
        "stdout": dbt_run_result.stdout,
        "stderr": dbt_run_result.stderr,
        "exit_code": dbt_run_result.returncode
    }

@app.post("/test-dbt")
def test_dbt():
    dbt_test_result = subprocess.run(
            ["dbt", "test", "--select", "tag:adsb"],  
            cwd="dbt/", 
            capture_output=True,  
            text=True
        )
    
    if dbt_test_result.returncode != 0:
        raise HTTPException(status_code=500, detail={                
            "stdout": dbt_test_result.stdout,
            "stderr": dbt_test_result.stderr,
            "exit_code": dbt_test_result.returncode
        })
    return {
        "stdout": dbt_test_result.stdout,
        "stderr": dbt_test_result.stderr,
        "exit_code": dbt_test_result.returncode
    }

@app.post("/upload-bronze")
def upload_bronze():
    LOCAL_BRONZE_DATA = "data/delta/bronze/live_states"
    CLOUD_BRONZE_LOCATION = "dbfs:/Volumes/workspace/default/adsb_data/bronze/live_states"

    upload_result = subprocess.run(
            ["databricks", "fs","cp", "-r", LOCAL_BRONZE_DATA, CLOUD_BRONZE_LOCATION,"--overwrite"],   
            capture_output=True,  
            text=True 
        )
    
    if upload_result.returncode != 0:
        raise HTTPException(status_code=500,detail={                
            "stdout": upload_result.stdout,
            "stderr": upload_result.stderr,
            "exit_code": upload_result.returncode
        })
    return {"status" : "ok"}
 


@app.post("/restart-stream")
def restart_stream():
    STREAM_SCRIPT = "spark/jobs/01_stream_ingest.py"
    
    # find and kill existing process
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            if any(STREAM_SCRIPT in arg for arg in proc.info['cmdline'] or []):
                proc.terminate()
                proc.wait(timeout=10)
                log.info(f"Killed stream process {proc.info['pid']}")
        except (psutil.NoSuchProcess, psutil.TimeoutExpired) as e:
            log.warning(f"Could not kill process: {e}")
    
    # restart in background
    subprocess.Popen(
        ["python", "-m", "spark.jobs.01_stream_ingest"],
        cwd="D:/ADS-B Flight Telemetry Pipeline/adsb-pipeline",
        creationflags=subprocess.DETACHED_PROCESS
    )
    
    return {"status": "restarted"}