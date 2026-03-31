provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}


resource "databricks_secret_scope" "opensky"{
    name = "opensky"
}

resource "databricks_secret" "opensky_user"{
    key          = "username"
    string_value = var.opensky_user
    scope        = databricks_secret_scope.opensky.name
}

resource "databricks_secret" "opensky_pass"{
    key          = "password"
    string_value = var.opensky_pass
    scope        = databricks_secret_scope.opensky.name
}

#jobs creation using loop 
locals {
  spark_jobs = {
    "stream_ingest"    = "dbfs:/adsb-pipeline/spark/jobs/01_stream_ingest.py"
    "silver_flights"   = "dbfs:/adsb-pipeline/spark/jobs/03_silver_flights.py"
    "anomaly_detect"   = "dbfs:/adsb-pipeline/spark/jobs/04_anomaly_detect.py"
    "gold_aggregates"  = "dbfs:/adsb-pipeline/spark/jobs/05_gold_aggregates.py"
  }
}

resource "databricks_job" "spark_jobs" {
  for_each = local.spark_jobs

  name = each.key

  environment {
    environment_key = "default"
    spec {
      client = "2"
    }
  }

  task {
    task_key = "${each.key}_task"

    spark_python_task {
      python_file = each.value
    }

    environment_key = "default"
  }
}