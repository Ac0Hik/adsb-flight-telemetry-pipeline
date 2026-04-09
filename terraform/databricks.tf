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
    "silver_flights"   = "${var.databricks_workspace_user_path}silver_flights"
    "anomaly_detect"   = "${var.databricks_workspace_user_path}anomaly_detect"
    "gold_aggregates"  = "${var.databricks_workspace_user_path}gold_aggregate"
  }
}

resource "databricks_job" "spark_jobs" {
  for_each = local.spark_jobs

  name = each.key

  task {
    task_key = "${each.key}_task"

    notebook_task {
      notebook_path = each.value
    }

  
  }
}