provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

data "databricks_node_type" "smallest" {
  local_disk = true
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_cluster" "cluster" {
  cluster_name            = "single_cluster"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 30 #constraints are 60 make sure it is set to 60 upon delivery
  spark_conf              = { "spark.databricks.delta.preview.enabled" = "true"
                              "spark.master"                            = "local[*]"
                              "spark.databricks.cluster.profile"        = "singleNode" 
  }

  custom_tags = {
  "ResourceClass" = "SingleNode"
  }
  num_workers = 0

  library {
      pypi {
        package = "delta-spark"
      }
    }

  library {
      pypi {
        package = "requests"
      }
    }
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

resource "databricks_job" "spark_jobs"{
    for_each = local.spark_jobs

    name = each.key
    task {
       task_key = "${each.key}task"
       existing_cluster_id = databricks_cluster.cluster.id

        spark_python_task {
            python_file = each.value
        }
    }

}