terraform {
  required_providers {
    docker = {
      source = "kreuzwerker/docker"
      version = "~> 3.9.0"
    }

    databricks = {
        source = "databricks/databricks"
        version = "~> 1.50.0"
    }
  }
}

provider "docker" {
    # host = "unix:///var/run/docker.sock"
    host = "npipe:////.//pipe//docker_engine"
}

resource "docker_network" "airflow_network" {
  name = "airflow_network"
}

resource "docker_volume" "postgres_data" {
  name = "postgres_data"
}

# resource "docker_volume" "airflow_dags" {
#   name = "airflow_dags"
# }

resource "docker_volume" "airflow_logs" {
  name = "airflow_logs"
}

resource "docker_container" "postgres" {
    name  = "adsb_postgres"
    image = "postgres:15"

    networks_advanced {
    name = docker_network.airflow_network.name
    }

    env = [
    "POSTGRES_USER=${var.postgres_username}",
    "POSTGRES_PASSWORD=${var.postgres_password}",
    "POSTGRES_DB=${var.postgres_db_name}"                           
    ]

    healthcheck {  
        test     = ["CMD-SHELL", "pg_isready -U ${var.postgres_username} -d ${var.postgres_db_name} || exit 1"]
        interval = "10s"
        timeout  = "5s"
        retries  = 5
    }

    volumes {
      volume_name = docker_volume.postgres_data.name
      container_path = "/var/lib/postgresql/data"
    }
}

#airflow init
resource "docker_container" "airflow_init"{
    name  = "airflow_init"
    image = "apache/airflow:2.9.0" 

    command = ["bash", "-c", "pip install deltalake==0.17.4 apache-airflow-providers-databricks==6.2.0 && airflow db migrate && airflow users create --username ${var.airflow_admin_username} --password ${var.airflow_admin_password} --firstname Admin --lastname User --role Admin --email admin@adbs.com"]
    restart = "no"
    networks_advanced {
      name = docker_network.airflow_network.name
    }
    volumes {
      host_path      = "D:/ADS-B Flight Telemetry Pipeline/adsb-pipeline/airflow/dags"
      container_path = "/opt/airflow/dags"
    }
    env = [
      "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${var.postgres_username}:${var.postgres_password}@adsb_postgres:5432/${var.postgres_db_name}"
    ]
    depends_on = [
        docker_container.postgres
    ]
}

resource "docker_container" "airflow_webserver" {
    name = "airflow_webserver"
    image = "apache/airflow:2.9.0" 

    command = ["webserver"]
    restart = "always"
    ports {
      internal = 8080
      external = 8080
    }
    networks_advanced {
      name = docker_network.airflow_network.name
    }
    volumes {
      # volume_name = docker_volume.airflow_dags.name
      host_path  = "D:/ADS-B Flight Telemetry Pipeline/adsb-pipeline/airflow/dags"
      container_path = "/opt/airflow/dags"
    }
    volumes {
      volume_name = docker_volume.airflow_logs.name
      container_path = "/opt/airflow/logs"
    }

    env = [
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${var.postgres_username}:${var.postgres_password}@adsb_postgres:5432/${var.postgres_db_name}",
        "AIRFLOW__CORE__FERNET_KEY=${var.fernet_key}",
        "AIRFLOW_CONN_DATABRICKS_DEFAULT=databricks://:${var.databricks_token}@${var.databricks_airflow_host}",
        "AIRFLOW_CONN_DATABRICKS_SQL_DEFAULT=databricks://:${var.databricks_token}@${var.databricks_airflow_host}?http_path=${var.databricks_sql_http_path}",
        "_PIP_ADDITIONAL_REQUIREMENTS=deltalake==0.17.4 apache-airflow-providers-databricks==6.2.0",
        # "_AIRFLOW_WWW_USER_USERNAME=${var.airflow_admin_username}",
        # "_AIRFLOW_WWW_USER_PASSWORD=${var.airflow_admin_password}",
        # "_AIRFLOW_WWW_USER_CREATE=true",
        # "AIRFLOW__CORE__LOAD_EXAMPLES=False"
    ]

    depends_on = [ docker_container.airflow_init ]

}

resource "docker_container" "airflow_scheduler" {
    name = "scheduler"
    image = "apache/airflow:2.9.0" 
    restart = "always"

    command = ["scheduler"]

    networks_advanced {
      name = docker_network.airflow_network.name
    }
    volumes {
      # volume_name = docker_volume.airflow_dags.name
      host_path   = "D:/ADS-B Flight Telemetry Pipeline/adsb-pipeline/airflow/dags"
      container_path = "/opt/airflow/dags"
    }
    volumes {
      volume_name = docker_volume.airflow_logs.name
      container_path = "/opt/airflow/logs"
    }
    env = [
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${var.postgres_username}:${var.postgres_password}@adsb_postgres:5432/${var.postgres_db_name}",
        "AIRFLOW__CORE__FERNET_KEY=${var.fernet_key}",
        "AIRFLOW_CONN_DATABRICKS_DEFAULT=databricks://:${var.databricks_token}@${var.databricks_airflow_host}",
        "AIRFLOW_CONN_DATABRICKS_SQL_DEFAULT=databricks://:${var.databricks_token}@${var.databricks_airflow_host}?http_path=${var.databricks_sql_http_path}",        
        "_PIP_ADDITIONAL_REQUIREMENTS=deltalake==0.17.4 apache-airflow-providers-databricks==6.2.0",
        # "_AIRFLOW_WWW_USER_USERNAME=${var.airflow_admin_username}",
        # "_AIRFLOW_WWW_USER_PASSWORD=${var.airflow_admin_password}",
        # "_AIRFLOW_WWW_USER_CREATE=true",
        # "AIRFLOW__CORE__LOAD_EXAMPLES=False"
    ]
    depends_on = [ docker_container.airflow_init ]
}