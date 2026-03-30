output "airflow_url" {
  description = "The URL to access the Airflow Web UI"
  value       = "http://localhost:${docker_container.airflow_webserver.ports[0].external}"
}

output "managed_containers" {
  description = "The list of Docker containers created for this Airflow stack"
  value = [
    docker_container.postgres.name,
    docker_container.airflow_init.name,
    docker_container.airflow_webserver.name,
    docker_container.airflow_scheduler.name
  ]
}