#postgress vars
variable "postgres_username" {
    description = "The username for the Postgres database."
    type        = string
    default     = "postgres"
}
variable "postgres_password" {
    description = "The password for the Postgres database."
    type        = string
    sensitive   = true
}

variable "postgres_db_name" {
    description = "The name of the Postgres database."
    type        = string
    default     = "airflow"
}
#airflow vars  
variable "airflow_admin_username" {
    description = "The username for the Airflow admin user."
    type        = string
    default     = "admin"
}

variable "airflow_admin_password" {
    description = "The password for the Airflow admin user."
    type        = string
    sensitive   = true
}
#fernet key for Airflow
variable "fernet_key" {
    description = "The Fernet key for Airflow."
    type        = string
    sensitive   = true
}

#databricks

variable databricks_token  {
    description = "access token for databricks with cluster, jobs and secrets scopes"
    type = string
    sensitive = true
}

variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL (e.g., https://adb-1234567890.12.azuredatabricks.net)"
}

#opensky

variable "opensky_user"{
    type = string
    description = "opensky username"
    sensitive = true
}

variable "opensky_pass"{
    type = string
    description = "opensky password"
    sensitive = true
}