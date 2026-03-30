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
