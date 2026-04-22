#!/bin/bash
# ADS-B Flight Telemetry Pipeline — Quick Start
# Requires: Git Bash (Windows) or bash (Linux/macOS), Python 3.10, Terraform, Docker Desktop

set -e  # exit on any error

echo "✈️  ADS-B Flight Telemetry Pipeline — Setup"
echo "--------------------------------------------"

# 1. Check dependencies
echo "Checking dependencies..."
command -v python >/dev/null 2>&1 || { echo "❌ Python not found. Install Python 3.10+"; exit 1; }
command -v terraform >/dev/null 2>&1 || { echo "❌ Terraform not found. Install Terraform"; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "❌ Docker not found. Install Docker Desktop"; exit 1; }
command -v databricks >/dev/null 2>&1 || { echo "❌ Databricks CLI not found. Install databricks CLI v0.295+"; exit 1; }
echo "✅ All dependencies found"

# 2. Check credentials
if [ ! -f "terraform/terraform.tfvars" ]; then
    echo ""
    echo "❌ terraform/terraform.tfvars not found."
    echo "   Copy terraform/terraform.tfvars.example and fill in your credentials:"
    echo "   cp terraform/terraform.tfvars.example terraform/terraform.tfvars"
    exit 1
fi
echo "✅ Credentials found"

# 3. Install Python dependencies
echo ""
echo "Installing Python dependencies..."
pip install -r requirements.txt -q
echo "✅ Python dependencies installed"

# 4. Install dbt dependencies
echo ""
echo "Installing dbt dependencies..."
cd dbt && dbt deps -q && cd ..
echo "✅ dbt dependencies installed"

# 5. Spin up infrastructure
echo ""
if [ -f "terraform/terraform.tfstate" ]; then
    echo "⚠️  Terraform state found — infrastructure may already be provisioned."
    echo "   Run 'terraform plan' to see what changes would be applied."
    echo "   Continuing with apply — only changes will be made..."
fi
echo "Spinning up infrastructure (Terraform)..."
cd terraform && terraform init -input=false -no-color && terraform apply -auto-approve -no-color && cd ..
echo "✅ Infrastructure ready"

# 6. Start the local REST API in background
echo ""
echo "Starting local REST API on port 8000..."
uvicorn api.main:app --port 8000 &
API_PID=$!
echo "✅ REST API running (PID $API_PID)"

# 7. Start the streaming job in background
echo ""
echo "Starting ADS-B streaming ingestion..."
python -m spark.jobs.01_stream_ingest &
STREAM_PID=$!
echo "✅ Streaming job running (PID $STREAM_PID)"

# 8. Done
echo ""
echo "--------------------------------------------"
echo "✈️  Pipeline is running!"
echo ""
echo "  Airflow UI:     http://localhost:8080"
echo "  REST API docs:  http://localhost:8000/docs"
echo ""
echo "⚠️  Manual step required:"
echo "  Add FastAPI connection in Airflow UI:"
echo "  Admin → Connections → New"
echo "  Conn ID: fastapi_default | Type: HTTP | Host: http://host.docker.internal | Port: 8000"
echo ""
echo "  To stop: kill $API_PID $STREAM_PID && cd terraform && terraform destroy"
echo "--------------------------------------------"