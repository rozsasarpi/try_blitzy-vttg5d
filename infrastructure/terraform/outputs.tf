# Terraform outputs definition file for the Electricity Market Price Forecasting System infrastructure.
# This file defines all output values that are exposed after the infrastructure is provisioned, including access URLs, storage paths, and deployment information.

# Output the server IP address
output "server_ip" {
  description = "IP address of the server hosting the forecasting system"
  value       = "127.0.0.1"
  sensitive   = false
}

# Output the URL for accessing the forecast visualization dashboard
output "dashboard_url" {
  description = "URL for accessing the forecast visualization dashboard"
  value       = var.nginx_enabled ? "http://${var.nginx_server_name}/" : "http://127.0.0.1:${var.visualization_port}"
  sensitive   = false
}

# Output the URL for accessing the forecast API
output "api_url" {
  description = "URL for accessing the forecast API"
  value       = var.nginx_enabled ? "http://${var.nginx_server_name}/api" : "http://127.0.0.1:${var.api_port}"
  sensitive   = false
}

# Output the URL for accessing the monitoring dashboard
output "monitoring_url" {
  description = "URL for accessing the monitoring dashboard"
  value       = var.enable_monitoring ? (var.nginx_enabled ? "http://${var.nginx_server_name}/monitoring" : "http://127.0.0.1:${var.grafana_port}") : "Monitoring not enabled"
  sensitive   = false
}

# Output the path to the forecast data storage location
output "forecast_data_path" {
  description = "Path to the forecast data storage location"
  value       = "${local.data_path}/forecasts"
  sensitive   = false
}

# Output the path to the application logs
output "log_path" {
  description = "Path to the application logs"
  value       = "${local.data_path}/logs"
  sensitive   = false
}

# Output the path to the backup storage location
output "backup_path" {
  description = "Path to the backup storage location"
  value       = "${local.backup_path}"
  sensitive   = false
}

# Output the deployed environment name
output "environment" {
  description = "Deployed environment name (development, staging, production)"
  value       = var.environment
  sensitive   = false
}

# Output the timestamp when the infrastructure was deployed
output "deployment_timestamp" {
  description = "Timestamp when the infrastructure was deployed"
  value       = local.timestamp
  sensitive   = false
}