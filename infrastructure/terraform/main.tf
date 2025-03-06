# Main Terraform configuration file for provisioning the infrastructure required by the Electricity Market Price Forecasting System.
# This file defines all resources, providers, and configurations needed to set up the complete environment for running the forecasting system, including compute resources, storage, networking, and monitoring components.

terraform {
  required_version = ">= 1.0.0"
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.2"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.1"
    }
    template = {
      source  = "hashicorp/template"
      version = "~> 2.2"
    }
  }
}

locals {
  timestamp           = timestamp()
  data_path           = var.environment == "production" ? "/data" : "/data/${var.environment}"
  backup_path         = var.environment == "production" ? "/backup" : "/backup/${var.environment}"
  log_path            = var.environment == "production" ? "/logs" : "/logs/${var.environment}"
  docker_compose_path = "${path.module}/../docker/docker-compose.full.yml"
  env_template_path   = "${path.module}/../docker/.env.template"
  env_file_path       = "${path.module}/../docker/.env"
  nginx_conf_path     = "${path.module}/../nginx/nginx.conf"
  deploy_script_path  = "${path.module}/../scripts/deploy.sh"
}

# Creates the environment file for Docker services from template
resource "local_file" "env_file" {
  description = "Creates the environment file for Docker services from template"
  content = templatefile(local.env_template_path, {
    ENVIRONMENT              = var.environment
    DEBUG                    = var.environment != "production" ? "True" : "False"
    LOG_LEVEL                = var.environment == "production" ? "INFO" : "DEBUG"
    FORECAST_HORIZON_HOURS   = var.forecast_horizon_hours
    PROBABILISTIC_SAMPLE_COUNT = var.probabilistic_sample_count
    FORECAST_SCHEDULE_HOUR   = 7
    FORECAST_SCHEDULE_MINUTE = 0
    API_PORT                 = var.api_port
    SERVER_PORT              = var.visualization_port
    NGINX_PORT               = var.nginx_port
    NGINX_SERVER_NAME        = var.nginx_server_name
    PROMETHEUS_RETENTION_TIME = "${var.monitoring_retention_days}d"
    FALLBACK_ENABLED         = var.fallback_enabled ? "True" : "False"
  })
  filename        = local.env_file_path
  file_permission = "0644"
}

# Creates the Nginx configuration file for reverse proxy
resource "local_file" "nginx_conf" {
  description = "Creates the Nginx configuration file for reverse proxy"
  content = templatefile(local.nginx_conf_path, {
    NGINX_SERVER_NAME     = var.nginx_server_name
    NGINX_CLIENT_MAX_BODY_SIZE = "10M"
    VISUALIZATION_PORT    = var.visualization_port
    API_PORT              = var.api_port
    PROMETHEUS_PORT       = var.prometheus_port
    GRAFANA_PORT          = var.grafana_port
  })
  filename        = "${path.module}/../nginx/nginx.conf.generated"
  file_permission = "0644"
}

# Creates necessary data directories for the forecasting system
resource "null_resource" "data_directories" {
  description = "Creates necessary data directories for the forecasting system"
  triggers = {
    always_run = local.timestamp
  }

  provisioner "local-exec" {
    command = "mkdir -p ${local.data_path}/forecasts ${local.data_path}/logs ${local.backup_path}"
  }
}

# Deploys the Docker services using the deploy script
resource "null_resource" "deploy_services" {
  description = "Deploys the Docker services using the deploy script"
  depends_on = [
    local_file.env_file,
    local_file.nginx_conf,
    null_resource.data_directories
  ]

  triggers = {
    env_file_hash     = filemd5(local.env_file_path)
    nginx_conf_hash   = filemd5("${path.module}/../nginx/nginx.conf.generated")
    docker_compose_hash = filemd5(local.docker_compose_path)
  }

  provisioner "local-exec" {
    command = "${local.deploy_script_path} --environment ${var.environment} --action deploy"
  }
}

# Sets up the backup schedule for forecast data
resource "null_resource" "backup_setup" {
  description = "Sets up the backup schedule for forecast data"
  depends_on = [
    null_resource.deploy_services
  ]

  triggers = {
    backup_retention_days = var.backup_retention_days
  }

  provisioner "local-exec" {
    command = "echo '0 1 * * * ${path.module}/../backup/backup.sh ${local.data_path}/forecasts ${local.backup_path} ${var.backup_retention_days}' | crontab -"
  }
}

# Sets up monitoring if enabled
resource "null_resource" "monitoring_setup" {
  description = "Sets up monitoring if enabled"
  count       = var.enable_monitoring ? 1 : 0
  depends_on = [
    null_resource.deploy_services
  ]

  triggers = {
    monitoring_enabled     = var.enable_monitoring
    monitoring_retention_days = var.monitoring_retention_days
  }

  provisioner "local-exec" {
    command = "docker-compose -f ${local.docker_compose_path} up -d prometheus grafana node-exporter"
  }
}

# Sets up alerting if enabled
resource "null_resource" "alerting_setup" {
  description = "Sets up alerting if enabled"
  count       = var.enable_alerting ? 1 : 0
  depends_on = [
    null_resource.monitoring_setup
  ]

  triggers = {
    alerting_enabled = var.enable_alerting
    alert_email      = var.alert_email
  }

  provisioner "local-exec" {
    command = "echo 'Setting up alerting to ${var.alert_email}'"
  }
}

# Outputs
output "server_ip" {
  description = "IP address of the server hosting the forecasting system"
  value       = "127.0.0.1"
}

output "dashboard_url" {
  description = "URL for accessing the forecast visualization dashboard"
  value       = var.nginx_enabled ? "http://${var.nginx_server_name}/" : "http://127.0.0.1:${var.visualization_port}"
}

output "api_url" {
  description = "URL for accessing the forecast API"
  value       = var.nginx_enabled ? "http://${var.nginx_server_name}/api" : "http://127.0.0.1:${var.api_port}"
}

output "monitoring_url" {
  description = "URL for accessing the monitoring dashboard"
  value       = var.enable_monitoring ? (var.nginx_enabled ? "http://${var.nginx_server_name}/monitoring" : "http://127.0.0.1:${var.grafana_port}") : "Monitoring not enabled"
}

output "forecast_data_path" {
  description = "Path to the forecast data storage location"
  value       = "\"${local.data_path}/forecasts\""
}

output "log_path" {
  description = "Path to the application logs"
  value       = "\"${local.data_path}/logs\""
}

output "backup_path" {
  description = "Path to the backup storage location"
  value       = "\"${local.backup_path}\""
}

output "environment" {
  description = "Deployed environment name (development, staging, production)"
  value       = var.environment
}

output "deployment_timestamp" {
  description = "Timestamp when the infrastructure was deployed"
  value       = local.timestamp
}