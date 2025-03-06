# Terraform variables for Electricity Market Price Forecasting System
# These variables configure the infrastructure components including compute resources,
# storage, networking, and application-specific settings.

# Environment Configuration
variable "environment" {
  type        = string
  description = "Deployment environment (development, staging, production)"
  default     = "development"

  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment must be one of: development, staging, production"
  }
}

variable "server_instance_type" {
  type        = string
  description = "Server instance type for compute resources"
  default     = "standard"

  validation {
    condition     = contains(["small", "standard", "large"], var.server_instance_type)
    error_message = "Server instance type must be one of: small, standard, large"
  }
}

# Storage Configuration
variable "storage_size" {
  type        = number
  description = "Storage size in GB for forecast data and logs"
  default     = 100

  validation {
    condition     = var.storage_size >= 50
    error_message = "Storage size must be at least 50 GB"
  }
}

variable "backup_retention_days" {
  type        = number
  description = "Number of days to retain backup data"
  default     = 90

  validation {
    condition     = var.backup_retention_days >= 30
    error_message = "Backup retention must be at least 30 days"
  }
}

variable "forecast_data_path" {
  type        = string
  description = "Path for storing forecast data"
  default     = "/data/forecasts"

  validation {
    condition     = startswith(var.forecast_data_path, "/")
    error_message = "Forecast data path must be an absolute path"
  }
}

variable "log_path" {
  type        = string
  description = "Path for storing application logs"
  default     = "/data/logs"

  validation {
    condition     = startswith(var.log_path, "/")
    error_message = "Log path must be an absolute path"
  }
}

# Monitoring Configuration
variable "enable_monitoring" {
  type        = bool
  description = "Flag to enable monitoring components (Prometheus, Grafana)"
  default     = true
}

variable "monitoring_retention_days" {
  type        = number
  description = "Number of days to retain monitoring data in Prometheus"
  default     = 15

  validation {
    condition     = var.monitoring_retention_days >= 7
    error_message = "Monitoring retention must be at least 7 days"
  }
}

variable "enable_alerting" {
  type        = bool
  description = "Flag to enable alerting system"
  default     = true
}

variable "alert_email" {
  type        = string
  description = "Email address for system alerts"
  default     = "admin@example.com"

  validation {
    condition     = can(regex("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", var.alert_email))
    error_message = "Alert email must be a valid email address"
  }
}

# Application Configuration
variable "scheduler_timezone" {
  type        = string
  description = "Timezone for the forecast scheduler (must be valid TZ database name)"
  default     = "America/Chicago"

  validation {
    condition     = can(regex("^[A-Za-z]+/[A-Za-z_]+$", var.scheduler_timezone))
    error_message = "Scheduler timezone must be a valid TZ database name (e.g., America/Chicago)"
  }
}

variable "forecast_horizon_hours" {
  type        = number
  description = "Forecast horizon in hours (must be 72 as per requirements)"
  default     = 72

  validation {
    condition     = var.forecast_horizon_hours == 72
    error_message = "Forecast horizon must be 72 hours as specified in requirements"
  }
}

variable "probabilistic_sample_count" {
  type        = number
  description = "Number of probabilistic samples in forecasts"
  default     = 100

  validation {
    condition     = var.probabilistic_sample_count >= 50 && var.probabilistic_sample_count <= 1000
    error_message = "Probabilistic sample count must be between 50 and 1000"
  }
}

variable "fallback_enabled" {
  type        = bool
  description = "Flag to enable fallback mechanism for forecast generation"
  default     = true
}

# Network Configuration
variable "visualization_port" {
  type        = number
  description = "Port for the visualization service (Dash)"
  default     = 8050

  validation {
    condition     = var.visualization_port >= 1024 && var.visualization_port <= 65535
    error_message = "Visualization port must be between 1024 and 65535"
  }
}

variable "api_port" {
  type        = number
  description = "Port for the API service"
  default     = 8000

  validation {
    condition     = var.api_port >= 1024 && var.api_port <= 65535
    error_message = "API port must be between 1024 and 65535"
  }
}

variable "grafana_port" {
  type        = number
  description = "Port for the Grafana monitoring dashboard"
  default     = 3000

  validation {
    condition     = var.grafana_port >= 1024 && var.grafana_port <= 65535
    error_message = "Grafana port must be between 1024 and 65535"
  }
}

variable "prometheus_port" {
  type        = number
  description = "Port for the Prometheus monitoring service"
  default     = 9090

  validation {
    condition     = var.prometheus_port >= 1024 && var.prometheus_port <= 65535
    error_message = "Prometheus port must be between 1024 and 65535"
  }
}

# Nginx Configuration
variable "nginx_enabled" {
  type        = bool
  description = "Flag to enable Nginx reverse proxy"
  default     = true
}

variable "nginx_server_name" {
  type        = string
  description = "Server name for Nginx configuration"
  default     = "forecast.example.com"

  validation {
    condition     = can(regex("^[a-zA-Z0-9.-]+$", var.nginx_server_name))
    error_message = "Nginx server name must be a valid hostname"
  }
}

variable "nginx_port" {
  type        = number
  description = "Port for Nginx server"
  default     = 80

  validation {
    condition     = var.nginx_port == 80 || var.nginx_port == 443 || (var.nginx_port >= 1024 && var.nginx_port <= 65535)
    error_message = "Nginx port must be 80, 443, or between 1024 and 65535"
  }
}