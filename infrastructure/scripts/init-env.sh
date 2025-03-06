#!/bin/bash
# -----------------------------------------------------------------------------
# Environment initialization script for the Electricity Market Price Forecasting System
#
# This script creates the necessary .env files with appropriate environment
# variables based on the target environment (development, staging, production).
# It ensures all required configuration is in place before deployment.
#
# Usage:
#   ./init-env.sh [--environment <env>]
#
# Options:
#   --environment   Target environment (development, staging, production)
#                   Default: development
#   --help          Display this help message
#
# Exit Codes:
#   0 - Initialization completed successfully
#   1 - Directory check failed
#   2 - Template check failed
#   3 - Environment file generation failed
# -----------------------------------------------------------------------------

# Get script directory (works with symlinks)
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_ROOT=$(realpath "${SCRIPT_DIR}/../..")

# Set directory paths
DOCKER_DIR="${PROJECT_ROOT}/infrastructure/docker"
BACKEND_DIR="${PROJECT_ROOT}/src/backend"
WEB_DIR="${PROJECT_ROOT}/src/web"

# Set file paths
DOCKER_ENV_TEMPLATE="${DOCKER_DIR}/.env.template"
DOCKER_ENV_FILE="${DOCKER_DIR}/.env"
BACKEND_ENV_EXAMPLE="${BACKEND_DIR}/.env.example"
BACKEND_ENV_FILE="${BACKEND_DIR}/.env"
WEB_ENV_EXAMPLE="${WEB_DIR}/.env.example"
WEB_ENV_FILE="${WEB_DIR}/.env"

# Default values
DEFAULT_ENVIRONMENT="development"

# Function to display usage information
print_usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo
    echo "Initialize environment configuration for the Electricity Market Price Forecasting System."
    echo
    echo "Options:"
    echo "  --environment <env>    Target environment (development, staging, production)"
    echo "                         Default: ${DEFAULT_ENVIRONMENT}"
    echo "  --help                 Display this help message"
    echo
    echo "Example:"
    echo "  $(basename "$0") --environment production"
}

# Function to log messages with timestamp and level
log_message() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${timestamp}] [${level}] ${message}"
}

# Function to check if required directories exist
check_directories() {
    local status=0

    if [ ! -d "${DOCKER_DIR}" ]; then
        log_message "ERROR" "Docker directory not found: ${DOCKER_DIR}"
        status=1
    fi

    if [ ! -d "${BACKEND_DIR}" ]; then
        log_message "ERROR" "Backend directory not found: ${BACKEND_DIR}"
        status=1
    fi

    if [ ! -d "${WEB_DIR}" ]; then
        log_message "ERROR" "Web directory not found: ${WEB_DIR}"
        status=1
    fi

    if [ "${status}" -eq 0 ]; then
        log_message "INFO" "All required directories exist"
    fi

    return "${status}"
}

# Function to check if template files exist
check_templates() {
    local status=0

    if [ ! -f "${DOCKER_ENV_TEMPLATE}" ]; then
        log_message "ERROR" "Docker environment template not found: ${DOCKER_ENV_TEMPLATE}"
        status=1
    fi

    if [ ! -f "${BACKEND_ENV_EXAMPLE}" ]; then
        log_message "ERROR" "Backend environment example not found: ${BACKEND_ENV_EXAMPLE}"
        status=1
    fi

    if [ ! -f "${WEB_ENV_EXAMPLE}" ]; then
        log_message "ERROR" "Web environment example not found: ${WEB_ENV_EXAMPLE}"
        status=1
    fi

    if [ "${status}" -eq 0 ]; then
        log_message "INFO" "All required template files exist"
    fi

    return "${status}"
}

# Function to generate a secure random key
generate_secure_key() {
    openssl rand -base64 32
}

# Function to update an environment variable in a .env file
update_env_var() {
    local file_path="$1"
    local var_name="$2"
    local var_value="$3"
    
    # Check if file exists
    if [ ! -f "${file_path}" ]; then
        log_message "ERROR" "File not found: ${file_path}"
        return 1
    fi
    
    # Check if variable exists in file
    if grep -q "^${var_name}=" "${file_path}"; then
        # Replace existing variable
        sed -i "s|^${var_name}=.*|${var_name}=${var_value}|" "${file_path}"
    else
        # Append variable to file
        echo "${var_name}=${var_value}" >> "${file_path}"
    fi
    
    return 0
}

# Function to generate Docker environment file
generate_docker_env() {
    local environment="$1"
    local status=0
    
    log_message "INFO" "Generating Docker environment file for ${environment}"
    
    # Copy template file
    cp "${DOCKER_ENV_TEMPLATE}" "${DOCKER_ENV_FILE}"
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Failed to copy Docker environment template"
        return 1
    fi
    
    # Update environment variable
    update_env_var "${DOCKER_ENV_FILE}" "ENVIRONMENT" "${environment}"
    
    # Set DEBUG based on environment
    if [ "${environment}" = "development" ]; then
        update_env_var "${DOCKER_ENV_FILE}" "DEBUG" "True"
        update_env_var "${DOCKER_ENV_FILE}" "LOG_LEVEL" "DEBUG"
    elif [ "${environment}" = "staging" ]; then
        update_env_var "${DOCKER_ENV_FILE}" "DEBUG" "False"
        update_env_var "${DOCKER_ENV_FILE}" "LOG_LEVEL" "INFO"
    else  # production
        update_env_var "${DOCKER_ENV_FILE}" "DEBUG" "False"
        update_env_var "${DOCKER_ENV_FILE}" "LOG_LEVEL" "INFO"
    fi
    
    # Generate secure key for production
    if [ "${environment}" = "production" ]; then
        local secure_key
        secure_key=$(generate_secure_key)
        update_env_var "${DOCKER_ENV_FILE}" "SECRET_KEY" "${secure_key}"
    fi
    
    # Set API endpoints based on environment
    if [ "${environment}" = "development" ]; then
        update_env_var "${DOCKER_ENV_FILE}" "API_BASE_URL" "http://localhost:8000/api"
    elif [ "${environment}" = "staging" ]; then
        update_env_var "${DOCKER_ENV_FILE}" "API_BASE_URL" "http://staging.example.com/api"
    else  # production
        update_env_var "${DOCKER_ENV_FILE}" "API_BASE_URL" "http://forecast.example.com/api"
    fi
    
    log_message "INFO" "Docker environment file generated successfully"
    return "${status}"
}

# Function to generate Backend environment file
generate_backend_env() {
    local environment="$1"
    local status=0
    
    log_message "INFO" "Generating Backend environment file for ${environment}"
    
    # Copy example file
    cp "${BACKEND_ENV_EXAMPLE}" "${BACKEND_ENV_FILE}"
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Failed to copy Backend environment example"
        return 1
    fi
    
    # Update environment variable
    update_env_var "${BACKEND_ENV_FILE}" "ENVIRONMENT" "${environment}"
    
    # Set DEBUG based on environment
    if [ "${environment}" = "development" ]; then
        update_env_var "${BACKEND_ENV_FILE}" "DEBUG" "True"
        update_env_var "${BACKEND_ENV_FILE}" "LOG_LEVEL" "DEBUG"
    elif [ "${environment}" = "staging" ]; then
        update_env_var "${BACKEND_ENV_FILE}" "DEBUG" "False"
        update_env_var "${BACKEND_ENV_FILE}" "LOG_LEVEL" "INFO"
    else  # production
        update_env_var "${BACKEND_ENV_FILE}" "DEBUG" "False"
        update_env_var "${BACKEND_ENV_FILE}" "LOG_LEVEL" "INFO"
    fi
    
    # Set API endpoints based on environment
    if [ "${environment}" = "development" ]; then
        # Use example URLs for development
        # These are already in the example file
        :
    elif [ "${environment}" = "staging" ]; then
        update_env_var "${BACKEND_ENV_FILE}" "LOAD_FORECAST_URL" "http://staging-data.example.com/api/load-forecast"
        update_env_var "${BACKEND_ENV_FILE}" "HISTORICAL_PRICES_URL" "http://staging-data.example.com/api/historical-prices"
        update_env_var "${BACKEND_ENV_FILE}" "GENERATION_FORECAST_URL" "http://staging-data.example.com/api/generation-forecast"
    else  # production
        update_env_var "${BACKEND_ENV_FILE}" "LOAD_FORECAST_URL" "http://data.example.com/api/load-forecast"
        update_env_var "${BACKEND_ENV_FILE}" "HISTORICAL_PRICES_URL" "http://data.example.com/api/historical-prices"
        update_env_var "${BACKEND_ENV_FILE}" "GENERATION_FORECAST_URL" "http://data.example.com/api/generation-forecast"
    fi
    
    # Set storage path based on environment
    if [ "${environment}" = "development" ]; then
        update_env_var "${BACKEND_ENV_FILE}" "STORAGE_PATH" "data/forecasts"
    elif [ "${environment}" = "staging" ]; then
        update_env_var "${BACKEND_ENV_FILE}" "STORAGE_PATH" "/var/data/staging/forecasts"
    else  # production
        update_env_var "${BACKEND_ENV_FILE}" "STORAGE_PATH" "/var/data/production/forecasts"
    fi
    
    log_message "INFO" "Backend environment file generated successfully"
    return "${status}"
}

# Function to generate Web environment file
generate_web_env() {
    local environment="$1"
    local status=0
    
    log_message "INFO" "Generating Web environment file for ${environment}"
    
    # Copy example file
    cp "${WEB_ENV_EXAMPLE}" "${WEB_ENV_FILE}"
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Failed to copy Web environment example"
        return 1
    fi
    
    # Update environment variable
    update_env_var "${WEB_ENV_FILE}" "ENVIRONMENT" "${environment}"
    
    # Set DEBUG based on environment
    if [ "${environment}" = "development" ]; then
        update_env_var "${WEB_ENV_FILE}" "DEBUG" "True"
        update_env_var "${WEB_ENV_FILE}" "LOG_LEVEL" "DEBUG"
    elif [ "${environment}" = "staging" ]; then
        update_env_var "${WEB_ENV_FILE}" "DEBUG" "False"
        update_env_var "${WEB_ENV_FILE}" "LOG_LEVEL" "INFO"
    else  # production
        update_env_var "${WEB_ENV_FILE}" "DEBUG" "False"
        update_env_var "${WEB_ENV_FILE}" "LOG_LEVEL" "INFO"
    fi
    
    # Generate secure key for non-development environments
    if [ "${environment}" != "development" ]; then
        local secure_key
        secure_key=$(generate_secure_key)
        update_env_var "${WEB_ENV_FILE}" "SECRET_KEY" "${secure_key}"
    fi
    
    # Set API base URL based on environment
    if [ "${environment}" = "development" ]; then
        update_env_var "${WEB_ENV_FILE}" "API_BASE_URL" "http://localhost:8000/api"
    elif [ "${environment}" = "staging" ]; then
        update_env_var "${WEB_ENV_FILE}" "API_BASE_URL" "http://staging.example.com/api"
    else  # production
        update_env_var "${WEB_ENV_FILE}" "API_BASE_URL" "http://forecast.example.com/api"
    fi
    
    # Set cache settings based on environment
    if [ "${environment}" = "development" ]; then
        update_env_var "${WEB_ENV_FILE}" "CACHE_ENABLED" "False"
    else
        update_env_var "${WEB_ENV_FILE}" "CACHE_ENABLED" "True"
        if [ "${environment}" = "production" ]; then
            update_env_var "${WEB_ENV_FILE}" "CACHE_TIMEOUT" "3600"  # 1 hour for production
        fi
    fi
    
    log_message "INFO" "Web environment file generated successfully"
    return "${status}"
}

# Main function to orchestrate environment initialization
main() {
    local environment="$1"
    local exit_code=0
    
    log_message "INFO" "Starting environment initialization for ${environment}"
    
    # Check directories
    check_directories
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Directory check failed"
        return 1
    fi
    
    # Check templates
    check_templates
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Template check failed"
        return 2
    fi
    
    # Generate Docker environment file
    generate_docker_env "${environment}"
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Docker environment file generation failed"
        exit_code=3
    fi
    
    # Generate Backend environment file
    generate_backend_env "${environment}"
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Backend environment file generation failed"
        exit_code=3
    fi
    
    # Generate Web environment file
    generate_web_env "${environment}"
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Web environment file generation failed"
        exit_code=3
    fi
    
    if [ "${exit_code}" -eq 0 ]; then
        log_message "INFO" "Environment initialization completed successfully"
    else
        log_message "ERROR" "Environment initialization completed with errors"
    fi
    
    return "${exit_code}"
}

# Parse command line arguments
ENVIRONMENT="${DEFAULT_ENVIRONMENT}"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --environment)
            if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
                ENVIRONMENT="$2"
                shift 2
            else
                log_message "ERROR" "Missing value for --environment"
                print_usage
                exit 1
            fi
            ;;
        --help)
            print_usage
            exit 0
            ;;
        *)
            log_message "ERROR" "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Validate environment value
if [ "${ENVIRONMENT}" != "development" ] && [ "${ENVIRONMENT}" != "staging" ] && [ "${ENVIRONMENT}" != "production" ]; then
    log_message "ERROR" "Invalid environment: ${ENVIRONMENT}"
    log_message "ERROR" "Valid options are: development, staging, production"
    exit 1
fi

# Execute main function
main "${ENVIRONMENT}"
exit $?