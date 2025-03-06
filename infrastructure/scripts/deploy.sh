#!/bin/bash
# -----------------------------------------------------------------------------
# Deployment script for the Electricity Market Price Forecasting System
#
# This script automates the deployment process, including environment setup,
# Docker container building, and service orchestration. It provides a
# consistent and reliable way to deploy the entire system with proper
# error handling and logging.
#
# Usage:
#   ./deploy.sh [--environment <env>] [--action <action>]
#
# Options:
#   --environment   Target environment (development, staging, production)
#                   Default: production
#   --action        Action to perform (deploy, stop, restart, status, cleanup)
#                   Default: deploy
#   --help          Display usage information
#
# Exit Codes:
#   0 - Deployment completed successfully
#   1 - Prerequisites check failed
#   2 - Environment setup failed
#   3 - Docker image build failed
#   4 - Service startup failed
#   5 - Service health check failed
# -----------------------------------------------------------------------------

# Ensure that the script exits immediately if a command exits with a non-zero status
set -e

# Define global variables
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_ROOT=$(realpath "${SCRIPT_DIR}/../..")
DOCKER_DIR="${PROJECT_ROOT}/infrastructure/docker"
DOCKER_COMPOSE_FILE="${DOCKER_DIR}/docker-compose.full.yml"
ENV_FILE="${DOCKER_DIR}/.env"
LOG_DIR="${PROJECT_ROOT}/logs"
DEPLOY_LOG="${LOG_DIR}/deploy_$(date +"%Y%m%d_%H%M%S").log"
DEFAULT_ENVIRONMENT="production"
DEFAULT_ACTION="deploy"

# Source the environment initialization script
source "${SCRIPT_DIR}/init-env.sh" # version: N/A

# Function to display usage information
print_usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo
    echo "Deploys the Electricity Market Price Forecasting System."
    echo
    echo "Options:"
    echo "  --environment <env>    Target environment (development, staging, production)"
    echo "                         Default: ${DEFAULT_ENVIRONMENT}"
    echo "  --action <action>       Action to perform (deploy, stop, restart, status, cleanup)"
    echo "                         Default: ${DEFAULT_ACTION}"
    echo "  --help                 Display this help message"
    echo
    echo "Example:"
    echo "  $(basename "$0") --environment staging --action deploy"
}

# Function to log messages with timestamp and level
log_message() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${timestamp}] [${level}] ${message}"
    echo "[${timestamp}] [${level}] ${message}" >> "${DEPLOY_LOG}"
}

# Function to check if all required tools and files are available
check_prerequisites() {
    local status=0

    # Check if docker command is available
    if ! command -v docker &> /dev/null; then # version: 20.10+
        log_message "ERROR" "docker command not found. Please install Docker."
        status=1
    fi

    # Check if docker-compose command is available
    if ! command -v docker-compose &> /dev/null; then # version: 1.29+
        log_message "ERROR" "docker-compose command not found. Please install Docker Compose."
        status=1
    fi

    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        log_message "ERROR" "Docker daemon is not running. Please start Docker."
        status=1
    fi

    # Verify that Docker Compose file exists
    if [ ! -f "${DOCKER_COMPOSE_FILE}" ]; then
        log_message "ERROR" "Docker Compose file not found: ${DOCKER_COMPOSE_FILE}"
        status=1
    fi

    if [ "${status}" -eq 0 ]; then
        log_message "INFO" "All prerequisites met."
    fi

    return "${status}"
}

# Function to set up the environment for deployment
setup_environment() {
    local environment="$1"
    local status=0

    # Check if environment initialization script exists
    if [ ! -x "${SCRIPT_DIR}/init-env.sh" ]; then
        log_message "ERROR" "Environment initialization script not found: ${SCRIPT_DIR}/init-env.sh"
        status=1
        return "${status}"
    fi

    # Execute environment initialization script with specified environment
    log_message "INFO" "Initializing environment for ${environment}"
    "${SCRIPT_DIR}/init-env.sh" --environment "${environment}"
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Environment initialization failed."
        status=1
        return "${status}"
    fi

    # Verify that environment file was created
    if [ ! -f "${ENV_FILE}" ]; then
        log_message "ERROR" "Environment file not created: ${ENV_FILE}"
        status=1
        return "${status}"
    fi

    log_message "INFO" "Environment setup completed successfully."
    return "${status}"
}

# Function to build Docker images for all services
build_images() {
    log_message "INFO" "Building Docker images..."
    docker-compose -f "${DOCKER_COMPOSE_FILE}" build
    local build_status=$?
    if [ "${build_status}" -ne 0 ]; then
        log_message "ERROR" "Docker image build failed."
    else
        log_message "INFO" "Docker images built successfully."
    fi
    return "${build_status}"
}

# Function to start all services defined in Docker Compose file
start_services() {
    log_message "INFO" "Starting services..."
    docker-compose -f "${DOCKER_COMPOSE_FILE}" up -d
    local up_status=$?
    if [ "${up_status}" -ne 0 ]; then
        log_message "ERROR" "Service startup failed."
    else
        log_message "INFO" "Services started successfully."
    fi
    return "${up_status}"
}

# Function to stop all running services
stop_services() {
    log_message "INFO" "Stopping services..."
    docker-compose -f "${DOCKER_COMPOSE_FILE}" down
    local down_status=$?
    if [ "${down_status}" -ne 0 ]; then
        log_message "ERROR" "Service stopping failed."
    else
        log_message "INFO" "Services stopped successfully."
    fi
    return "${down_status}"
}

# Function to restart all services
restart_services() {
    log_message "INFO" "Restarting services..."
    stop_services
    local stop_status=$?
    if [ "${stop_status}" -ne 0 ]; then
        log_message "ERROR" "Service stopping failed during restart."
        return "${stop_status}"
    fi
    start_services
    local start_status=$?
    if [ "${start_status}" -ne 0 ]; then
        log_message "ERROR" "Service starting failed during restart."
        return "${start_status}"
    fi
    log_message "INFO" "Services restarted successfully."
    return 0
}

# Function to check the health status of deployed services
check_service_health() {
    log_message "INFO" "Checking service health..."
    docker-compose -f "${DOCKER_COMPOSE_FILE}" ps
    local ps_status=$?
    if [ "${ps_status}" -ne 0 ]; then
        log_message "ERROR" "Failed to get service status."
        return "${ps_status}"
    fi

    # Verify that forecasting-service is running
    if ! docker ps | grep -q "forecasting-service"; then
        log_message "ERROR" "forecasting-service is not running."
        return 5
    fi

    # Verify that visualization-service is running
    if ! docker ps | grep -q "visualization-service"; then
        log_message "ERROR" "visualization-service is not running."
        return 5
    fi

    log_message "INFO" "All services are running."
    return 0
}

# Function to display information about running services
display_service_info() {
    log_message "INFO" "Displaying service information..."

    # Get service status information
    docker-compose -f "${DOCKER_COMPOSE_FILE}" ps

    # Display URLs for accessing services
    log_message "INFO" "Access the visualization dashboard at: http://localhost:8050"
    log_message "INFO" "Access the API endpoint at: http://localhost:8000/api"

    # Show monitoring dashboard URL if available
    if command -v docker &> /dev/null && docker ps | grep -q "prometheus"; then
        log_message "INFO" "Access the monitoring dashboard at: http://localhost:3000"
    fi
}

# Function to clean up unused Docker resources
cleanup_resources() {
    log_message "INFO" "Cleaning up unused Docker resources..."

    # Remove unused Docker images
    docker image prune -f
    local image_prune_status=$?
    if [ "${image_prune_status}" -ne 0 ]; then
        log_message "WARNING" "Failed to remove unused Docker images."
    fi

    # Remove unused Docker volumes
    docker volume prune -f
    local volume_prune_status=$?
    if [ "${volume_prune_status}" -ne 0 ]; then
        log_message "WARNING" "Failed to remove unused Docker volumes."
    fi

    # Remove unused Docker networks
    docker network prune -f
    local network_prune_status=$?
    if [ "${network_prune_status}" -ne 0 ]; then
        log_message "WARNING" "Failed to remove unused Docker networks."
    fi

    log_message "INFO" "Docker resource cleanup completed."
}

# Function to parse command-line arguments for the script
parse_arguments() {
    # Define available arguments (environment, action)
    local ENVIRONMENT=""
    local ACTION=""

    # Parse command-line arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --environment)
                ENVIRONMENT="$2"
                shift 2
                ;;
            --action)
                ACTION="$2"
                shift 2
                ;;
            --help)
                print_usage
                exit 0
                ;;
            *)
                log_message "ERROR" "Unknown argument: $1"
                print_usage
                exit 1
                ;;
        esac
    done

    # Set default values for missing arguments
    if [ -z "${ENVIRONMENT}" ]; then
        ENVIRONMENT="${DEFAULT_ENVIRONMENT}"
    fi
    if [ -z "${ACTION}" ]; then
        ACTION="${DEFAULT_ACTION}"
    fi

    # Validate argument values
    if [[ ! "${ENVIRONMENT}" =~ ^(development|staging|production)$ ]]; then
        log_message "ERROR" "Invalid environment: ${ENVIRONMENT}. Must be development, staging, or production."
        exit 1
    fi
    if [[ ! "${ACTION}" =~ ^(deploy|stop|restart|status|cleanup)$ ]]; then
        log_message "ERROR" "Invalid action: ${ACTION}. Must be deploy, stop, restart, status, or cleanup."
        exit 1
    fi

    # Return parsed arguments as dictionary
    echo "{ \"environment\": \"${ENVIRONMENT}\", \"action\": \"${ACTION}\" }"
}

# Main function that orchestrates the deployment process
main() {
    local environment="$1"
    local action="$2"

    # Create log directory if it doesn't exist
    mkdir -p "${LOG_DIR}"

    # Log deployment start with environment and action information
    log_message "INFO" "Starting deployment with environment: ${environment}, action: ${action}"

    # Check prerequisites
    check_prerequisites
    local prereq_status=$?
    if [ "${prereq_status}" -ne 0 ]; then
        log_message "ERROR" "Prerequisites check failed."
        exit 1
    fi

    # Perform action based on argument (deploy, stop, restart, status, cleanup)
    case "${action}" in
        deploy)
            # Setup environment
            setup_environment "${environment}"
            local env_status=$?
            if [ "${env_status}" -ne 0 ]; then
                log_message "ERROR" "Environment setup failed."
                exit 2
            fi

            # Build images
            build_images
            local build_status=$?
            if [ "${build_status}" -ne 0 ]; then
                log_message "ERROR" "Docker image build failed."
                exit 3
            fi

            # Start services
            start_services
            local up_status=$?
            if [ "${up_status}" -ne 0 ]; then
                log_message "ERROR" "Service startup failed."
                exit 4
            fi

            # Check health
            check_service_health
            local health_status=$?
            if [ "${health_status}" -ne 0 ]; then
                log_message "ERROR" "Service health check failed."
                exit 5
            fi
            ;;
        stop)
            stop_services
            local down_status=$?
            if [ "${down_status}" -ne 0 ]; then
                log_message "ERROR" "Service stopping failed."
                exit 4
            fi
            ;;
        restart)
            restart_services
            local restart_status=$?
            if [ "${restart_status}" -ne 0 ]; then
                log_message "ERROR" "Service restart failed."
                exit 4
            fi

            # Check health
            check_service_health
            local health_status=$?
            if [ "${health_status}" -ne 0 ]; then
                log_message "ERROR" "Service health check failed after restart."
                exit 5
            fi
            ;;
        status)
            display_service_info
            ;;
        cleanup)
            stop_services
            cleanup_resources
            ;;
        *)
            log_message "ERROR" "Invalid action: ${action}. Must be deploy, stop, restart, status, or cleanup."
            print_usage
            exit 1
            ;;
    esac

    # Log deployment completion
    log_message "INFO" "Deployment completed successfully."
    exit 0
}

# Parse command line arguments
if [[ $# -eq 0 ]]; then
    ARGS=$(parse_arguments)
else
    ARGS=$(parse_arguments "$@")
fi

# Extract environment and action from parsed arguments
environment=$(echo "$ARGS" | jq -r .environment)
action=$(echo "$ARGS" | jq -r .action)

# Execute main function
main "${environment}" "${action}"
exit $?