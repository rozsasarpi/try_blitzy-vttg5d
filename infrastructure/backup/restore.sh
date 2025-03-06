#!/bin/bash
#
# Restoration script for the Electricity Market Price Forecasting System
# This script provides functionality to restore forecast dataframes, logs, and
# configuration files from previously created backups, supporting disaster recovery procedures.
#
# Usage: ./restore.sh [--backup-dir <path>] [--skip-restart] [--list-only]
#
# Exit codes:
#   0 - Restoration completed successfully
#   1 - Prerequisites check failed
#   2 - Backup validation failed
#   3 - Docker volume restoration failed
#   4 - Configuration restoration failed
#   5 - Service restart failed
#   6 - Restoration report creation failed

# Global variables
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_ROOT=$(realpath "${SCRIPT_DIR}/../..")
BACKUP_ROOT="/backup"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TEMP_DIR="/tmp/forecast_restore_${TIMESTAMP}"
RESTORE_LOG="${BACKUP_ROOT}/restore_log.txt"

# Function to log messages to console and log file
log_message() {
    local message="$1"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[${timestamp}] ${message}"
    echo "[${timestamp}] ${message}" >> "${RESTORE_LOG}"
}

# Function to check if all prerequisites are met
check_prerequisites() {
    log_message "Checking prerequisites..."
    
    # Check if docker command is available
    if ! command -v docker &> /dev/null; then
        log_message "ERROR: Docker is not installed or not in PATH"
        return 1
    fi
    
    # Check if backup directory exists and is readable
    if [ ! -d "${BACKUP_ROOT}" ]; then
        log_message "ERROR: Backup directory ${BACKUP_ROOT} does not exist"
        return 1
    fi
    
    if [ ! -r "${BACKUP_ROOT}" ]; then
        log_message "ERROR: Backup directory ${BACKUP_ROOT} is not readable"
        return 1
    fi
    
    # Check if docker is running
    if ! docker info &> /dev/null; then
        log_message "ERROR: Docker daemon is not running"
        return 1
    fi
    
    # Check if forecast volumes exist
    if ! docker volume ls | grep -q forecast-data; then
        log_message "ERROR: forecast-data volume does not exist"
        return 1
    fi
    
    if ! docker volume ls | grep -q forecast-logs; then
        log_message "ERROR: forecast-logs volume does not exist"
        return 1
    fi
    
    log_message "All prerequisites met"
    return 0
}

# Function to list all available backups
list_available_backups() {
    log_message "Listing available backups..."
    
    # Find backup directories
    local backups=()
    while IFS= read -r backup_dir; do
        if [ -d "$backup_dir" ] && [ -f "$backup_dir/backup_info.json" ]; then
            backups+=("$backup_dir")
        fi
    done < <(find "${BACKUP_ROOT}" -maxdepth 1 -type d -name "backup_*" | sort -r)
    
    # Display backups
    if [ ${#backups[@]} -eq 0 ]; then
        log_message "No backups found in ${BACKUP_ROOT}"
        return 1
    fi
    
    echo "Available backups:"
    for i in "${!backups[@]}"; do
        local backup_name=$(basename "${backups[$i]}")
        local backup_date=$(echo "$backup_name" | sed 's/backup_//')
        local formatted_date=$(date -d "${backup_date:0:8} ${backup_date:9:2}:${backup_date:11:2}:${backup_date:13:2}" "+%Y-%m-%d %H:%M:%S" 2>/dev/null)
        
        if [ -z "$formatted_date" ]; then
            formatted_date="Unknown date"
        fi
        
        echo "[$((i+1))] ${backup_name} - ${formatted_date}"
        
        # Show contents summary
        if [ -f "${backups[$i]}/backup_info.json" ]; then
            echo "    Contents: $(grep -o '"components":\[[^]]*\]' "${backups[$i]}/backup_info.json" | sed 's/"components":\[//;s/\]//;s/"//g;s/,/, /g')"
        else
            echo "    Contents: Information not available"
        fi
    done
    
    return 0
}

# Function to validate that a backup is complete and restorable
validate_backup() {
    local backup_dir="$1"
    
    log_message "Validating backup at ${backup_dir}..."
    
    # Check if backup directory exists
    if [ ! -d "${backup_dir}" ]; then
        log_message "ERROR: Backup directory ${backup_dir} does not exist"
        return 1
    fi
    
    # Check if required files exist
    local required_files=("forecast-data.tar.gz" "forecast-logs.tar.gz" "configuration.tar.gz" "backup_info.json")
    for file in "${required_files[@]}"; do
        if [ ! -f "${backup_dir}/${file}" ]; then
            log_message "ERROR: Required file ${file} is missing from backup"
            return 1
        fi
    done
    
    # Check integrity of archive files
    for archive in "forecast-data.tar.gz" "forecast-logs.tar.gz" "configuration.tar.gz"; do
        if ! gzip -t "${backup_dir}/${archive}" &> /dev/null; then
            log_message "ERROR: Archive ${archive} is corrupted"
            return 1
        fi
    done
    
    # Verify backup info file is valid JSON
    if ! jq . "${backup_dir}/backup_info.json" &> /dev/null; then
        log_message "ERROR: backup_info.json is not valid JSON"
        return 1
    fi
    
    log_message "Backup validation successful"
    return 0
}

# Function to restore Docker volumes from backup archives
restore_docker_volumes() {
    local backup_dir="$1"
    
    log_message "Restoring Docker volumes from ${backup_dir}..."
    
    # Create temporary directory for extraction
    mkdir -p "${TEMP_DIR}"
    if [ ! -d "${TEMP_DIR}" ]; then
        log_message "ERROR: Failed to create temporary directory ${TEMP_DIR}"
        return 1
    fi
    
    # Extract forecast-data archive
    log_message "Extracting forecast data..."
    mkdir -p "${TEMP_DIR}/forecast-data"
    if ! tar -xzf "${backup_dir}/forecast-data.tar.gz" -C "${TEMP_DIR}/forecast-data"; then
        log_message "ERROR: Failed to extract forecast-data archive"
        rm -rf "${TEMP_DIR}"
        return 1
    fi
    
    # Extract forecast-logs archive
    log_message "Extracting forecast logs..."
    mkdir -p "${TEMP_DIR}/forecast-logs"
    if ! tar -xzf "${backup_dir}/forecast-logs.tar.gz" -C "${TEMP_DIR}/forecast-logs"; then
        log_message "ERROR: Failed to extract forecast-logs archive"
        rm -rf "${TEMP_DIR}"
        return 1
    fi
    
    # Extract cache-data archive if it exists
    if [ -f "${backup_dir}/cache-data.tar.gz" ]; then
        log_message "Extracting cache data..."
        mkdir -p "${TEMP_DIR}/cache-data"
        if ! tar -xzf "${backup_dir}/cache-data.tar.gz" -C "${TEMP_DIR}/cache-data"; then
            log_message "WARNING: Failed to extract cache-data archive, continuing without cache restoration"
        fi
    fi
    
    # Find running container that uses the volumes
    local forecast_container=$(docker ps --filter "volume=forecast-data" -q | head -1)
    if [ -z "$forecast_container" ]; then
        # Find any container that might work
        forecast_container=$(docker ps -q | head -1)
        if [ -z "$forecast_container" ]; then
            log_message "ERROR: No running containers found to use for volume restoration"
            rm -rf "${TEMP_DIR}"
            return 1
        fi
        log_message "WARNING: No container found using forecast volumes, using container ${forecast_container} instead"
    fi
    
    # Copy data to Docker volumes
    log_message "Copying data to forecast-data volume..."
    if ! docker cp "${TEMP_DIR}/forecast-data/." "${forecast_container}:/forecast-data/"; then
        log_message "ERROR: Failed to copy data to forecast-data volume"
        rm -rf "${TEMP_DIR}"
        return 1
    fi
    
    log_message "Copying logs to forecast-logs volume..."
    if ! docker cp "${TEMP_DIR}/forecast-logs/." "${forecast_container}:/forecast-logs/"; then
        log_message "ERROR: Failed to copy logs to forecast-logs volume"
        rm -rf "${TEMP_DIR}"
        return 1
    fi
    
    # Copy cache data if extracted
    if [ -d "${TEMP_DIR}/cache-data" ]; then
        log_message "Copying data to cache-data volume..."
        if ! docker cp "${TEMP_DIR}/cache-data/." "${forecast_container}:/cache-data/"; then
            log_message "WARNING: Failed to copy data to cache-data volume, continuing without cache restoration"
        fi
    fi
    
    # Verify data was copied successfully
    if ! docker exec "$forecast_container" ls -la /forecast-data/ &> /dev/null; then
        log_message "ERROR: Failed to verify data restoration for forecast-data volume"
        rm -rf "${TEMP_DIR}"
        return 1
    fi
    
    if ! docker exec "$forecast_container" ls -la /forecast-logs/ &> /dev/null; then
        log_message "ERROR: Failed to verify data restoration for forecast-logs volume"
        rm -rf "${TEMP_DIR}"
        return 1
    fi
    
    # Clean up temporary directory
    rm -rf "${TEMP_DIR}"
    
    log_message "Docker volumes restored successfully"
    return 0
}

# Function to restore configuration files from backup
restore_configuration() {
    local backup_dir="$1"
    
    log_message "Restoring configuration files from ${backup_dir}..."
    
    # Create temporary directory for extraction
    mkdir -p "${TEMP_DIR}"
    if [ ! -d "${TEMP_DIR}" ]; then
        log_message "ERROR: Failed to create temporary directory ${TEMP_DIR}"
        return 1
    fi
    
    # Extract configuration archive
    log_message "Extracting configuration files..."
    if ! tar -xzf "${backup_dir}/configuration.tar.gz" -C "${TEMP_DIR}"; then
        log_message "ERROR: Failed to extract configuration archive"
        rm -rf "${TEMP_DIR}"
        return 1
    fi
    
    # Copy .env files
    if [ -f "${TEMP_DIR}/.env" ]; then
        log_message "Restoring .env file..."
        cp "${TEMP_DIR}/.env" "${PROJECT_ROOT}/"
    fi
    
    # Copy docker-compose files if they exist
    if [ -f "${TEMP_DIR}/docker-compose.yml" ]; then
        log_message "Restoring docker-compose.yml..."
        cp "${TEMP_DIR}/docker-compose.yml" "${PROJECT_ROOT}/"
    fi
    
    if [ -f "${TEMP_DIR}/docker-compose.override.yml" ]; then
        log_message "Restoring docker-compose.override.yml..."
        cp "${TEMP_DIR}/docker-compose.override.yml" "${PROJECT_ROOT}/"
    fi
    
    # Copy nginx configuration if it exists
    if [ -d "${TEMP_DIR}/nginx" ]; then
        log_message "Restoring nginx configuration..."
        mkdir -p "${PROJECT_ROOT}/nginx"
        cp -r "${TEMP_DIR}/nginx/." "${PROJECT_ROOT}/nginx/"
    fi
    
    # Verify configuration files were copied successfully
    if [ -f "${TEMP_DIR}/.env" ] && [ ! -f "${PROJECT_ROOT}/.env" ]; then
        log_message "ERROR: Failed to verify .env file restoration"
        rm -rf "${TEMP_DIR}"
        return 1
    fi
    
    # Clean up temporary directory
    rm -rf "${TEMP_DIR}"
    
    log_message "Configuration files restored successfully"
    return 0
}

# Function to restart Docker services
restart_services() {
    log_message "Restarting Docker services..."
    
    # Navigate to Docker Compose directory
    cd "${PROJECT_ROOT}" || {
        log_message "ERROR: Failed to navigate to ${PROJECT_ROOT}"
        return 1
    }
    
    # Stop running containers
    log_message "Stopping running containers..."
    if ! docker-compose down; then
        log_message "ERROR: Failed to stop running containers"
        return 1
    fi
    
    # Start containers
    log_message "Starting containers..."
    if ! docker-compose up -d; then
        log_message "ERROR: Failed to start containers"
        return 1
    fi
    
    # Verify services started successfully
    sleep 5
    if [ "$(docker-compose ps -q | wc -l)" -eq 0 ]; then
        log_message "ERROR: No services started"
        return 1
    fi
    
    log_message "Services restarted successfully"
    return 0
}

# Function to create a report of the restoration process
create_restore_report() {
    local backup_dir="$1"
    local success="$2"
    
    log_message "Creating restoration report..."
    
    # Prepare report data
    local status="success"
    if [ "$success" != "true" ]; then
        status="failed"
    fi
    
    local report_file="${BACKUP_ROOT}/restore_report_${TIMESTAMP}.json"
    
    # Create JSON report
    cat > "${report_file}" <<EOF
{
    "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "backup_used": "$(basename "${backup_dir}")",
    "status": "${status}",
    "details": {
        "forecast_data_restored": true,
        "forecast_logs_restored": true,
        "configuration_restored": true,
        "services_restarted": ${success}
    }
}
EOF
    
    if [ ! -f "${report_file}" ]; then
        log_message "ERROR: Failed to create restoration report"
        return 1
    fi
    
    # Display summary to console
    log_message "Restoration report created: ${report_file}"
    log_message "Restoration status: ${status}"
    
    return 0
}

# Function to parse command-line arguments
parse_arguments() {
    local backup_dir=""
    local skip_restart=false
    local list_only=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --backup-dir)
                backup_dir="$2"
                shift 2
                ;;
            --skip-restart)
                skip_restart=true
                shift
                ;;
            --list-only)
                list_only=true
                shift
                ;;
            --help)
                echo "Usage: ./restore.sh [--backup-dir <path>] [--skip-restart] [--list-only]"
                echo ""
                echo "Options:"
                echo "  --backup-dir <path>  Specific backup directory to restore from"
                echo "  --skip-restart       Skip restarting services after restoration"
                echo "  --list-only          Only list available backups without restoring"
                echo "  --help               Display this help message"
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Return parsed arguments as a JSON-like string
    echo "{\"backup_dir\":\"${backup_dir}\",\"skip_restart\":${skip_restart},\"list_only\":${list_only}}"
}

# Main function that orchestrates the restoration process
main() {
    local backup_dir="$1"
    local skip_restart="$2"
    
    log_message "Starting restoration process..."
    
    # Check prerequisites
    if ! check_prerequisites; then
        log_message "ERROR: Prerequisites check failed"
        return 1
    fi
    
    # If no specific backup directory provided, list available backups and prompt for selection
    if [ -z "${backup_dir}" ]; then
        if ! list_available_backups; then
            log_message "ERROR: No backups available for restoration"
            return 1
        fi
        
        echo ""
        read -p "Enter backup number to restore or 'q' to quit: " selection
        
        if [[ "${selection}" == "q" ]]; then
            log_message "Restoration cancelled by user"
            return 0
        fi
        
        # Find selected backup
        local backups=()
        while IFS= read -r backup; do
            if [ -d "$backup" ] && [ -f "$backup/backup_info.json" ]; then
                backups+=("$backup")
            fi
        done < <(find "${BACKUP_ROOT}" -maxdepth 1 -type d -name "backup_*" | sort -r)
        
        if [[ ! "${selection}" =~ ^[0-9]+$ ]] || [ "${selection}" -lt 1 ] || [ "${selection}" -gt "${#backups[@]}" ]; then
            log_message "ERROR: Invalid selection"
            return 1
        fi
        
        backup_dir="${backups[$((selection-1))]}"
    fi
    
    # Validate selected backup
    if ! validate_backup "${backup_dir}"; then
        log_message "ERROR: Backup validation failed"
        return 2
    fi
    
    # Restore Docker volumes from backup
    if ! restore_docker_volumes "${backup_dir}"; then
        log_message "ERROR: Docker volume restoration failed"
        return 3
    fi
    
    # Restore configuration files from backup
    if ! restore_configuration "${backup_dir}"; then
        log_message "ERROR: Configuration restoration failed"
        return 4
    fi
    
    # Restart services unless skip_restart is specified
    local restart_success=true
    if [ "${skip_restart}" != "true" ]; then
        if ! restart_services; then
            log_message "ERROR: Service restart failed"
            restart_success=false
            return 5
        fi
    else
        log_message "Skipping service restart as requested"
    fi
    
    # Create restoration report
    if ! create_restore_report "${backup_dir}" "${restart_success}"; then
        log_message "ERROR: Restoration report creation failed"
        return 6
    fi
    
    log_message "Restoration process completed successfully"
    return 0
}

# Parse command-line arguments
args=$(parse_arguments "$@")
backup_dir=$(echo "${args}" | grep -o '"backup_dir":"[^"]*"' | cut -d'"' -f4)
skip_restart=$(echo "${args}" | grep -o '"skip_restart":\(true\|false\)' | cut -d':' -f2)
list_only=$(echo "${args}" | grep -o '"list_only":\(true\|false\)' | cut -d':' -f2)

# Create log directory if it doesn't exist
mkdir -p "$(dirname "${RESTORE_LOG}")"

# Handle list-only mode
if [ "${list_only}" = "true" ]; then
    list_available_backups
    exit $?
fi

# Run main function
main "${backup_dir}" "${skip_restart}"
exit $?