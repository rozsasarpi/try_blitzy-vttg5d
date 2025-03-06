#!/bin/bash
#
# backup.sh - Backup script for the Electricity Market Price Forecasting System
#
# This script creates backups of the forecast dataframes, logs, and configuration files.
# It supports disaster recovery procedures by maintaining daily backups with a 
# configurable retention period.
#
# Usage: ./backup.sh [--no-cleanup] [--retention-days N] [--help]
#
# Exit codes:
#   0 - Backup completed successfully
#   1 - Prerequisites check failed
#   2 - Docker volume backup failed
#   3 - Configuration backup failed
#   4 - Backup info creation failed
#   5 - Old backup cleanup failed

# Get the directory where the script is located
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
# Get the project root directory (assuming script is in infrastructure/backup)
PROJECT_ROOT=$(realpath "${SCRIPT_DIR}/../..")

# Default variables
BACKUP_ROOT="/backup"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_DIR="${BACKUP_ROOT}/backup_${TIMESTAMP}"
BACKUP_LOG="${BACKUP_ROOT}/backup_log.txt"
TEMP_DIR="/tmp/forecast_backup_${TIMESTAMP}"
RETENTION_DAYS=90
DO_CLEANUP=true

# Process command-line arguments
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --no-cleanup)
            DO_CLEANUP=false
            ;;
        --retention-days)
            if [[ "$2" =~ ^[0-9]+$ ]]; then
                RETENTION_DAYS="$2"
                shift
            else
                echo "Error: --retention-days requires a numeric value"
                exit 1
            fi
            ;;
        --help)
            echo "Usage: ./backup.sh [OPTIONS]"
            echo ""
            echo "Creates a backup of the Electricity Market Price Forecasting System"
            echo ""
            echo "Options:"
            echo "  --no-cleanup           Skip cleanup of old backups"
            echo "  --retention-days N     Number of days to retain backups (default: 90)"
            echo "  --help                 Display this help message"
            echo ""
            echo "Environment variables:"
            echo "  BACKUP_ROOT            Root directory for backups (default: /backup)"
            echo "  RETENTION_DAYS         Number of days to retain backups (default: 90)"
            exit 0
            ;;
        *)
            echo "Unknown parameter: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
    shift
done

# Override with environment variables if set
if [[ -n "${BACKUP_ROOT_ENV}" ]]; then
    BACKUP_ROOT="${BACKUP_ROOT_ENV}"
    BACKUP_DIR="${BACKUP_ROOT}/backup_${TIMESTAMP}"
    BACKUP_LOG="${BACKUP_ROOT}/backup_log.txt"
fi

if [[ -n "${RETENTION_DAYS_ENV}" ]]; then
    RETENTION_DAYS="${RETENTION_DAYS_ENV}"
fi

# Function to log messages to both console and log file
log_message() {
    local message="$1"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${timestamp}] ${message}"
    echo "[${timestamp}] ${message}" >> "${BACKUP_LOG}"
}

# Function to check if all prerequisites are met
check_prerequisites() {
    # Check if docker command is available
    if ! command -v docker &> /dev/null; then
        log_message "ERROR: docker command not found. Please install Docker."
        return 1
    fi

    # Check if backup directory exists or can be created
    if [[ ! -d "${BACKUP_ROOT}" ]]; then
        log_message "Backup directory ${BACKUP_ROOT} does not exist. Attempting to create it..."
        mkdir -p "${BACKUP_ROOT}" || {
            log_message "ERROR: Failed to create backup directory ${BACKUP_ROOT}"
            return 1
        }
    fi

    # Check if backup directory is writable
    if [[ ! -w "${BACKUP_ROOT}" ]]; then
        log_message "ERROR: Backup directory ${BACKUP_ROOT} is not writable."
        return 1
    fi

    # Check if docker is running
    docker info &> /dev/null || {
        log_message "ERROR: Docker daemon is not running."
        return 1
    }

    # Verify that forecast volumes exist
    docker volume inspect forecast-data &> /dev/null || {
        log_message "ERROR: Docker volume 'forecast-data' does not exist."
        return 1
    }
    
    docker volume inspect forecast-logs &> /dev/null || {
        log_message "WARNING: Docker volume 'forecast-logs' does not exist."
        # This is a warning, not an error, as logs might be stored elsewhere
    }

    return 0
}

# Function to backup Docker volumes containing forecast data
backup_docker_volumes() {
    log_message "Starting backup of Docker volumes..."
    
    # Create temporary directory for volume data
    mkdir -p "${TEMP_DIR}" || {
        log_message "ERROR: Failed to create temporary directory ${TEMP_DIR}"
        return 2
    }
    
    # Create a temporary container to access volumes
    local temp_container=$(docker run -d \
        -v forecast-data:/forecast-data \
        -v forecast-logs:/forecast-logs \
        --name forecast-backup-helper \
        alpine:latest \
        sleep 300)
    
    if [[ -z "${temp_container}" ]]; then
        log_message "ERROR: Failed to create temporary container for volume access."
        return 2
    fi
    
    # Backup forecast-data volume
    log_message "Backing up forecast-data volume..."
    docker cp "${temp_container}:/forecast-data" "${TEMP_DIR}/" || {
        log_message "ERROR: Failed to copy forecast-data volume."
        docker rm -f "${temp_container}" &> /dev/null
        return 2
    }
    
    # Backup forecast-logs volume if it exists
    log_message "Backing up forecast-logs volume..."
    docker cp "${temp_container}:/forecast-logs" "${TEMP_DIR}/" || {
        log_message "WARNING: Failed to copy forecast-logs volume. Continuing..."
    }
    
    # Also try to backup cache-data volume if it exists
    docker volume inspect cache-data &> /dev/null && {
        log_message "Backing up cache-data volume..."
        docker cp "${temp_container}:/cache-data" "${TEMP_DIR}/" || {
            log_message "WARNING: Failed to copy cache-data volume. Continuing..."
        }
    }
    
    # Remove the temporary container
    docker rm -f "${temp_container}" &> /dev/null
    
    # Create tar archives of each volume's data
    log_message "Creating archive of volume data..."
    (cd "${TEMP_DIR}" && tar -cf forecast-data.tar forecast-data) || {
        log_message "ERROR: Failed to create archive of forecast-data."
        return 2
    }
    
    # Create archive of logs if directory exists
    if [[ -d "${TEMP_DIR}/forecast-logs" ]]; then
        (cd "${TEMP_DIR}" && tar -cf forecast-logs.tar forecast-logs) || {
            log_message "WARNING: Failed to create archive of forecast-logs. Continuing..."
        }
    fi
    
    # Create archive of cache if directory exists
    if [[ -d "${TEMP_DIR}/cache-data" ]]; then
        (cd "${TEMP_DIR}" && tar -cf cache-data.tar cache-data) || {
            log_message "WARNING: Failed to create archive of cache-data. Continuing..."
        }
    fi
    
    # Compress archives with gzip
    log_message "Compressing volume archives..."
    gzip -f "${TEMP_DIR}/forecast-data.tar" || {
        log_message "ERROR: Failed to compress forecast-data archive."
        return 2
    }
    
    if [[ -f "${TEMP_DIR}/forecast-logs.tar" ]]; then
        gzip -f "${TEMP_DIR}/forecast-logs.tar" || {
            log_message "WARNING: Failed to compress forecast-logs archive. Continuing..."
        }
    fi
    
    if [[ -f "${TEMP_DIR}/cache-data.tar" ]]; then
        gzip -f "${TEMP_DIR}/cache-data.tar" || {
            log_message "WARNING: Failed to compress cache-data archive. Continuing..."
        }
    fi
    
    # Move compressed archives to backup directory
    log_message "Moving archives to backup directory..."
    mkdir -p "${BACKUP_DIR}" || {
        log_message "ERROR: Failed to create backup directory ${BACKUP_DIR}"
        return 2
    }
    
    mv "${TEMP_DIR}/forecast-data.tar.gz" "${BACKUP_DIR}/" || {
        log_message "ERROR: Failed to move forecast-data archive to backup directory."
        return 2
    }
    
    if [[ -f "${TEMP_DIR}/forecast-logs.tar.gz" ]]; then
        mv "${TEMP_DIR}/forecast-logs.tar.gz" "${BACKUP_DIR}/" || {
            log_message "WARNING: Failed to move forecast-logs archive to backup directory. Continuing..."
        }
    fi
    
    if [[ -f "${TEMP_DIR}/cache-data.tar.gz" ]]; then
        mv "${TEMP_DIR}/cache-data.tar.gz" "${BACKUP_DIR}/" || {
            log_message "WARNING: Failed to move cache-data archive to backup directory. Continuing..."
        }
    fi
    
    # Clean up temporary directory
    rm -rf "${TEMP_DIR}"
    
    log_message "Docker volumes backup completed successfully."
    return 0
}

# Function to backup configuration files
backup_configuration() {
    log_message "Starting backup of configuration files..."
    
    # Create temporary directory for configuration files
    local config_temp_dir="${TEMP_DIR}_config"
    mkdir -p "${config_temp_dir}" || {
        log_message "ERROR: Failed to create temporary directory for configuration."
        return 3
    }
    
    # Copy environment files
    log_message "Copying environment files..."
    find "${PROJECT_ROOT}" -name ".env*" -type f -exec cp --parents {} "${config_temp_dir}/" \; || {
        log_message "WARNING: Some environment files could not be copied. Continuing..."
    }
    
    # Copy docker-compose files
    log_message "Copying docker-compose files..."
    find "${PROJECT_ROOT}" -name "docker-compose*.yml" -type f -exec cp --parents {} "${config_temp_dir}/" \; || {
        log_message "WARNING: Some docker-compose files could not be copied. Continuing..."
    }
    
    # Copy nginx configuration if it exists
    if [[ -d "${PROJECT_ROOT}/infrastructure/nginx" ]]; then
        log_message "Copying nginx configuration..."
        cp -r "${PROJECT_ROOT}/infrastructure/nginx" "${config_temp_dir}/infrastructure/" || {
            log_message "WARNING: Nginx configuration could not be copied. Continuing..."
        }
    fi
    
    # Create tar archive of configuration files
    log_message "Creating archive of configuration files..."
    (cd "${config_temp_dir}" && tar -cf configuration.tar .) || {
        log_message "ERROR: Failed to create archive of configuration files."
        rm -rf "${config_temp_dir}"
        return 3
    }
    
    # Compress archive with gzip
    log_message "Compressing configuration archive..."
    gzip -f "${config_temp_dir}/configuration.tar" || {
        log_message "ERROR: Failed to compress configuration archive."
        rm -rf "${config_temp_dir}"
        return 3
    }
    
    # Move compressed archive to backup directory
    log_message "Moving configuration archive to backup directory..."
    mv "${config_temp_dir}/configuration.tar.gz" "${BACKUP_DIR}/" || {
        log_message "ERROR: Failed to move configuration archive to backup directory."
        rm -rf "${config_temp_dir}"
        return 3
    }
    
    # Clean up temporary directory
    rm -rf "${config_temp_dir}"
    
    log_message "Configuration backup completed successfully."
    return 0
}

# Function to create a JSON file with backup metadata
create_backup_info() {
    log_message "Creating backup metadata file..."
    
    local info_file="${BACKUP_DIR}/backup_info.json"
    local hostname=$(hostname)
    local docker_version=$(docker --version)
    local os_info=$(uname -a)
    local backup_files=$(ls -la "${BACKUP_DIR}")
    local containers_info=$(docker ps -a --format "{{.Names}}: {{.Status}}")
    
    # Get sizes of backed up files
    local data_size=$(du -h "${BACKUP_DIR}/forecast-data.tar.gz" 2>/dev/null | cut -f1)
    local logs_size="N/A"
    if [[ -f "${BACKUP_DIR}/forecast-logs.tar.gz" ]]; then
        logs_size=$(du -h "${BACKUP_DIR}/forecast-logs.tar.gz" | cut -f1)
    fi
    local cache_size="N/A"
    if [[ -f "${BACKUP_DIR}/cache-data.tar.gz" ]]; then
        cache_size=$(du -h "${BACKUP_DIR}/cache-data.tar.gz" | cut -f1)
    fi
    local config_size=$(du -h "${BACKUP_DIR}/configuration.tar.gz" | cut -f1)
    
    # Create JSON file
    cat > "${info_file}" << EOF
{
    "backup_timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "backup_name": "backup_${TIMESTAMP}",
    "system_info": {
        "hostname": "${hostname}",
        "operating_system": "${os_info}",
        "docker_version": "${docker_version}"
    },
    "backup_contents": {
        "forecast_data": {
            "filename": "forecast-data.tar.gz",
            "size": "${data_size}"
        },
        "forecast_logs": {
            "filename": "forecast-logs.tar.gz",
            "size": "${logs_size}"
        },
        "cache_data": {
            "filename": "cache-data.tar.gz",
            "size": "${cache_size}"
        },
        "configuration": {
            "filename": "configuration.tar.gz",
            "size": "${config_size}"
        }
    },
    "container_status": ${containers_info//\"/\\\"}
}
EOF
    
    if [[ $? -ne 0 ]]; then
        log_message "ERROR: Failed to create backup info file."
        return 4
    fi
    
    log_message "Backup metadata file created successfully."
    return 0
}

# Function to remove backups older than the retention period
cleanup_old_backups() {
    if [[ "${DO_CLEANUP}" != "true" ]]; then
        log_message "Cleanup of old backups skipped due to --no-cleanup flag."
        return 0
    fi

    log_message "Cleaning up backups older than ${RETENTION_DAYS} days..."
    
    local count=0
    local old_backups=$(find "${BACKUP_ROOT}" -type d -name "backup_*" -mtime +${RETENTION_DAYS})
    
    for backup in ${old_backups}; do
        rm -rf "${backup}" && {
            log_message "Removed old backup: ${backup}"
            count=$((count + 1))
        } || {
            log_message "WARNING: Failed to remove old backup: ${backup}"
        }
    done
    
    log_message "Cleanup completed. Removed ${count} old backup(s)."
    return 0
}

# Main function that orchestrates the backup process
main() {
    log_message "==== Starting backup process for Electricity Market Price Forecasting System ===="
    log_message "Using backup directory: ${BACKUP_DIR}"
    
    # Check prerequisites
    log_message "Checking prerequisites..."
    check_prerequisites
    if [[ $? -ne 0 ]]; then
        log_message "Prerequisites check failed. Aborting."
        return 1
    fi
    
    # Create backup directory
    mkdir -p "${BACKUP_DIR}" || {
        log_message "ERROR: Failed to create backup directory ${BACKUP_DIR}"
        return 1
    }
    
    # Backup Docker volumes
    backup_docker_volumes
    if [[ $? -ne 0 ]]; then
        log_message "Docker volume backup failed. Aborting."
        return 2
    fi
    
    # Backup configuration files
    backup_configuration
    if [[ $? -ne 0 ]]; then
        log_message "Configuration backup failed. Aborting."
        return 3
    fi
    
    # Create backup info file
    create_backup_info
    if [[ $? -ne 0 ]]; then
        log_message "Backup info creation failed. Continuing..."
        # Not critical enough to abort
    fi
    
    # Cleanup old backups
    cleanup_old_backups
    if [[ $? -ne 0 ]]; then
        log_message "Old backup cleanup failed. Continuing..."
        # Not critical enough to abort
    fi
    
    log_message "Backup completed successfully: ${BACKUP_DIR}"
    log_message "==== Backup process completed ===="
    
    return 0
}

# Execute main function
main
exit $?