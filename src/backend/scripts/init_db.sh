#!/bin/bash

# init_db.sh
# Initialize the storage structure for the Electricity Market Price Forecasting System

# Get the directory where the script is located
SCRIPT_DIR=$(dirname "$0")
PROJECT_ROOT=$(realpath "$SCRIPT_DIR/../..")
PYTHON_EXECUTABLE=python3

# Print header
function print_header() {
    echo "=================================================="
    echo "  Initializing Electricity Market Price Forecasting Storage"
    echo "=================================================="
    echo
}

# Print success message
function print_success() {
    echo "[SUCCESS] $1"
}

# Print error message
function print_error() {
    echo "[ERROR] $1"
}

# Check if required Python dependencies are installed
function check_python_dependencies() {
    echo "Checking Python dependencies..."
    
    if ! $PYTHON_EXECUTABLE -c "import pandas, pandera" 2>/dev/null; then
        print_error "Required Python packages (pandas, pandera) are not installed."
        print_error "Please install them using: pip install pandas>=2.0.0 pandera>=0.16.0"
        return 1
    fi
    
    print_success "All required Python dependencies are installed."
    return 0
}

# Create the storage directories
function create_storage_directories() {
    echo "Creating storage directories..."
    
    # Execute Python code to create storage directories using imported modules
    $PYTHON_EXECUTABLE -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
from src.backend.config.settings import STORAGE_ROOT_DIR, STORAGE_LATEST_DIR
from pathlib import Path

def create_dirs():
    try:
        # Create root storage directory
        root_dir = Path(STORAGE_ROOT_DIR)
        root_dir.mkdir(parents=True, exist_ok=True)
        print(f'Storage root directory created: {root_dir}')
        
        # Create latest directory for symbolic links
        latest_dir = Path(STORAGE_LATEST_DIR)
        latest_dir.mkdir(parents=True, exist_ok=True)
        print(f'Latest forecasts directory created: {latest_dir}')
        
        return True
    except Exception as e:
        print(f'Error creating directories: {str(e)}')
        return False

success = create_dirs()
exit(0 if success else 1)
"
    
    if [ $? -eq 0 ]; then
        print_success "Storage directories created successfully."
        return 0
    else
        print_error "Failed to create storage directories."
        return 1
    fi
}

# Initialize the forecast index
function initialize_forecast_index() {
    echo "Initializing forecast index..."
    
    # Execute Python code to initialize the forecast index
    $PYTHON_EXECUTABLE -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
from src.backend.storage.storage_manager import initialize_storage

try:
    initialized = initialize_storage()
    if initialized:
        print('Forecast index initialized successfully.')
    else:
        print('Forecast index was already initialized.')
    exit(0)
except Exception as e:
    print(f'Error initializing forecast index: {str(e)}')
    exit(1)
"
    
    if [ $? -eq 0 ]; then
        print_success "Forecast index initialized successfully."
        return 0
    else
        print_error "Failed to initialize forecast index."
        return 1
    fi
}

# Rebuild the storage index
function rebuild_forecast_index() {
    echo "Rebuilding forecast index..."
    
    # Execute Python code to rebuild the forecast index
    $PYTHON_EXECUTABLE -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
from src.backend.storage.storage_manager import rebuild_storage_index

try:
    stats = rebuild_storage_index()
    print(f'Forecast index rebuilt successfully.')
    print(f'Processed {stats.get(\"files_processed\", 0)} files.')
    exit(0)
except Exception as e:
    print(f'Error rebuilding forecast index: {str(e)}')
    exit(1)
"
    
    if [ $? -eq 0 ]; then
        print_success "Forecast index rebuilt successfully."
        return 0
    else
        print_error "Failed to rebuild forecast index."
        return 1
    fi
}

# Display storage information
function display_storage_info() {
    echo "Storage system information:"
    echo "-------------------------"
    
    # Execute Python code to get and display storage information
    $PYTHON_EXECUTABLE -c "
import sys
import json
sys.path.insert(0, '$PROJECT_ROOT')
from src.backend.storage.storage_manager import get_storage_info

try:
    info = get_storage_info()
    
    # Format and display storage information
    print('Storage Paths:')
    print(f\"  Root Directory: {info['storage_paths']['root_dir']}\")
    print(f\"  Index File: {info['storage_paths']['index_file']}\")
    print('')
    
    print('Schema Information:')
    schema_info = info.get('schema_info', {})
    print(f\"  Schema Version: {schema_info.get('version', 'N/A')}\")
    print(f\"  Number of Columns: {len(schema_info.get('columns', {}))}\")
    print('')
    
    print('Storage Statistics:')
    stats = info.get('storage_stats', {})
    print(f\"  Total Forecasts: {stats.get('total_forecasts', 0)}\")
    print(f\"  Storage Space: {stats.get('storage_space_mb', 0)} MB\")
    print('')
    
    print('Index Statistics:')
    index_stats = info.get('index_stats', {})
    print(f\"  Total Entries: {index_stats.get('total_entries', 0)}\")
    print('')
    
    print('Products:')
    for product in info.get('products', []):
        print(f\"  - {product}\")
        
except Exception as e:
    print(f'Error getting storage information: {str(e)}')
    exit(1)
"
}

# Main function
function main() {
    print_header
    
    # Check Python dependencies
    check_python_dependencies
    if [ $? -ne 0 ]; then
        return 1
    fi
    
    # Create storage directories
    create_storage_directories
    if [ $? -ne 0 ]; then
        return 1
    fi
    
    # Initialize forecast index
    initialize_forecast_index
    if [ $? -ne 0 ]; then
        return 1
    fi
    
    # Rebuild forecast index to ensure it's up-to-date
    rebuild_forecast_index
    if [ $? -ne 0 ]; then
        return 1
    fi
    
    # Display storage information
    display_storage_info
    
    echo
    print_success "Storage initialization completed successfully."
    return 0
}

# Execute main function
main
exit $?