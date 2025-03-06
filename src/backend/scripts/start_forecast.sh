#!/bin/bash

# Electricity Market Price Forecasting System
# Script to start the forecasting process or scheduler service

# Global variables
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_ROOT=$(realpath "$SCRIPT_DIR/../..")
PYTHON_EXECUTABLE="python"
MAIN_SCRIPT="$PROJECT_ROOT/backend/main.py"
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/forecast_$(date +%Y%m%d_%H%M%S).log"

# Print usage information
print_usage() {
    echo "Electricity Market Price Forecasting System"
    echo "Usage: $(basename "$0") [command] [options]"
    echo ""
    echo "Commands:"
    echo "  forecast [date]     Generate a forecast for the specified date (YYYY-MM-DD)"
    echo "                      If date is not provided, forecast for tomorrow will be generated"
    echo "  scheduler           Start the forecast scheduler service (runs daily at 7 AM CST)"
    echo "  api [port]          Start the API server on the specified port (default: 8000)"
    echo "  help                Display this help message"
    echo ""
    echo "Examples:"
    echo "  $(basename "$0") forecast 2023-06-01     Generate forecast for June 1, 2023"
    echo "  $(basename "$0") forecast                 Generate forecast for tomorrow"
    echo "  $(basename "$0") scheduler                Start scheduler service"
    echo "  $(basename "$0") api 8080                 Start API server on port 8080"
    echo ""
}

# Setup environment for running the forecast
setup_environment() {
    # Create log directory if it doesn't exist
    mkdir -p "$LOG_DIR"
    
    # Check if Python executable is available
    if ! command -v "$PYTHON_EXECUTABLE" &> /dev/null; then
        echo "Error: Python executable not found. Please install Python 3.10 or later." >&2
        return 1
    fi
    
    # Check if main script exists
    if [ ! -f "$MAIN_SCRIPT" ]; then
        echo "Error: Main script not found at $MAIN_SCRIPT" >&2
        return 1
    fi
    
    # Set timezone to CST to ensure correct scheduling
    export TZ="America/Chicago"
    
    # Set any additional environment variables needed by the forecast system
    export FORECAST_ENV="production"
    
    return 0
}

# Run a forecast for a specific date
run_forecast() {
    local date="$1"
    local exit_code=0
    
    # If no date provided, use tomorrow's date
    if [ -z "$date" ]; then
        date=$(date -d "tomorrow" +%Y-%m-%d)
    fi
    
    echo "Starting forecast generation for $date at $(date)"
    echo "Log file: $LOG_FILE"
    
    # Execute the main Python script with forecast command
    "$PYTHON_EXECUTABLE" "$MAIN_SCRIPT" forecast --date "$date" > "$LOG_FILE" 2>&1
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "Forecast completed successfully."
    else
        echo "Forecast failed with exit code $exit_code. Check log file for details."
    fi
    
    return $exit_code
}

# Start the forecast scheduler service
run_scheduler() {
    echo "Starting forecast scheduler service at $(date)"
    echo "Log file: $LOG_FILE"
    
    # Two options for scheduler implementation:
    # 1. Set up a cron job
    # 2. Start a long-running Python service
    
    # Option 2: Start the scheduler as a long-running service
    "$PYTHON_EXECUTABLE" "$MAIN_SCRIPT" scheduler > "$LOG_FILE" 2>&1 &
    
    # Capture PID of the background process
    local scheduler_pid=$!
    echo "Scheduler service started with PID: $scheduler_pid"
    
    # Save PID to file for later management
    echo $scheduler_pid > "$PROJECT_ROOT/scheduler.pid"
    
    # Check if process started successfully
    if ps -p $scheduler_pid > /dev/null; then
        echo "Scheduler service is running. It will execute forecasts daily at 7 AM CST."
        return 0
    else
        echo "Failed to start scheduler service. Check log file for details."
        return 1
    fi
}

# Start the API server
run_api() {
    local port="$1"
    
    # Use default port if not specified
    if [ -z "$port" ]; then
        port="8000"
    fi
    
    echo "Starting API server on port $port at $(date)"
    echo "Log file: $LOG_FILE"
    
    # Execute the main Python script with API command
    "$PYTHON_EXECUTABLE" "$MAIN_SCRIPT" api --port "$port" > "$LOG_FILE" 2>&1 &
    
    # Capture PID of the background process
    local api_pid=$!
    echo "API server started with PID: $api_pid"
    
    # Save PID to file for later management
    echo $api_pid > "$PROJECT_ROOT/api.pid"
    
    # Check if process started successfully
    sleep 2
    if ps -p $api_pid > /dev/null; then
        echo "API server is running on http://localhost:$port"
        return 0
    else
        echo "Failed to start API server. Check log file for details."
        return 1
    fi
}

# Main entry point
main() {
    local command="$1"
    local exit_code=0
    
    # Setup environment
    setup_environment
    if [ $? -ne 0 ]; then
        return 1
    fi
    
    # Process command
    case "$command" in
        forecast)
            run_forecast "$2"
            exit_code=$?
            ;;
        scheduler)
            run_scheduler
            exit_code=$?
            ;;
        api)
            run_api "$2"
            exit_code=$?
            ;;
        help|"")
            print_usage
            exit_code=0
            ;;
        *)
            echo "Error: Unknown command '$command'" >&2
            print_usage
            exit_code=1
            ;;
    esac
    
    return $exit_code
}

# Execute main function with all arguments
main "$@"
exit $?