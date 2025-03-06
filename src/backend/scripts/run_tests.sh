#!/bin/bash
# run_tests.sh
# Shell script to run the test suite for the Electricity Market Price Forecasting System
# backend with appropriate configuration and reporting options.

set -e  # Exit immediately if a command exits with a non-zero status

# Default values
COVERAGE_FLAG=false
VERBOSE_FLAG=false
TEST_PATH="tests/"
PARALLEL_JOBS=0
KEYWORD=""
PYTEST_ARGS=""
FULL_TEST_SUITE=false

print_header() {
    echo "========================================================================="
    echo "Test Execution: $(date)"
    echo "Electricity Market Price Forecasting System - Backend Test Suite"
    echo "========================================================================="
}

print_usage() {
    echo "Usage: $(basename "$0") [options]"
    echo
    echo "Run the test suite for the Electricity Market Price Forecasting System backend."
    echo
    echo "Options:"
    echo "  -c          Enable coverage reporting"
    echo "  -v          Enable verbose output"
    echo "  -p PATH     Specify test path (default: tests/)"
    echo "  -j JOBS     Run tests in parallel with specified number of jobs"
    echo "  -k KEYWORD  Only run tests matching the given keyword expression"
    echo "  -f          Run full test suite with coverage and recommended settings"
    echo "  -h          Show this help message"
    echo
    echo "Examples:"
    echo "  $(basename "$0") -c -v        # Run all tests with coverage and verbose output"
    echo "  $(basename "$0") -p tests/test_forecasting_engine.py  # Run specific test file"
    echo "  $(basename "$0") -k \"forecast\"  # Run tests with 'forecast' in the name"
    echo "  $(basename "$0") -j 4         # Run tests in parallel using 4 processes"
    echo "  $(basename "$0") -f           # Run full test suite with recommended settings"
}

parse_arguments() {
    while getopts "cvp:j:k:fh" opt; do
        case $opt in
            c)
                COVERAGE_FLAG=true
                ;;
            v)
                VERBOSE_FLAG=true
                ;;
            p)
                TEST_PATH="$OPTARG"
                ;;
            j)
                PARALLEL_JOBS="$OPTARG"
                ;;
            k)
                KEYWORD="$OPTARG"
                ;;
            f)
                FULL_TEST_SUITE=true
                ;;
            h)
                print_usage
                exit 0
                ;;
            \?)
                echo "Invalid option: -$OPTARG" >&2
                print_usage
                exit 1
                ;;
        esac
    done
}

main() {
    print_header
    
    # Parse command-line arguments
    parse_arguments "$@"
    
    # If full test suite option is selected, set recommended options
    if $FULL_TEST_SUITE; then
        COVERAGE_FLAG=true
        VERBOSE_FLAG=true
        # Use CPU count - 1 for parallel jobs, but at least 1
        PARALLEL_JOBS=$(( $(nproc 2>/dev/null || echo 2) - 1 ))
        PARALLEL_JOBS=$(( PARALLEL_JOBS > 0 ? PARALLEL_JOBS : 1 ))
        echo "Running full test suite with recommended settings"
    fi
    
    # Set environment variables for testing
    export PYTHONPATH="$(pwd):${PYTHONPATH}"
    export FORECAST_ENV="test"
    
    echo "Starting test execution..."
    echo "Test path: $TEST_PATH"
    
    # Build pytest command based on options
    if $VERBOSE_FLAG; then
        PYTEST_ARGS="$PYTEST_ARGS -v"
    fi
    
    if $COVERAGE_FLAG; then
        PYTEST_ARGS="$PYTEST_ARGS --cov=src --cov-report=term --cov-report=html"
        echo "Coverage reporting enabled"
    fi
    
    if [ "$PARALLEL_JOBS" -gt 0 ]; then
        PYTEST_ARGS="$PYTEST_ARGS -n $PARALLEL_JOBS"
        echo "Running tests in parallel with $PARALLEL_JOBS jobs"
    fi
    
    if [ -n "$KEYWORD" ]; then
        # When using -k with pytest, we need to ensure proper quoting
        PYTEST_ARGS="$PYTEST_ARGS -k \"$KEYWORD\""
        echo "Only running tests matching: $KEYWORD"
    fi
    
    # Execute the tests
    echo "Running tests with: pytest $PYTEST_ARGS $TEST_PATH"
    
    # Use eval to handle complex arguments properly
    set +e  # Temporarily disable exit on error to handle test failures
    eval "pytest $PYTEST_ARGS $TEST_PATH"
    TEST_STATUS=$?
    set -e  # Re-enable exit on error
    
    # Generate coverage report if requested
    if $COVERAGE_FLAG; then
        echo "Generating coverage report..."
        echo "Coverage report available at: $(pwd)/htmlcov/index.html"
    fi
    
    # Print summary
    echo "========================================================================="
    if [ $TEST_STATUS -eq 0 ]; then
        echo "✅ All tests passed!"
    else
        echo "❌ Tests failed with status code $TEST_STATUS"
    fi
    echo "Completed at: $(date)"
    echo "========================================================================="
    
    return $TEST_STATUS
}

# Execute main function with all script arguments
main "$@"