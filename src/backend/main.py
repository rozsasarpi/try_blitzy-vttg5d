import argparse
import datetime
import sys
import os
import signal

from flask import Flask, jsonify

# Internal imports
from .pipeline.pipeline_executor import execute_forecasting_pipeline, get_default_config
from .scheduler.forecast_scheduler import start_scheduler, stop_scheduler, schedule_forecast_job, run_forecast_now
from .api.routes import api_blueprint
from .utils.logging_utils import get_logger, setup_logging
from .config.settings import FORECAST_SCHEDULE_TIME, TIMEZONE, API_HOST, API_PORT

# Initialize logger
logger = get_logger(__name__)

# Create a Flask application instance
app = Flask(__name__)


def main() -> int:
    """Main entry point for the application"""
    try:
        # Set up logging configuration
        setup_logging()

        # Parse command-line arguments
        args = parse_args()

        # Execute the appropriate command based on arguments
        if args.command == "run":
            return run_forecast(args)
        elif args.command == "schedule":
            return start_scheduler_service(args)
        elif args.command == "serve":
            return start_api_server(args)
        else:
            logger.error("Invalid command")
            return 1

    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        return 1
    

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="Electricity Market Price Forecasting System")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Configure 'run' command for immediate forecast execution
    run_parser = subparsers.add_parser("run", help="Run a forecast immediately")
    run_parser.add_argument("--target_date", type=str, help="Target date for the forecast (YYYY-MM-DD), defaults to current date")
    run_parser.add_argument("--config_file", type=str, help="Path to a custom pipeline configuration file")

    # Configure 'schedule' command for starting scheduler service
    schedule_parser = subparsers.add_parser("schedule", help="Start the scheduler service")

    # Configure 'serve' command for starting API server
    serve_parser = subparsers.add_parser("serve", help="Start the API server")
    serve_parser.add_argument("--host", type=str, default=API_HOST, help="Host address for the API server")
    serve_parser.add_argument("--port", type=int, default=API_PORT, help="Port number for the API server")

    # Parse and return command-line arguments
    return parser.parse_args()


def run_forecast(args: argparse.Namespace) -> int:
    """Run a forecast immediately"""
    logger.info("Starting immediate forecast execution")

    # Parse target date from args if provided, otherwise use current date
    target_date_str = args.target_date
    target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date() if target_date_str else datetime.date.today()

    # Parse config file from args if provided, otherwise use default config
    config_file = args.config_file
    config = load_config_from_file(config_file) if config_file else get_default_config()

    # Execute forecasting pipeline with target date and config
    results = execute_forecasting_pipeline(target_date, config)

    logger.info(f"Forecast execution completed successfully: {results}")
    return 0


def start_scheduler_service(args: argparse.Namespace) -> int:
    """Start the scheduler service for automated forecasts"""
    logger.info("Starting scheduler service")

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start the scheduler service
    start_scheduler()

    # Schedule the daily forecast job at 7 AM CST
    schedule_forecast_job()

    # Keep the service running until interrupted
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Scheduler service interrupted by user")
    finally:
        stop_scheduler("Scheduler service stopped")

    return 0


def start_api_server(args: argparse.Namespace) -> int:
    """Start the API server for forecast access"""
    logger.info("Starting API server")

    # Register the API blueprint with the Flask app
    app.register_blueprint(api_blueprint)

    # Configure host and port from settings or command-line args
    host = args.host or API_HOST
    port = args.port or API_PORT

    # Start the Flask development server
    app.run(host=host, port=port, debug=True)

    return 0


def signal_handler(signum: int, frame: object) -> None:
    """Handle termination signals for graceful shutdown"""
    logger.info(f"Received termination signal: {signum}")
    stop_scheduler(reason=f"Received termination signal {signum}")
    sys.exit(0)


def load_config_from_file(config_file: str) -> dict:
    """Load configuration from a JSON file"""
    try:
        # Check if config file exists
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file not found: {config_file}")

        # Open and parse JSON configuration file
        with open(config_file, "r") as f:
            config = json.load(f)

        # Return the parsed configuration dictionary
        return config

    except FileNotFoundError as e:
        logger.error(f"Config file not found: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON config file: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error loading config file: {str(e)}")
        raise


if __name__ == "__main__":
    sys.exit(main())