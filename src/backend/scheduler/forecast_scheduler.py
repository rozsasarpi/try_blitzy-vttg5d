"""Core scheduler implementation for the Electricity Market Price Forecasting System that manages the daily execution of forecast generation at 7 AM CST. Provides functionality to initialize, start, and stop the scheduler, as well as schedule and execute forecast jobs with appropriate error handling and monitoring."""

import datetime  # standard library
import time  # standard library
import uuid  # standard library
import threading  # standard library
import typing  # standard library

from apscheduler.schedulers.background import BackgroundScheduler  # version: 3.10.0

from .exceptions import SchedulerError, JobSchedulingError, JobExecutionError, ScheduleConfigurationError, SchedulerInitializationError  # Module: src/backend/scheduler/exceptions.py
from .job_registry import register_job, update_job_status, get_job, JOB_STATUS_PENDING, JOB_STATUS_RUNNING, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED  # Module: src/backend/scheduler/job_registry.py
from .execution_monitor import start_job_monitoring, stop_job_monitoring, DEFAULT_TIMEOUT_SECONDS  # Module: src/backend/scheduler/execution_monitor.py
from .scheduler_logging import log_scheduler_startup, log_scheduler_shutdown, log_scheduler_error, log_scheduler_job_added, log_job_execution_start, log_job_execution_completion, log_job_execution_failure  # Module: src/backend/scheduler/scheduler_logging.py
from ..pipeline.pipeline_executor import execute_forecasting_pipeline, get_default_config  # Module: src/backend/pipeline/pipeline_executor.py
from ..config.settings import FORECAST_SCHEDULE_TIME, TIMEZONE  # Module: src/backend/config/settings.py
from ..utils.decorators import timing_decorator, log_exceptions  # Module: src/backend/utils/decorators.py
from ..utils.logging_utils import get_logger  # Module: src/backend/utils/logging_utils.py

# Global variables
_scheduler: Optional[BackgroundScheduler] = None
_scheduler_lock = threading.RLock()
_scheduler_running = False
_scheduler_job_ids: List[str] = []
JOB_TYPE_FORECAST = "forecast"
logger = get_logger(__name__)


@log_exceptions
def initialize_scheduler(config: Optional[Dict] = None) -> BackgroundScheduler:
    """Initialize the APScheduler instance with appropriate configuration

    Args:
        config: Configuration dictionary for the scheduler

    Returns:
        Configured scheduler instance
    """
    with _scheduler_lock:
        try:
            # Create default configuration if none provided
            if config is None:
                config = {}

            # Configure APScheduler with timezone, job defaults, and executors
            scheduler_config = {
                'timezone': str(TIMEZONE),
                'job_defaults': {
                    'misfire_grace_time': 60,  # 1 minute
                    'coalesce': True,
                    'max_instances': 1
                }
            }

            # Create BackgroundScheduler instance with configuration
            scheduler = BackgroundScheduler(**scheduler_config)

            # Log scheduler initialization with configuration details
            log_scheduler_startup(config)

            return scheduler

        except Exception as e:
            log_scheduler_error(f"Scheduler initialization failed: {str(e)}", e, {"operation": "initialize_scheduler"})
            raise SchedulerInitializationError(f"Scheduler initialization failed: {str(e)}", e)


@log_exceptions
def start_scheduler() -> bool:
    """Start the scheduler service to begin processing jobs

    Returns:
        True if scheduler started successfully, False if already running
    """
    with _scheduler_lock:
        try:
            global _scheduler
            global _scheduler_running

            # Check if scheduler is already running
            if is_scheduler_running():
                logger.warning("Scheduler is already running")
                return False

            # Initialize scheduler if not already initialized
            if _scheduler is None:
                _scheduler = initialize_scheduler()

            # Start the scheduler
            _scheduler.start()
            _scheduler_running = True

            # Log scheduler startup
            log_scheduler_startup({})

            return True

        except Exception as e:
            log_scheduler_error(f"Scheduler startup failed: {str(e)}", e, {"operation": "start_scheduler"})
            raise SchedulerError(f"Scheduler startup failed: {str(e)}")


@log_exceptions
def stop_scheduler(reason: str) -> bool:
    """Stop the scheduler service

    Args:
        reason: Reason for stopping the scheduler

    Returns:
        True if scheduler stopped successfully, False if not running
    """
    with _scheduler_lock:
        try:
            global _scheduler
            global _scheduler_running

            # Check if scheduler is running
            if not is_scheduler_running():
                logger.warning("Scheduler is not running")
                return False

            # Shutdown the scheduler
            _scheduler.shutdown(wait=True)
            _scheduler_running = False

            # Log scheduler shutdown
            log_scheduler_shutdown(reason, {})

            return True

        except Exception as e:
            log_scheduler_error(f"Scheduler shutdown failed: {str(e)}", e, {"operation": "stop_scheduler"})
            raise SchedulerError(f"Scheduler shutdown failed: {str(e)}")


def is_scheduler_running() -> bool:
    """Check if the scheduler is currently running

    Returns:
        True if scheduler is running, False otherwise
    """
    with _scheduler_lock:
        return _scheduler_running


@timing_decorator
@log_exceptions
def schedule_forecast_job(job_params: Optional[Dict] = None) -> str:
    """Schedule the daily forecast generation job at 7 AM CST

    Args:
        job_params: Dictionary of job parameters

    Returns:
        ID of the scheduled job
    """
    with _scheduler_lock:
        try:
            global _scheduler
            global _scheduler_job_ids

            # Check if scheduler is running
            if _scheduler is None or not is_scheduler_running():
                raise SchedulerError("Scheduler is not running")

            # Create job parameters dictionary with default values if not provided
            if job_params is None:
                job_params = {}

            # Calculate next run time at 7 AM CST
            next_run_time = _calculate_next_run_time()

            # Register job in registry
            job_id = register_job(job_type=JOB_TYPE_FORECAST, schedule_time=next_run_time, job_params=job_params)

            # Add job to scheduler
            _scheduler.add_job(execute_forecast_job, 'date', run_date=next_run_time, args=[job_id, job_params], id=job_id)
            _scheduler_job_ids.append(job_id)

            # Log job addition
            log_scheduler_job_added(job_id, JOB_TYPE_FORECAST, next_run_time, job_params)

            return job_id

        except Exception as e:
            log_scheduler_error(f"Job scheduling failed: {str(e)}", e, {"operation": "schedule_forecast_job"})
            raise JobSchedulingError(f"Job scheduling failed: {str(e)}", "unknown", JOB_TYPE_FORECAST, datetime.datetime.now())


@timing_decorator
@log_exceptions
def schedule_one_time_forecast(run_time: datetime.datetime, job_params: Optional[Dict] = None) -> str:
    """Schedule a one-time forecast generation at a specific time

    Args:
        run_time: Time to run the forecast generation
        job_params: Dictionary of job parameters

    Returns:
        ID of the scheduled job
    """
    with _scheduler_lock:
        try:
            global _scheduler
            global _scheduler_job_ids

            # Check if scheduler is running
            if _scheduler is None or not is_scheduler_running():
                raise SchedulerError("Scheduler is not running")

            # Create job parameters dictionary with default values if not provided
            if job_params is None:
                job_params = {}

            # Register job in registry
            job_id = register_job(job_type=JOB_TYPE_FORECAST, schedule_time=run_time, job_params=job_params)

            # Add job to scheduler
            _scheduler.add_job(execute_forecast_job, 'date', run_date=run_time, args=[job_id, job_params], id=job_id)
            _scheduler_job_ids.append(job_id)

            # Log job addition
            log_scheduler_job_added(job_id, JOB_TYPE_FORECAST, run_time, job_params)

            return job_id

        except Exception as e:
            log_scheduler_error(f"Job scheduling failed: {str(e)}", e, {"operation": "schedule_one_time_forecast"})
            raise JobSchedulingError(f"Job scheduling failed: {str(e)}", "unknown", JOB_TYPE_FORECAST, run_time)


@timing_decorator
@log_exceptions
def run_forecast_now(job_params: Optional[Dict] = None) -> dict:
    """Execute a forecast generation immediately

    Args:
        job_params: Dictionary of job parameters

    Returns:
        Results of the forecast execution
    """
    try:
        # Generate a job ID
        job_id = str(uuid.uuid4())

        # Create job parameters dictionary with default values if not provided
        if job_params is None:
            job_params = {}

        # Register job in registry
        register_job(job_id=job_id, job_type=JOB_TYPE_FORECAST, schedule_time=datetime.datetime.now(), job_params=job_params)

        # Execute the forecast job directly
        results = execute_forecast_job(job_id, job_params)

        return results

    except Exception as e:
        log_scheduler_error(f"Job execution failed: {str(e)}", e, {"operation": "run_forecast_now"})
        raise JobExecutionError(f"Job execution failed: {str(e)}", "unknown", JOB_TYPE_FORECAST, datetime.datetime.now(), e)


@timing_decorator
@log_exceptions
def execute_forecast_job(job_id: str, job_params: Dict) -> dict:
    """Execute the forecast generation job

    Args:
        job_id: ID of the job to execute
        job_params: Dictionary of job parameters

    Returns:
        Results of the forecast execution
    """
    try:
        # Get job details from registry
        job_details = get_job(job_id)
        if job_details is None:
            raise ValueError(f"Job with ID {job_id} not found")

        # Update job status to running
        update_job_status(job_id, JOB_STATUS_RUNNING)

        # Start job monitoring
        start_job_monitoring(job_id)

        # Log job execution start
        log_job_execution_start(job_id, job_details["job_type"], job_details["job_params"])

        # Get target date from job parameters or use current date
        target_date = job_params.get("target_date", datetime.datetime.now())

        # Get pipeline configuration from job parameters or use default
        pipeline_config = job_params.get("pipeline_config", get_default_config())

        # Execute the forecasting pipeline
        results = execute_forecasting_pipeline(target_date, pipeline_config)

        # Update job status to completed
        update_job_status(job_id, JOB_STATUS_COMPLETED, results)

        # Stop job monitoring
        stop_job_monitoring(job_id, success=True, execution_details=results)

        # Log job execution completion
        log_job_execution_completion(job_id, job_details["job_type"], results["execution_time"], results)

        return results

    except Exception as e:
        # Update job status to failed
        update_job_status(job_id, JOB_STATUS_FAILED, {"error": str(e)})

        # Stop job monitoring
        stop_job_monitoring(job_id, success=False, error=e)

        # Log failure
        log_job_execution_failure(job_id, job_details["job_type"], 0, e, {"error": str(e)})

        raise JobExecutionError(f"Job execution failed: {str(e)}", job_id, job_details["job_type"], datetime.datetime.now(), e)


@log_exceptions
def get_next_run_time() -> Optional[datetime.datetime]:
    """Get the next scheduled forecast run time

    Returns:
        Next scheduled run time or None if no jobs scheduled
    """
    with _scheduler_lock:
        try:
            # Check if scheduler is running
            if _scheduler is None or not is_scheduler_running():
                logger.warning("Scheduler is not running, cannot get next run time")
                return None

            # Get all scheduled jobs
            jobs = _scheduler.get_jobs()

            # Find the next job with the earliest run time
            next_run_time = None
            for job in jobs:
                if next_run_time is None or job.next_run_time < next_run_time:
                    next_run_time = job.next_run_time

            return next_run_time

        except Exception as e:
            log_scheduler_error(f"Failed to get next run time: {str(e)}", e, {"operation": "get_next_run_time"})
            return None


@log_exceptions
def get_scheduler_status() -> dict:
    """Get current scheduler status information

    Returns:
        Dictionary with scheduler status details
    """
    with _scheduler_lock:
        try:
            # Create status dictionary
            status = {"running": is_scheduler_running()}

            # If scheduler is running, add job count and next run time
            if is_scheduler_running():
                status["job_count"] = len(_scheduler.get_jobs())
                next_run_time = get_next_run_time()
                status["next_run_time"] = next_run_time.isoformat() if next_run_time else None

            return status

        except Exception as e:
            log_scheduler_error(f"Failed to get scheduler status: {str(e)}", e, {"operation": "get_scheduler_status"})
            return {"running": False, "error": str(e)}


def _calculate_next_run_time() -> datetime.datetime:
    """Calculate the next run time for daily forecast at 7 AM CST

    Returns:
        Next 7 AM CST run time
    """
    # Get current time in CST timezone
    now_cst = datetime.datetime.now(TIMEZONE)

    # Create datetime for today at 7 AM CST
    next_time = datetime.datetime.combine(
        now_cst.date(),
        FORECAST_SCHEDULE_TIME,
        tzinfo=TIMEZONE
    )

    # If current time is past 7 AM, use tomorrow at 7 AM
    if now_cst >= next_time:
        next_time = next_time + datetime.timedelta(days=1)

    return next_time


class ForecastScheduler:
    """Class that encapsulates the forecast scheduling functionality"""

    def __init__(self, config: Optional[Dict] = None):
        """Initialize the forecast scheduler with configuration

        Args:
            config: Configuration dictionary for the scheduler
        """
        self._config = config or {}
        self._scheduler: Optional[BackgroundScheduler] = None
        self._running = False
        self._job_ids: List[str] = []

    def initialize(self):
        """Initialize the scheduler with configuration"""
        self._scheduler = initialize_scheduler(self._config)

    def start(self) -> bool:
        """Start the scheduler service

        Returns:
            True if started successfully, False if already running
        """
        if self.is_running():
            logger.warning("Scheduler is already running")
            return False

        if self._scheduler is None:
            self.initialize()

        self._scheduler.start()
        self._running = True
        log_scheduler_startup(self._config)
        return True

    def stop(self, reason: str) -> bool:
        """Stop the scheduler service

        Args:
            reason: Reason for stopping the scheduler

        Returns:
            True if stopped successfully, False if not running
        """
        if not self.is_running():
            logger.warning("Scheduler is not running")
            return False

        self._scheduler.shutdown()
        self._running = False
        log_scheduler_shutdown(reason, {})
        return True

    def is_running(self) -> bool:
        """Check if scheduler is running

        Returns:
            True if running, False otherwise
        """
        return self._running

    def schedule_daily_forecast(self, job_params: Optional[Dict] = None) -> str:
        """Schedule daily forecast at 7 AM CST

        Args:
            job_params: Dictionary of job parameters

        Returns:
            Job ID of scheduled forecast
        """
        if not self.is_running():
            raise SchedulerError("Scheduler is not running")

        job_id = schedule_forecast_job(job_params)
        self._job_ids.append(job_id)
        return job_id

    def schedule_one_time(self, run_time: datetime.datetime, job_params: Optional[Dict] = None) -> str:
        """Schedule one-time forecast at specific time

        Args:
            run_time: Time to run the forecast
            job_params: Dictionary of job parameters

        Returns:
            Job ID of scheduled forecast
        """
        if not self.is_running():
            raise SchedulerError("Scheduler is not running")

        job_id = schedule_one_time_forecast(run_time, job_params)
        self._job_ids.append(job_id)
        return job_id

    def run_now(self, job_params: Optional[Dict] = None) -> dict:
        """Run forecast immediately

        Args:
            job_params: Dictionary of job parameters

        Returns:
            Forecast execution results
        """
        return run_forecast_now(job_params)

    def get_next_run_time(self) -> Optional[datetime.datetime]:
        """Get next scheduled run time

        Returns:
            Next run time or None
        """
        if not self.is_running():
            logger.warning("Scheduler is not running, cannot get next run time")
            return None

        return get_next_run_time()

    def get_status(self) -> dict:
        """Get scheduler status information

        Returns:
            Status dictionary
        """
        return get_scheduler_status()