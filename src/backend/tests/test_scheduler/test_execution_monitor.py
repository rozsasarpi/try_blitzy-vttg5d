import unittest
from unittest.mock import patch, Mock
import time
import datetime
import threading

# Internal imports
from src.backend.scheduler.execution_monitor import start_job_monitoring, stop_job_monitoring, get_monitored_jobs, is_job_monitored, get_job_monitoring_info, get_monitoring_status, DEFAULT_TIMEOUT_SECONDS, _monitored_jobs, _monitor_thread, _monitoring_active, _check_for_timeouts, _handle_job_timeout
from src.backend.scheduler.exceptions import MonitoringError, JobTimeoutError
from src.backend.scheduler.job_registry import get_job, update_job_status, JOB_STATUS_RUNNING, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED, JOB_STATUS_TIMEOUT


class TestExecutionMonitor(unittest.TestCase):
    """Test case for the execution monitor component"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        _monitored_jobs.clear()
        _monitoring_active = False
        _monitor_thread = None

    def tearDown(self):
        """Clean up after each test method"""
        _monitored_jobs.clear()
        _monitoring_active = False
        _monitor_thread = None

    @patch('src.backend.scheduler.execution_monitor.get_job')
    @patch('src.backend.scheduler.execution_monitor.update_job_status')
    def test_start_job_monitoring_success(self, mock_update_job_status, mock_get_job):
        """Test that job monitoring starts successfully"""
        mock_get_job.return_value = {'job_id': 'test_job', 'job_type': 'test_type'}
        result = start_job_monitoring('test_job', timeout_seconds=60)
        self.assertTrue(result)
        self.assertIn('test_job', _monitored_jobs)
        mock_update_job_status.assert_called_with('test_job', JOB_STATUS_RUNNING)

    @patch('src.backend.scheduler.execution_monitor.get_job')
    def test_start_job_monitoring_nonexistent_job(self, mock_get_job):
        """Test that monitoring fails for nonexistent job"""
        mock_get_job.return_value = None
        result = start_job_monitoring('nonexistent_job', timeout_seconds=60)
        self.assertFalse(result)
        self.assertNotIn('nonexistent_job', _monitored_jobs)

    @patch('src.backend.scheduler.execution_monitor.update_job_status')
    def test_stop_job_monitoring_success(self, mock_update_job_status):
        """Test that job monitoring stops successfully"""
        _monitored_jobs['test_job'] = {'job_id': 'test_job', 'start_time': datetime.datetime.now(), 'timeout_seconds': 60, 'job_type': 'test_type'}
        result = stop_job_monitoring('test_job', success=True)
        self.assertTrue(result)
        self.assertNotIn('test_job', _monitored_jobs)
        mock_update_job_status.assert_called_with('test_job', JOB_STATUS_COMPLETED, None)

    @patch('src.backend.scheduler.execution_monitor.update_job_status')
    def test_stop_job_monitoring_failure(self, mock_update_job_status):
        """Test that job monitoring stops with failure status"""
        _monitored_jobs['test_job'] = {'job_id': 'test_job', 'start_time': datetime.datetime.now(), 'timeout_seconds': 60, 'job_type': 'test_type'}
        test_exception = Exception('Test exception')
        result = stop_job_monitoring('test_job', success=False, error=test_exception)
        self.assertTrue(result)
        self.assertNotIn('test_job', _monitored_jobs)
        mock_update_job_status.assert_called_with('test_job', JOB_STATUS_FAILED, {'error': 'Test exception'})

    def test_stop_job_monitoring_nonexistent_job(self):
        """Test that stopping monitoring fails for nonexistent job"""
        result = stop_job_monitoring('nonexistent_job', success=True)
        self.assertFalse(result)

    def test_get_monitored_jobs(self):
        """Test retrieving list of monitored jobs"""
        _monitored_jobs['job1'] = {'job_id': 'job1', 'start_time': datetime.datetime.now(), 'timeout_seconds': 60, 'job_type': 'test_type'}
        _monitored_jobs['job2'] = {'job_id': 'job2', 'start_time': datetime.datetime.now(), 'timeout_seconds': 60, 'job_type': 'test_type'}
        monitored_jobs = get_monitored_jobs()
        self.assertEqual(len(monitored_jobs), 2)
        self.assertIn(_monitored_jobs['job1'], monitored_jobs)
        self.assertIn(_monitored_jobs['job2'], monitored_jobs)

    def test_is_job_monitored(self):
        """Test checking if a job is monitored"""
        _monitored_jobs['test_job'] = {'job_id': 'test_job', 'start_time': datetime.datetime.now(), 'timeout_seconds': 60, 'job_type': 'test_type'}
        self.assertTrue(is_job_monitored('test_job'))
        self.assertFalse(is_job_monitored('nonexistent_job'))

    def test_get_job_monitoring_info(self):
        """Test retrieving monitoring information for a job"""
        start_time = datetime.datetime.now()
        _monitored_jobs['test_job'] = {'job_id': 'test_job', 'start_time': start_time, 'timeout_seconds': 60, 'job_type': 'test_type'}
        job_info = get_job_monitoring_info('test_job')
        self.assertIsNotNone(job_info)
        self.assertEqual(job_info['job_id'], 'test_job')
        self.assertEqual(job_info['timeout_seconds'], 60)
        self.assertIn('elapsed_time', job_info)
        self.assertIsInstance(job_info['elapsed_time'], float)
        self.assertIsNone(get_job_monitoring_info('nonexistent_job'))

    def test_get_monitoring_status(self):
        """Test retrieving overall monitoring system status"""
        _monitoring_active = True
        mock_thread = Mock(spec=threading.Thread)
        mock_thread.is_alive.return_value = True
        _monitor_thread = mock_thread
        _monitored_jobs['test_job'] = {'job_id': 'test_job', 'start_time': datetime.datetime.now(), 'timeout_seconds': 60, 'job_type': 'test_type'}
        status = get_monitoring_status()
        self.assertTrue(status['active'])
        self.assertEqual(status['job_count'], 1)
        self.assertTrue(status['thread_alive'])

    @patch('src.backend.scheduler.execution_monitor._handle_job_timeout')
    @patch('src.backend.scheduler.execution_monitor.datetime')
    def test_check_for_timeouts_no_timeouts(self, mock_datetime, mock_handle_job_timeout):
        """Test timeout checking when no jobs have timed out"""
        start_time = datetime.datetime.now()
        _monitored_jobs['test_job'] = {'job_id': 'test_job', 'start_time': start_time, 'timeout_seconds': 60, 'job_type': 'test_type'}
        mock_datetime.datetime.now.return_value = start_time + datetime.timedelta(seconds=30)
        _check_for_timeouts()
        mock_handle_job_timeout.assert_not_called()
        self.assertIn('test_job', _monitored_jobs)

    @patch('src.backend.scheduler.execution_monitor._handle_job_timeout')
    @patch('src.backend.scheduler.execution_monitor.datetime')
    def test_check_for_timeouts_with_timeout(self, mock_datetime, mock_handle_job_timeout):
        """Test timeout checking when a job has timed out"""
        start_time = datetime.datetime.now() - datetime.timedelta(seconds=120)
        _monitored_jobs['test_job'] = {'job_id': 'test_job', 'start_time': start_time, 'timeout_seconds': 60, 'job_type': 'test_type'}
        mock_datetime.datetime.now.return_value = datetime.datetime.now()
        _check_for_timeouts()
        mock_handle_job_timeout.assert_called_with('test_job', _monitored_jobs['test_job'], (mock_datetime.datetime.now() - start_time).total_seconds())

    @patch('src.backend.scheduler.execution_monitor.update_job_status')
    @patch('src.backend.scheduler.execution_monitor.get_job')
    def test_handle_job_timeout(self, mock_get_job, mock_update_job_status):
        """Test handling of a job timeout"""
        mock_get_job.return_value = {'job_id': 'test_job', 'job_type': 'test_type'}
        _monitored_jobs['test_job'] = {'job_id': 'test_job', 'start_time': datetime.datetime.now(), 'timeout_seconds': 60, 'job_type': 'test_type'}
        _handle_job_timeout('test_job', _monitored_jobs['test_job'], 120)
        self.assertNotIn('test_job', _monitored_jobs)
        mock_update_job_status.assert_called_with('test_job', JOB_STATUS_TIMEOUT, {'elapsed_time': 120, 'timeout_seconds': 60})

    @patch('src.backend.scheduler.execution_monitor.threading.Thread')
    def test_monitor_thread_lifecycle(self, mock_thread):
        """Test the lifecycle of the monitoring thread"""
        start_job_monitoring('test_job', timeout_seconds=60)
        self.assertTrue(_monitoring_active)
        mock_thread.assert_called()
        mock_thread.return_value.start.assert_called()

        _monitored_jobs.clear()
        _check_for_timeouts()
        self.assertFalse(_monitoring_active)

    @patch('src.backend.scheduler.execution_monitor.get_job')
    def test_exception_handling(self, mock_get_job):
        """Test that exceptions are properly handled"""
        mock_get_job.side_effect = Exception('Test exception')
        with self.assertRaises(MonitoringError):
            start_job_monitoring('test_job', timeout_seconds=60)

        mock_get_job.side_effect = None
        mock_get_job.return_value = {'job_id': 'test_job', 'job_type': 'test_type'}
        start_job_monitoring('test_job', timeout_seconds=60)
        with patch('src.backend.scheduler.execution_monitor.update_job_status') as mock_update_job_status:
            mock_update_job_status.side_effect = Exception('Test exception')
            with self.assertRaises(MonitoringError):
                stop_job_monitoring('test_job', success=True)


if __name__ == '__main__':
    unittest.main()