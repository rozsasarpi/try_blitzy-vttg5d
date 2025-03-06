import logging

from app import server  # src/web/app.py

# Configure logging
logger = logging.getLogger(__name__)

def setup_wsgi_logging():
    """
    Configure logging specifically for the WSGI environment
    """
    # Configure basic logging format
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # Set log level based on environment
    log_level = logging.INFO  # Default log level
    # Example: if os.environ.get('ENVIRONMENT') == 'production':
    #     log_level = logging.WARNING

    logger.setLevel(log_level)

    # Log WSGI application startup
    logger.info("Starting WSGI application")

# Configure logging for WSGI environment
setup_wsgi_logging()
# Expose the Flask server instance for WSGI compatibility with Gunicorn
# The 'server' variable is required by Gunicorn