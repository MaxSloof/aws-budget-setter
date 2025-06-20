import logging
import os

# Determine if running in AWS Lambda
running_in_lambda = "AWS_LAMBDA_FUNCTION_NAME" in os.environ

# Create a logger instance
logger = logging.getLogger(__name__)


# Function to configure logging
def configure_logging():
    # Remove existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # Set log level
    log_level = os.getenv("LAMBDA_LOG_LEVEL", "INFO")
    logger.setLevel(log_level)

    # Create a handler
    handler = logging.StreamHandler()

    # Set the formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


# Only configure logging if it hasn't been done yet
if not logger.hasHandlers():
    configure_logging()
