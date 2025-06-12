import sys
from loguru import logger
from config import settings

# Remove default handler
logger.remove()

# Add a new handler for structured JSON logging to stderr
logger.add(
    sys.stderr,
    level=settings.LOG_LEVEL.upper(),
    format="{time} {level} {message}",
    serialize=True, # Enable JSON serialization
    backtrace=True, # Include traceback in case of errors
    diagnose=True   # Include variables in traceback
)

# You can also add another handler for a file if needed
# logger.add("file_{time}.log", serialize=True, rotation="1 day")

# Make the logger instance available for import
__all__ = ["logger"]
