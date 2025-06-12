import sys
from loguru import logger
from config import settings

# Remove default handler
logger.remove()

# Add a new handler for structured JSON logging to stderr
# Changed serialize=False and updated format for human-readability
logger.add(
    sys.stderr,
    level=settings.LOG_LEVEL.upper(),
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    serialize=False, # Disable JSON serialization for human-readable logs
    backtrace=True, # Include traceback in case of errors
    diagnose=True   # Include variables in traceback
)

# You can also add another handler for a file if needed
# logger.add("file_{time}.log", serialize=True, rotation="1 day")

# Make the logger instance available for import
__all__ = ["logger"]
