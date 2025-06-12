import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from loguru import logger

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore" # Ignore extra env vars not defined here
    )

    REDIS_URL: str = Field("redis://localhost:6379/0", description="URL for Redis connection")
    FORUM_URL: str = Field( # Changed from HttpUrl to str for precise URL handling of complex forum paths
        "https://www.1tamilblasters.fi/index.php?/forums/forum/63-tamil-new-web-series-tv-shows/", # Ensure this is the complete base forum page URL
        description="Base URL of the forum's target page to crawl (e.g., https://www.1tamilblasters.fi/index.php?/forums/forum/63-tamil-new-web-series-tv-shows/)"
    )
    INITIAL_PAGES: int = Field(5, description="Number of initial forum pages to crawl on startup")
    CRAWL_INTERVAL_SECONDS: int = Field(1800, description="Interval in seconds for full forum re-crawl")
    THREAD_REVISIT_HOURS: int = Field(24, description="Interval in hours to revisit individual threads")
    MAX_CONCURRENCY: int = Field(8, description="Maximum concurrent HTTP requests for crawling")
    REQUEST_THROTTLE_MS: int = Field(250, description="Milliseconds to wait between requests to the forum")
    PURGE_ON_START: bool = Field(False, description="If true, purge all Redis data on startup")
    DOMAIN_MONITOR: str = Field(
        "https://www.1tamilblasters.fi", # Placeholder if the domain changes, currently same as FORUM_URL base
        description="URL to monitor for domain changes (optional, for future use)"
    )
    LOG_LEVEL: str = Field("INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    # Add a fallback for the NLTK data path if not set by Dockerfile
    NLTK_DATA: str = Field(os.getenv("NLTK_DATA", "/usr/local/nltk_data"), description="Path to NLTK data directory")

# Initialize settings instance
settings = Settings()

# Log loaded settings (sensitive info redacted)
logger.info(f"Loaded application settings:")
for key, value in settings.model_dump().items():
    if key.upper() in ["REDIS_URL"]: # Add other sensitive keys here if any
        logger.info(f"  {key}: *********")
    else:
        logger.info(f"  {key}: {value}")

