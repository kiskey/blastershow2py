import asyncio
import random
from aiohttp import ClientSession, ClientError
from tenacity import retry, wait_exponential, stop_after_attempt, before_log
from asyncio_throttle import Throttler
from logger import logger
from config import settings
from utils.bloom_filter import BloomFilter # Assuming BloomFilter is in the same directory

class HttpClient:
    """
    Asynchronous HTTP client with user-agent rotation, exponential backoff,
    request throttling, and Bloom filter for URL skipping.
    """
    def __init__(self, bloom_filter: BloomFilter = None):
        """
        Initializes the HTTP client.

        Args:
            bloom_filter (BloomFilter): An optional Bloom filter instance to use for URL checks.
        """
        self.session: ClientSession | None = None
        self.throttler = Throttler(1 / (settings.REQUEST_THROTTLE_MS / 1000)) # requests per second
        self.bloom_filter = bloom_filter
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/120.0.6099.109 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/120.0.6099.109 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Linux; Android 10; SM-G960F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.109 Mobile Safari/537.36"
        ]
        logger.info("HTTP Client initialized with user-agent rotation and throttling.")

    async def __aenter__(self):
        """
        Async context manager entry point. Initializes the aiohttp session.
        """
        logger.debug("Initializing aiohttp ClientSession.")
        self.session = ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Async context manager exit point. Closes the aiohttp session.
        """
        if self.session:
            logger.debug("Closing aiohttp ClientSession.")
            await self.session.close()
            self.session = None

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10), # Wait 1, 2, 4, 8, 10, 10... seconds
        stop=stop_after_attempt(5), # Try up to 5 times
        before_sleep=before_log(logger, "warning"), # Log before sleeping
        reraise=True # Re-raise the last exception if all attempts fail
    )
    async def get(self, url: str, force_fetch: bool = False, **kwargs) -> str | None:
        """
        Performs an asynchronous HTTP GET request with retries, throttling,
        and user-agent rotation.

        Args:
            url (str): The URL to request.
            force_fetch (bool): If True, bypass the Bloom filter check.
            **kwargs: Additional keyword arguments to pass to session.get().

        Returns:
            str | None: The response text if successful, None otherwise.
        """
        if not self.session:
            logger.error("HTTP session not initialized. Call HttpClient in an 'async with' block.")
            return None

        # Check Bloom filter before making the request, unless forced to fetch
        if self.bloom_filter and not force_fetch:
            if await self.bloom_filter.contains(url):
                logger.info(f"Skipping already processed URL (Bloom filter): {url}")
                return None

        headers = kwargs.pop("headers", {})
        headers["User-Agent"] = random.choice(self.user_agents)
        kwargs["headers"] = headers

        try:
            async with self.throttler: # Apply throttling before the request
                logger.debug(f"Attempting to fetch URL: {url} with User-Agent: {headers['User-Agent']}")
                async with self.session.get(url, **kwargs) as response:
                    response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
                    text = await response.text()
                    logger.info(f"Successfully fetched URL: {url}")
                    # Add to Bloom filter after successful fetch, unless force_fetch was used
                    if self.bloom_filter and not force_fetch:
                        await self.bloom_filter.add(url)
                    return text
        except ClientError as e:
            # Changed to error level as this is a significant client-side HTTP error
            logger.error(f"HTTP Client Error for {url}: {e}")
            # Re-raise to trigger tenacity retry or be caught by calling function
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while fetching {url}: {e}", exc_info=True)
            return None

