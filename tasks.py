import time
import asyncio
from typing import List
from aiohttp import ClientSession, ClientError
from logger import logger
from config import settings
from db import RedisClient
from crawler import Crawler # Assuming crawler is in the same directory
from utils.http_client import HttpClient
from parser import ThreadParser
from metadata_parser import MetadataParser
from normalizer import TitleNormalizer


async def crawl_forum_task(
    redis_client: RedisClient,
    http_client: HttpClient,
    thread_parser: ThreadParser,
    normalizer: TitleNormalizer
):
    """
    Background task to initiate a full forum crawl.
    """
    logger.info("Starting scheduled forum crawl task...")
    crawler = Crawler(redis_client, http_client, thread_parser, normalizer)
    try:
        await crawler.crawl_forum(initial_pages=settings.INITIAL_PAGES)
        logger.info("Scheduled forum crawl task completed.")
    except Exception as e:
        logger.error(f"Error during scheduled forum crawl: {e}", exc_info=True)
        await redis_client.add_error_to_queue({
            "component": "scheduled_crawl_task",
            "message": "Failed during full forum crawl",
            "error": str(e),
            "timestamp": int(time.time())
        })


async def fetch_and_cache_trackers(redis_client: RedisClient):
    """
    Background task to fetch and cache top trackers from a public source.
    """
    tracker_url = "https://ngosang.github.io/trackerslist/trackers_all.txt" # Example public tracker list
    trackers: List[str] = []
    logger.info(f"Starting scheduled tracker fetch task from {tracker_url}...")

    # Use a simple aiohttp session for this specific fetch
    async with ClientSession() as session:
        try:
            async with session.get(tracker_url, timeout=10) as response:
                response.raise_for_status() # Raise an exception for HTTP errors
                text = await response.text()
                # Split by newline and filter out empty lines or comments
                trackers = [line.strip() for line in text.split('\n') if line.strip() and not line.strip().startswith('#')]
                await redis_client.set_trackers(trackers)
                logger.info(f"Successfully fetched and cached {len(trackers)} trackers.")
        except ClientError as e:
            logger.error(f"HTTP client error fetching trackers: {e}", exc_info=True)
            await redis_client.add_error_to_queue({
                "component": "tracker_fetcher",
                "message": f"Failed to fetch trackers from {tracker_url}",
                "error": str(e),
                "timestamp": int(time.time())
            })
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching trackers from {tracker_url}")
            await redis_client.add_error_to_queue({
                "component": "tracker_fetcher",
                "message": f"Timeout fetching trackers from {tracker_url}",
                "error": "TimeoutError",
                "timestamp": int(time.time())
            })
        except Exception as e:
            logger.error(f"An unexpected error occurred fetching trackers: {e}", exc_info=True)
            await redis_client.add_error_to_queue({
                "component": "tracker_fetcher",
                "message": "Unexpected error during tracker fetch",
                "error": str(e),
                "timestamp": int(time.time())
            })

