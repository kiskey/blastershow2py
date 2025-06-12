import asyncio
import schedule
import time
from functools import partial

from logger import logger
from config import settings
from db import RedisClient
from utils.http_client import HttpClient
from parser import ThreadParser
from metadata_parser import MetadataParser
from normalizer import TitleNormalizer
from tasks import crawl_forum_task, fetch_and_cache_trackers # Import the defined tasks

class Scheduler:
    """
    Manages and runs background tasks using the 'schedule' library within an asyncio loop.
    """
    def __init__(self, redis_client: RedisClient, http_client: HttpClient,
                 thread_parser: ThreadParser, normalizer: TitleNormalizer):
        self.redis = redis_client
        self.http_client = http_client
        self.thread_parser = thread_parser
        self.normalizer = normalizer
        self.stop_event = asyncio.Event()
        logger.info("Scheduler initialized.")

    async def start(self):
        """
        Starts the scheduler, registering and running background tasks.
        """
        logger.info("Starting background scheduler...")

        # Schedule forum crawl task
        schedule.every(settings.CRAWL_INTERVAL_SECONDS).seconds.do(
            self._run_async_job,
            partial(crawl_forum_task, self.redis, self.http_client, self.thread_parser, self.normalizer)
        ).tag("forum_crawl")
        logger.info(f"Scheduled forum crawl every {settings.CRAWL_INTERVAL_SECONDS} seconds.")

        # Schedule tracker fetch task
        schedule.every(24).hours.do(
            self._run_async_job,
            partial(fetch_and_cache_trackers, self.redis)
        ).tag("tracker_fetch")
        logger.info(f"Scheduled tracker fetch every 24 hours.")

        # Run initial tasks immediately
        logger.info("Running initial background tasks...")
        await crawl_forum_task(self.redis, self.http_client, self.thread_parser, self.normalizer)
        await fetch_and_cache_trackers(self.redis)
        logger.info("Initial background tasks completed.")

        # Keep running the scheduler loop in the background
        while not self.stop_event.is_set():
            try:
                # Run pending jobs, if any
                await asyncio.to_thread(schedule.run_pending)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                await self.redis.add_error_to_queue({
                    "component": "scheduler_loop",
                    "message": "Error in scheduler loop",
                    "error": str(e),
                    "timestamp": int(time.time())
                })
            await asyncio.sleep(1) # Check every second

    async def stop(self):
        """
        Stops the scheduler gracefully.
        """
        logger.info("Stopping scheduler...")
        self.stop_event.set() # Signal the loop to stop
        # Clear all pending jobs
        schedule.clear()
        logger.info("Scheduler stopped.")

    def _run_async_job(self, func):
        """
        Helper to run an async function within the schedule's sync context.
        It creates a new task in the main event loop.
        """
        loop = asyncio.get_event_loop()
        loop.create_task(func())
        logger.debug(f"Scheduled async job '{func.__name__}' submitted.")

