import asyncio
import aiorun
from contextlib import asynccontextmanager

from logger import logger
from config import settings
from db import RedisClient
from utils.http_client import HttpClient
from parser import ThreadParser
from metadata_parser import MetadataParser
from normalizer import TitleNormalizer, FuzzyMatcher
from crawler import Crawler
from scheduler import Scheduler
from api import StremioAPI
from aiohttp import web # Explicitly import web for aiohttp.web.AppRunner, etc.

@asynccontextmanager
async def lifespan_manager():
    """
    Manages the lifecycle of application components (startup and shutdown).
    """
    logger.info("Application starting up...")

    # Initialize components
    redis_client = RedisClient()
    normalizer = TitleNormalizer()
    fuzzy_matcher = FuzzyMatcher(normalizer)
    thread_parser = ThreadParser() # It internally uses MetadataParser

    # HTTP client managed by context manager
    async with HttpClient() as http_client:
        try:
            await redis_client.connect()
            if settings.PURGE_ON_START:
                await redis_client.purge_all_data()

            # Initialize scheduler and start it as a background task
            scheduler_instance = Scheduler(
                redis_client=redis_client,
                http_client=http_client,
                thread_parser=thread_parser,
                normalizer=normalizer
            )
            # Create a separate task for the scheduler
            scheduler_task = asyncio.create_task(scheduler_instance.start())
            logger.info("Scheduler task initiated.")

            # Initialize and get the aiohttp application
            stremio_api = StremioAPI(redis_client, normalizer, fuzzy_matcher)
            runner = web.AppRunner(stremio_api.app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', 8080)
            await site.start()
            logger.info("Stremio API server started on http://0.0.0.0:8080")

            yield # Application is running

        finally:
            logger.info("Application shutting down...")
            # Stop the scheduler first
            await scheduler_instance.stop()
            scheduler_task.cancel() # Cancel the scheduler task
            try:
                await scheduler_task # Await cancellation
            except asyncio.CancelledError:
                logger.info("Scheduler task cancelled successfully.")

            # Shut down aiohttp server
            await site.stop()
            await runner.cleanup()
            logger.info("Stremio API server shut down.")

            # Disconnect Redis
            await redis_client.disconnect()

            logger.info("Application shutdown complete.")

async def run_application():
    """
    Wrapper coroutine to properly run the lifespan_manager async context manager.
    """
    async with lifespan_manager():
        # Keep the event loop running indefinitely while the application is active
        # This prevents the 'run_application' coroutine from exiting prematurely.
        await asyncio.Event().wait()


def main():
    """
    Main entry point for the application using aiorun.
    """
    # Pass the run_application coroutine function directly to aiorun.run().
    aiorun.run(run_application, stop_on_unhandled_errors=True)

if __name__ == "__main__":
    main()
