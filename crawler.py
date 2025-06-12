import asyncio
import re
from urllib.parse import urljoin, urlparse
import time
from typing import Dict, Any, List
from aiohttp import ClientError, ClientResponseError # Explicitly import ClientResponseError

from logger import logger
from config import settings
from db import RedisClient
from utils.http_client import HttpClient
from utils.bloom_filter import BloomFilter
from parser import ThreadParser
from metadata_parser import MetadataParser
from normalizer import TitleNormalizer

class Crawler:
    """
    Crawls the forum, identifies new/updated threads, and enqueues them for processing.
    Handles pagination, concurrency, thread revisit intervals, and duplicate URL skipping.
    """
    def __init__(self, redis_client: RedisClient, http_client: HttpClient,
                 thread_parser: ThreadParser, normalizer: TitleNormalizer):
        self.redis = redis_client
        self.http_client = http_client
        self.thread_parser = thread_parser
        self.normalizer = normalizer

        self.forum_base_url_for_pagination = settings.FORUM_URL
        if not self.forum_base_url_for_pagination.endswith('/'):
            self.forum_base_url_for_pagination += '/'

        parsed_settings_forum_url = urlparse(settings.FORUM_URL)
        self.base_domain_url = f"{parsed_settings_forum_url.scheme}://{parsed_settings_forum_url.netloc}"

        self.thread_url_pattern = re.compile(r"/forums/topic/(\d+)-")
        self.thread_processing_queue = asyncio.Queue()
        self.crawl_bloom_filter = BloomFilter(
            redis_client.client, "bloomfilter:urls",
            capacity=100000, error_rate=0.01
        )
        self.http_client.bloom_filter = self.crawl_bloom_filter
        logger.info(f"Crawler initialized. Forum base URL for pagination: {self.forum_base_url_for_pagination}")
        logger.info(f"Crawler initialized. Base domain for general joins: {self.base_domain_url}")


    async def _get_thread_links_from_page(self, html_content: str) -> List[Dict[str, str]]:
        """
        Extracts valid thread links from a forum page's HTML content.
        Robustly parses links by targeting common IP.Board thread title structure (h4 within ipsDataItem).
        """
        soup = self.thread_parser.BeautifulSoup(html_content, 'html.parser')
        thread_links = []

        # Find list items that are likely thread containers.
        # This is a common class for individual entries in IP.Board lists.
        thread_containers = soup.find_all("li", class_=lambda x: x and "ipsDataItem" in x.split())

        for container in thread_containers:
            # Within each container, look for an h4 tag that typically holds the title,
            # and then find an <a> tag within that h4 that has an href.
            title_h4 = container.find("h4", class_="ipsDataItem_title")
            if title_h4:
                a_tag = title_h4.find("a", href=True)
                if a_tag:
                    href = a_tag["href"]

                    # Ensure it's a full URL by joining with the base_domain_url if it's a relative path.
                    if not href.startswith("http"):
                        href = urljoin(self.base_domain_url, href)

                    # Check if the href matches the defined thread URL pattern.
                    match = self.thread_url_pattern.search(href)
                    if match:
                        thread_id = match.group(1)
                        # Basic deduplication: Add only if the thread_id is found and not already in our list.
                        if thread_id and not any(link['id'] == thread_id for link in thread_links):
                            thread_links.append({"url": href, "id": thread_id})
                            logger.debug(f"Found thread link: {href} (ID: {thread_id})")
        
        if not thread_links:
            logger.info("No thread links found on the current page. Check HTML structure or URL patterns.")
        return thread_links

    async def _process_thread(self, thread_url: str, thread_id: str):
        """
        Fetches and processes a single thread page.
        """
        try:
            html = await self.http_client.get(thread_url)
            if html:
                parsed_data = self.thread_parser.parse_thread_page(html, self.base_domain_url)
                if parsed_data and parsed_data["title"]:
                    parsed_data["thread_id"] = thread_id
                    await self.thread_processing_queue.put(parsed_data)
                    logger.info(f"Enqueued thread '{parsed_data['title']}' (ID: {thread_id}) for processing.")
                else:
                    logger.warning(f"Failed to parse essential data from thread: {thread_url}")
            else:
                logger.warning(f"Failed to fetch content for thread: {thread_url}")
        except ClientResponseError as e: # Specific HTTP client errors (e.g., 403, 404, 500)
            logger.error(f"HTTP Client Response Error processing thread {thread_url}: Status={e.status}, Message='{e.message}', URL='{e.url}'")
            await self.redis.add_error_to_queue({
                "component": "crawler",
                "message": f"HTTP Client Response Error processing thread {thread_url}: Status={e.status}, Error='{e.message}'",
                "error": str(e),
                "timestamp": int(time.time())
            })
        except ClientError as e: # Other generic aiohttp client errors (e.g., network issues)
            logger.error(f"Generic HTTP Client Error processing thread {thread_url}: {str(e)}") # Log without exc_info
            await self.redis.add_error_to_queue({
                "component": "crawler",
                "message": f"Generic HTTP Client Error processing thread {thread_url}: {str(e)}",
                "error": str(e),
                "timestamp": int(time.time())
            })
        except Exception as e: # Catch-all for other unexpected errors during processing
            logger.error(f"Error processing thread {thread_url}: {str(e)}") # Log without exc_info to prevent loguru conflict
            await self.redis.add_error_to_queue({
                "component": "crawler",
                "message": f"Failed to process thread {thread_url}",
                "error": str(e),
                "timestamp": int(time.time())
            })

    async def crawl_forum(self, initial_pages: int = settings.INITIAL_PAGES):
        """
        Initiates the forum crawling process.
        Crawls paginated URLs until a 404 is returned or initial_pages limit is reached.
        Dispatches thread processing to workers.
        """
        logger.info(f"Starting forum crawl for {initial_pages} initial pages.")
        page_num = 1
        processed_thread_count = 0
        current_time = int(time.time())
        revisit_threshold = current_time - (settings.THREAD_REVISIT_HOURS * 3600)

        worker_tasks = [
            asyncio.create_task(self._thread_processor_worker(f"Worker-{i+1}"))
            for i in range(settings.MAX_CONCURRENCY)
        ]

        try:
            while page_num <= initial_pages:
                if page_num == 1:
                    page_url = settings.FORUM_URL
                else:
                    page_url = f"{self.forum_base_url_for_pagination}page/{page_num}/"

                logger.info(f"Crawling forum page: {page_url}")

                page_html = await self.http_client.get(page_url, force_fetch=True)
                if not page_html:
                    logger.info(f"Reached end of forum pagination at page {page_num} (no content or 404).")
                    break

                thread_links = await self._get_thread_links_from_page(page_html)

                tasks = []
                for thread in thread_links:
                    thread_id = thread["id"]
                    thread_url = thread["url"]

                    last_visited = await self.redis.get_thread_last_visited(thread_id)

                    if not last_visited or last_visited < revisit_threshold:
                        tasks.append(self._process_thread(thread_url, thread_id))
                        await self.redis.set_thread_last_visited(thread_id, current_time)
                        processed_thread_count += 1
                    else:
                        logger.debug(f"Skipping recently visited thread: {thread_url} (ID: {thread_id})")

                if tasks:
                    await asyncio.gather(*tasks)
                else:
                    logger.info(f"No new or outdated threads found on page {page_num}.")

                page_num += 1

        except ClientResponseError as e: # Specific HTTP client errors during forum crawl
            logger.error(f"HTTP Client Error during forum crawling: Status={e.status}, Message='{e.message}', URL='{e.url}'")
            await self.redis.add_error_to_queue({
                "component": "crawler",
                "message": f"HTTP Client Error during forum crawl: Status={e.status}, Error='{e.message}'",
                "error": str(e),
                "timestamp": int(time.time())
            })
        except ClientError as e: # Other generic aiohttp client errors during forum crawl
            logger.error(f"Generic HTTP Client Error during forum crawling: {str(e)}")
            await self.redis.add_error_to_queue({
                "component": "crawler",
                "message": f"Generic HTTP Client Error during forum crawl: {str(e)}",
                "error": str(e),
                "timestamp": int(time.time())
            })
        except Exception as e: # Catch-all for other unexpected errors during forum crawl
            logger.error(f"Critical error during forum crawling: {str(e)}") # Log without exc_info
            await self.redis.add_error_to_queue({
                "component": "crawler",
                "message": f"Critical forum crawl error",
                "error": str(e),
                "timestamp": int(time.time())
            })
        finally:
            logger.info(f"Finished forum crawl. Processed {processed_thread_count} threads.")
            for _ in worker_tasks:
                await self.thread_processing_queue.put(None)
            await asyncio.gather(*worker_tasks)
            logger.info("All thread processing workers finished.")

    async def _thread_processor_worker(self, worker_name: str):
        """
        Worker task to process items from the thread processing queue.
        """
        logger.info(f"{worker_name} started.")
        while True:
            thread_data = await self.thread_processing_queue.get()
            if thread_data is None:
                self.thread_processing_queue.task_done()
                logger.info(f"{worker_name} received exit signal and is shutting down.")
                break

            try:
                await self._store_parsed_thread_data(thread_data)
                logger.info(f"{worker_name} successfully processed and stored thread: {thread_data['title']}")
            except Exception as e:
                logger.error(f"{worker_name} failed to store thread data for '{thread_data.get('title', 'N/A')}': {str(e)}", exc_info=True)
                await self.redis.add_error_to_queue({
                    "component": "thread_processor_worker",
                    "message": f"Failed to store thread data for {thread_data.get('title', 'N/A')}",
                    "error": str(e),
                    "timestamp": int(time.time())
                })
            finally:
                self.thread_processing_queue.task_done()

    async def _store_parsed_thread_data(self, data: Dict[str, Any]):
        """
        Stores the parsed thread data into Redis according to the schema.
        - show:{normalized_title}
        - season:{showId}:{season}
        - episode:{season_key}:{ep}
        """
        original_title = data["title"]
        poster_url = data["poster"]
        magnets_metadata = data["raw_magnets_metadata"]
        thread_last_modified = data["last_modified_timestamp"]
        thread_id = data["thread_id"]

        # 1. Normalize show title for show_id
        normalized_show_title = self.normalizer.normalize(original_title)
        if not normalized_show_title:
            logger.warning(f"Could not normalize title for thread {thread_id}: '{original_title}'. Skipping storage.")
            return

        # Prepare show-level metadata
        show_metadata = {
            "title": original_title,
            "poster": poster_url,
            "languages": [],
            "last_updated": thread_last_modified,
            "thread_id": thread_id
        }

        all_episode_languages = set()
        grouped_episodes: Dict[str, Dict[str, Any]] = {}

        for magnet_meta in magnets_metadata:
            magnet_title = magnet_meta.get("title")
            parsed_title = magnet_meta.get("title")
            parsed_season = magnet_meta.get("season")
            parsed_episode_start = magnet_meta.get("episode_start")
            parsed_episode_end = magnet_meta.get("episode_end")
            parsed_languages = magnet_meta.get("languages", [])
            magnet_uri = magnet_meta.get("magnet_uri")

            if not magnet_uri:
                logger.warning(f"Skipping magnet '{magnet_title}' from thread {thread_id} due to missing URI.")
                continue

            if parsed_season is None and parsed_episode_start is None:
                logger.warning(f"No season/episode info for magnet '{magnet_title}'. Storing as S1E1 under show '{original_title}'.")
                parsed_season = 1
                parsed_episode_start = 1
                if parsed_episode_end is None:
                    parsed_episode_end = 1
                show_id_for_episode = normalized_show_title
                episode_title_for_stremio = original_title
            else:
                show_id_for_episode = normalized_show_title
                episode_title_for_stremio = parsed_title or original_title

            if parsed_season is None:
                logger.warning(f"Magnet '{magnet_title}' has episode info but no season. Defaulting to Season 1.")
                parsed_season = 1

            all_episode_languages.update(parsed_languages)

            start_ep = parsed_episode_start
            end_ep = parsed_episode_end if parsed_episode_end is not None else start_ep

            if start_ep is None:
                start_ep = 1
                end_ep = 1
                logger.warning(f"Still no episode number for magnet '{magnet_title}'. Defaulting to Episode 1.")


            for ep_num in range(start_ep, end_ep + 1):
                episode_key = f"{show_id_for_episode}:{parsed_season}:{ep_num}"
                if episode_key not in grouped_episodes:
                    grouped_episodes[episode_key] = {
                        "show_id": show_id_for_episode,
                        "season": parsed_season,
                        "episode": ep_num,
                        "magnets": [],
                        "title": episode_title_for_stremio,
                        "languages": set(),
                        "last_modified": thread_last_modified
                    }
                grouped_episodes[episode_key]["magnets"].append({
                    "uri": magnet_uri,
                    "title": magnet_title,
                    **{k:v for k,v in magnet_meta.items() if k not in ["title", "magnet_uri"]}
                })
                grouped_episodes[episode_key]["languages"].update(parsed_languages)


        show_metadata["languages"] = list(all_episode_languages)
        await self.redis.set_show_metadata(normalized_show_title, show_metadata)
        logger.debug(f"Updated show metadata for '{normalized_show_title}' with poster and languages.")

        for ep_key, ep_data in grouped_episodes.items():
            show_id = ep_data["show_id"]
            season = ep_data["season"]
            episode = ep_data["episode"]

            episode_meta_to_store = {
                "title": ep_data["title"],
                "languages": list(ep_data["languages"]),
                "last_modified": ep_data["last_modified"],
                "magnets": ep_data["magnets"]
            }
            await self.redis.set_episode_metadata(show_id, season, episode, episode_meta_to_store)
            logger.debug(f"Stored episode meta for {show_id} S{season}E{episode}")

            first_magnet = ep_data["magnets"][0] if ep_data["magnets"] else {}
            quality = first_magnet.get("resolution", "unknown")
            lang = first_magnet.get("languages", ["unknown"])[0]
            await self.redis.add_season_entry(show_id, season, thread_id, quality, lang, thread_last_modified)
            logger.debug(f"Added season entry for {show_id} S{season} from thread {thread_id}")

