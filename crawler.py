import asyncio
import re
from urllib.parse import urljoin, urlparse
import time

from logger import logger
from config import settings
from db import RedisClient
from utils.http_client import HttpClient
from utils.bloom_filter import BloomFilter
from parser import ThreadParser # Assuming parser is in the same directory
from metadata_parser import MetadataParser # Assuming metadata_parser is in the same directory
from normalizer import TitleNormalizer # Assuming normalizer is in the same directory

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
        self.base_url = str(settings.FORUM_URL)
        self.forum_path = "/index.php?/forums/forum/63-tamil-new-web-series-tv-shows/"
        self.thread_url_pattern = re.compile(r"/forums/topic/(\d+)-") # Matches /forums/topic/{id}-
        self.thread_processing_queue = asyncio.Queue()
        self.crawl_bloom_filter = BloomFilter(
            redis_client.client, "bloomfilter:urls",
            capacity=100000, error_rate=0.01 # Max 100k URLs, 1% false positive
        )
        self.http_client.bloom_filter = self.crawl_bloom_filter # Inject bloom filter into http_client
        logger.info(f"Crawler initialized for forum: {self.base_url}{self.forum_path}")

    async def _get_thread_links_from_page(self, html_content: str) -> List[Dict[str, str]]:
        """
        Extracts valid thread links from a forum page's HTML content.
        Parses `<a class="" data-ipshover>` links for threads.
        """
        soup = self.thread_parser.BeautifulSoup(html_content, 'html.parser')
        thread_links = []
        # Find all <a> tags that are likely thread links.
        # This selector targets links typically found in thread lists.
        # It's an approximation, adjust if the forum's HTML changes.
        for a_tag in soup.find_all("a", class_="ipsType_break ipsContained"):
            href = a_tag.get("href")
            # Ensure it's a full URL or join it
            if href and not href.startswith("http"):
                href = urljoin(self.base_url, href)

            if href and self.thread_url_pattern.search(href):
                # Extract thread ID from the URL using the compiled regex
                match = self.thread_url_pattern.search(href)
                thread_id = match.group(1) if match else None
                if thread_id:
                    thread_links.append({"url": href, "id": thread_id})
                    logger.debug(f"Found thread link: {href} (ID: {thread_id})")
        return thread_links

    async def _process_thread(self, thread_url: str, thread_id: str):
        """
        Fetches and processes a single thread page.
        """
        try:
            html = await self.http_client.get(thread_url)
            if html:
                parsed_data = self.thread_parser.parse_thread_page(html, self.base_url)
                if parsed_data and parsed_data["title"]:
                    # Ensure thread_id is stored correctly as string
                    parsed_data["thread_id"] = thread_id
                    await self.thread_processing_queue.put(parsed_data)
                    logger.info(f"Enqueued thread '{parsed_data['title']}' (ID: {thread_id}) for processing.")
                else:
                    logger.warning(f"Failed to parse essential data from thread: {thread_url}")
            else:
                logger.warning(f"Failed to fetch content for thread: {thread_url}")
        except Exception as e:
            logger.error(f"Error processing thread {thread_url}: {e}", exc_info=True)
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

        # Start worker tasks for thread processing
        worker_tasks = [
            asyncio.create_task(self._thread_processor_worker(f"Worker-{i+1}"))
            for i in range(settings.MAX_CONCURRENCY)
        ]

        try:
            while page_num <= initial_pages:
                page_url = urljoin(self.base_url, f"{self.forum_path}page/{page_num}/")
                logger.info(f"Crawling forum page: {page_url}")

                page_html = await self.http_client.get(page_url, force_fetch=True) # Always fetch page HTML
                if not page_html:
                    logger.info(f"Reached end of forum pagination at page {page_num} (no content or 404).")
                    break

                thread_links = await self._get_thread_links_from_page(page_html)

                tasks = []
                for thread in thread_links:
                    thread_id = thread["id"]
                    thread_url = thread["url"]

                    last_visited = await self.redis.get_thread_last_visited(thread_id)

                    # Only process if new or older than revisit interval
                    if not last_visited or last_visited < revisit_threshold:
                        tasks.append(self._process_thread(thread_url, thread_id))
                        await self.redis.set_thread_last_visited(thread_id, current_time) # Update visit time immediately
                        processed_thread_count += 1
                    else:
                        logger.debug(f"Skipping recently visited thread: {thread_url} (ID: {thread_id})")

                if tasks:
                    await asyncio.gather(*tasks) # Run processing tasks for this page concurrently
                else:
                    logger.info(f"No new or outdated threads found on page {page_num}.")

                page_num += 1

        except Exception as e:
            logger.error(f"Critical error during forum crawling: {e}", exc_info=True)
            await self.redis.add_error_to_queue({
                "component": "crawler",
                "message": f"Critical forum crawl error",
                "error": str(e),
                "timestamp": int(time.time())
            })
        finally:
            logger.info(f"Finished forum crawl. Processed {processed_thread_count} threads.")
            # Signal workers to exit by putting None for each worker
            for _ in worker_tasks:
                await self.thread_processing_queue.put(None)
            # Wait for all workers to finish
            await asyncio.gather(*worker_tasks)
            logger.info("All thread processing workers finished.")

    async def _thread_processor_worker(self, worker_name: str):
        """
        Worker task to process items from the thread processing queue.
        """
        logger.info(f"{worker_name} started.")
        while True:
            thread_data = await self.thread_processing_queue.get()
            if thread_data is None: # Signal to exit
                self.thread_processing_queue.task_done()
                logger.info(f"{worker_name} received exit signal and is shutting down.")
                break

            try:
                await self._store_parsed_thread_data(thread_data)
                logger.info(f"{worker_name} successfully processed and stored thread: {thread_data['title']}")
            except Exception as e:
                logger.error(f"{worker_name} failed to store thread data for '{thread_data.get('title', 'N/A')}': {e}", exc_info=True)
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
            "languages": [], # Will be aggregated from episodes
            "last_updated": thread_last_modified,
            "thread_id": thread_id # Store the originating thread ID for show
        }

        # Collect all languages from episodes
        all_episode_languages = set()

        # Group magnets by show_id, season, episode for processing
        grouped_episodes: Dict[str, Dict[str, Any]] = {} # Key: f"{season_id}_{episode_id}"

        for magnet_meta in magnets_metadata:
            magnet_title = magnet_meta.get("title")
            parsed_title = magnet_meta.get("title") # This is the full parsed title from guessit/regex, not necessarily the original show title
            parsed_season = magnet_meta.get("season")
            parsed_episode_start = magnet_meta.get("episode_start")
            parsed_episode_end = magnet_meta.get("episode_end") # Handle episode ranges
            parsed_languages = magnet_meta.get("languages", [])
            magnet_uri = magnet_meta.get("magnet_uri")

            if not magnet_uri:
                logger.warning(f"Skipping magnet '{magnet_title}' from thread {thread_id} due to missing URI.")
                continue

            # If season/episode are missing, it might be a movie or a misparsed series episode
            # We'll try to infer or handle as a generic "show"
            if parsed_season is None and parsed_episode_start is None:
                # Treat as Season 1, Episode 1 if no specific season/episode info,
                # and if the thread title indicates it's a series.
                # Or could try to derive from original_title if it's a movie/single episode.
                # For simplicity, if guessit didn't find S/E, we treat it generally for now,
                # but might need more robust inference.
                logger.warning(f"No season/episode info for magnet '{magnet_title}'. Storing as S1E1 under show '{original_title}'.")
                parsed_season = 1
                parsed_episode_start = 1
                if parsed_episode_end is None:
                    parsed_episode_end = 1
                # If parsed_title from magnet_meta is different from original_title,
                # it's likely a specific episode title, not the show title.
                # So we should use original_title as the show_id.
                show_id_for_episode = normalized_show_title
                episode_title_for_stremio = original_title # Use show title as episode title fallback
            else:
                show_id_for_episode = normalized_show_title # Always use the normalized main title as show ID
                episode_title_for_stremio = parsed_title or original_title # Use parsed title if available, else original

            if parsed_season is None:
                logger.warning(f"Magnet '{magnet_title}' has episode info but no season. Defaulting to Season 1.")
                parsed_season = 1

            # Populate all_episode_languages for show metadata
            all_episode_languages.update(parsed_languages)

            # Process episodes, handling ranges
            start_ep = parsed_episode_start
            end_ep = parsed_episode_end if parsed_episode_end is not None else start_ep

            if start_ep is None: # If still no episode, just store under S1E1 for now
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
                        "title": episode_title_for_stremio, # Title for this specific episode meta
                        "languages": set(), # Aggregated languages for this specific episode
                        "last_modified": thread_last_modified
                    }
                grouped_episodes[episode_key]["magnets"].append({
                    "uri": magnet_uri,
                    "title": magnet_title, # The original magnet display title
                    **{k:v for k,v in magnet_meta.items() if k not in ["title", "magnet_uri"]} # Add other parsed fields
                })
                grouped_episodes[episode_key]["languages"].update(parsed_languages)


        # Store show-level metadata (poster, aggregated languages)
        show_metadata["languages"] = list(all_episode_languages)
        await self.redis.set_show_metadata(normalized_show_title, show_metadata)
        logger.debug(f"Updated show metadata for '{normalized_show_title}' with poster and languages.")

        # Store episode-level metadata and season entries
        for ep_key, ep_data in grouped_episodes.items():
            show_id = ep_data["show_id"]
            season = ep_data["season"]
            episode = ep_data["episode"]

            # Store episode metadata
            episode_meta_to_store = {
                "title": ep_data["title"],
                "languages": list(ep_data["languages"]),
                "last_modified": ep_data["last_modified"],
                "magnets": ep_data["magnets"]
            }
            await self.redis.set_episode_metadata(show_id, season, episode, episode_meta_to_store)
            logger.debug(f"Stored episode meta for {show_id} S{season}E{episode}")

            # Add entry to season ZSET
            # Use a representative magnet's quality and first language for the ZSET entry
            first_magnet = ep_data["magnets"][0] if ep_data["magnets"] else {}
            quality = first_magnet.get("resolution", "unknown")
            lang = first_magnet.get("languages", ["unknown"])[0]
            await self.redis.add_season_entry(show_id, season, thread_id, quality, lang, thread_last_modified)
            logger.debug(f"Added season entry for {show_id} S{season} from thread {thread_id}")

