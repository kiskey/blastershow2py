import asyncio
import time
from aiohttp import web
from typing import Dict, Any, List, Optional
import re
from urllib.parse import urlparse, parse_qs
import hashlib # For BTIH validation
import struct # For BTIH validation from magnet URI
from binascii import unhexlify

from logger import logger
from config import settings
from db import RedisClient
from stremio_models import (
    StremioManifest, StremioCatalog, StremioMetaPreview,
    StremioMeta, StremioEpisode, StremioStreams, StremioStream
)
from normalizer import TitleNormalizer, FuzzyMatcher

class StremioAPI:
    """
    Aiohttp web server for the Stremio Addon API.
    Exposes catalog, meta, stream, search, health, and debug endpoints.
    """
    def __init__(self, redis_client: RedisClient, normalizer: TitleNormalizer, fuzzy_matcher: FuzzyMatcher):
        self.redis = redis_client
        self.normalizer = normalizer
        self.fuzzy_matcher = fuzzy_matcher
        self.app = web.Application()
        self._setup_routes()
        logger.info("Stremio API initialized.")

    def _setup_routes(self):
        """Configures the API routes."""
        self.app.router.add_get("/", self.manifest_handler)
        self.app.router.add_get("/manifest.json", self.manifest_handler)
        self.app.router.add_get("/catalog/{type}/{id}.json", self.catalog_handler)
        self.app.router.add_get("/catalog/{type}/{id}/skip={skip}.json", self.catalog_handler) # Pagination
        self.app.router.add_get("/meta/{type}/{id}.json", self.meta_handler)
        self.app.router.add_get("/stream/{type}/{id}:{season}:{episode}.json", self.stream_handler)
        self.app.router.add_get("/search/{type}/{id}.json", self.search_handler) # Stremio search convention
        self.app.router.add_get("/search", self.search_handler) # Alternative for direct queries
        self.app.router.add_get("/health", self.health_handler)
        self.app.router.add_get("/debug/streams/{show_id}", self.debug_streams_handler)
        self.app.router.add_get("/debug/redis/{key}", self.debug_redis_handler)
        logger.info("API routes configured.")

    async def manifest_handler(self, request: web.Request) -> web.Response:
        """Handles the /manifest.json request."""
        manifest = StremioManifest().model_dump(by_alias=True, exclude_none=True)
        return web.json_response(manifest)

    async def catalog_handler(self, request: web.Request) -> web.Response:
        """Handles /catalog/series/tamil-web.json requests."""
        catalog_type = request.match_info.get("type")
        catalog_id = request.match_info.get("id")
        skip = int(request.match_info.get("skip", 0))

        if catalog_type != "series" or catalog_id != "tamil-web":
            return web.json_response({"metas": []})

        logger.info(f"Catalog request for type={catalog_type}, id={catalog_id}, skip={skip}")
        all_shows_raw = await self.redis.get_all_shows()

        # Sort alphabetically by title
        all_shows_raw.sort(key=lambda x: x.get("title", "").lower())

        # Implement pagination
        page_size = 20 # Stremio default page size for catalogs
        paginated_shows = all_shows_raw[skip : skip + page_size]

        metas: List[StremioMetaPreview] = []
        for show_data in paginated_shows:
            try:
                # Ensure show_id is always present and correctly derived
                show_id = show_data.get("id") or self.normalizer.normalize(show_data.get("title", ""))
                if not show_id:
                    logger.warning(f"Skipping catalog entry due to missing show_id or title: {show_data}")
                    continue

                metas.append(StremioMetaPreview(
                    id=f"ttb_{show_id}", # Prefix to ensure uniqueness
                    type="series",
                    name=show_data.get("title", "Unknown Title"),
                    poster=show_data.get("poster", "https://placehold.co/200x300/1E90FF/FFFFFF?text=No+Poster"),
                    releaseInfo=f"Last updated: {time.strftime('%Y-%m-%d', time.localtime(show_data.get('last_updated', 0)))}",
                    genres=show_data.get("languages", ["Tamil"]), # Use languages as genres
                    description=f"Languages: {', '.join(show_data.get('languages', ['N/A']))}" # More details
                ))
            except Exception as e:
                logger.error(f"Error creating StremioMetaPreview for show: {show_data.get('title', 'N/A')}: {e}", exc_info=True)
                await self.redis.add_error_to_queue({
                    "component": "api_catalog",
                    "message": f"Error formatting catalog entry for {show_data.get('title', 'N/A')}",
                    "error": str(e),
                    "timestamp": int(time.time())
                })
                continue


        catalog_response = StremioCatalog(metas=metas).model_dump(by_alias=True, exclude_none=True)
        return web.json_response(catalog_response)

    async def meta_handler(self, request: web.Request) -> web.Response:
        """Handles /meta/series/{show_id}.json requests."""
        media_type = request.match_info.get("type")
        stremio_show_id = request.match_info.get("id")

        if media_type != "series" or not stremio_show_id.startswith("ttb_"):
            return web.json_response({}) # Return empty if type/id mismatch

        show_id = stremio_show_id.replace("ttb_", "")
        logger.info(f"Meta request for show_id={show_id}")

        show_data = await self.redis.get_show_metadata(show_id)
        if not show_data:
            logger.warning(f"Show metadata not found for ID: {show_id}")
            return web.json_response({})

        # Get all season entries to determine available seasons and episodes
        all_season_keys = await self.redis.client.keys(f"season:{show_id}:*")
        available_seasons = set()
        episode_promises = []

        for season_key_bytes in all_season_keys:
            season_key = season_key_bytes.decode('utf-8')
            # Extract season number from key: season:{showId}:{season}
            match = re.match(r"season:.*:(\d+)", season_key)
            if match:
                season_num = int(match.group(1))
                available_seasons.add(season_num)
                # For each season, we need to fetch all episodes that belong to it
                # We can fetch episode metadata using keys like episode:{show_id}:{season}:{episode}
                # For meta request, Stremio expects 'videos' array containing all episodes.
                # It doesn't fetch individual episodes, but displays them from this 'videos' list.
                # So we iterate all episodes and combine them.
                episode_keys = await self.redis.client.keys(f"episode:{show_id}:{season_num}:*")
                for ep_key_bytes in episode_keys:
                    ep_key = ep_key_bytes.decode('utf-8')
                    ep_match = re.match(r"episode:.*:\d+:(\d+)", ep_key)
                    if ep_match:
                        episode_num = int(ep_match.group(1))
                        episode_promises.append(self.redis.get_episode_metadata(show_id, season_num, episode_num))

        all_episodes_data_raw = await asyncio.gather(*episode_promises)
        videos: List[StremioEpisode] = []
        for ep_data in all_episodes_data_raw:
            if ep_data:
                # Each episode entry might have multiple magnets, but for Stremio Meta, we just need title/season/episode
                videos.append(StremioEpisode(
                    season=ep_data["season"],
                    episode=ep_data["episode"],
                    title=ep_data["title"],
                    overview=f"Available in languages: {', '.join(ep_data.get('languages', ['N/A']))}",
                    aired=time.strftime('%Y-%m-%d', time.localtime(ep_data.get('last_modified', 0))) # Use last modified as aired date
                ))
        # Sort episodes by season then episode number
        videos.sort(key=lambda x: (x.season, x.episode))

        meta = StremioMeta(
            id=stremio_show_id,
            type="series",
            name=show_data.get("title", "Unknown Title"),
            poster=show_data.get("poster", "https://placehold.co/200x300/1E90FF/FFFFFF?text=No+Poster"),
            background=show_data.get("poster", "https://placehold.co/1280x720/1E90FF/FFFFFF?text=No+Background"),
            description=f"Languages: {', '.join(show_data.get('languages', ['N/A']))}",
            releaseInfo=f"Last updated: {time.strftime('%Y-%m-%d', time.localtime(show_data.get('last_updated', 0)))}",
            genres=show_data.get("languages", ["Tamil"]), # Using languages as genres
            videos=videos,
            status="Continuing" # Assuming continuing unless we can detect otherwise
        ).model_dump(by_alias=True, exclude_none=True)
        return web.json_response(meta)

    async def stream_handler(self, request: web.Request) -> web.Response:
        """Handles /stream/series/{show_id}:{season}:{episode}.json requests."""
        media_type = request.match_info.get("type")
        stremio_show_id = request.match_info.get("id")
        season = int(request.match_info.get("season"))
        episode = int(request.match_info.get("episode"))

        if media_type != "series" or not stremio_show_id.startswith("ttb_"):
            return web.json_response({"streams": []})

        show_id = stremio_show_id.replace("ttb_", "")
        logger.info(f"Stream request for show_id={show_id}, S{season}E{episode}")

        episode_data = await self.redis.get_episode_metadata(show_id, season, episode)
        if not episode_data or not episode_data.get("magnets"):
            logger.warning(f"No stream data found for {show_id} S{season}E{episode}")
            return web.json_response({"streams": []})

        trackers = await self.redis.get_trackers()
        streams: List[StremioStream] = []

        for magnet_info in episode_data["magnets"]:
            magnet_uri = magnet_info["uri"]
            magnet_title = magnet_info["title"]

            # Add trackers to magnet URI
            if trackers:
                # Ensure there are no duplicate trackers already in the URI
                parsed_magnet = urlparse(magnet_uri)
                query_params = parse_qs(parsed_magnet.query)
                existing_trackers = set(query_params.get('tr', []))

                new_trackers_to_add = [tr for tr in trackers if tr not in existing_trackers]
                if new_trackers_to_add:
                    # Append new trackers, ensuring proper encoding if needed
                    # For simplicity, if magnet URI doesn't have existing 'tr' params, just append
                    if 'tr' not in query_params:
                        magnet_uri += '&tr=' + '&tr='.join([t.replace('&', '%26') for t in new_trackers_to_add])
                    else:
                        # If existing trackers, add to current query
                        existing_tr_str = query_params['tr'][0] if query_params['tr'] else ''
                        updated_tr_str = existing_tr_str + '&tr=' + '&tr='.join([t.replace('&', '%26') for t in new_trackers_to_add])
                        magnet_uri = re.sub(r'([?&]tr=)[^&]+', r'\1' + updated_tr_str, magnet_uri, count=1)
                        if 'tr' not in magnet_uri: # Fallback if regex failed to replace
                            magnet_uri += '&tr=' + '&tr='.join([t.replace('&', '%26') for t in new_trackers_to_add])

            # Optional: BTIH validation (basic check)
            info_hash = self._extract_btih_from_magnet(magnet_uri)
            if not info_hash:
                logger.warning(f"Invalid BTIH for magnet: {magnet_uri}. Skipping stream.")
                continue

            streams.append(StremioStream(
                name=f"{magnet_title} ({magnet_info.get('resolution', 'N/A')} / {', '.join(magnet_info.get('languages', ['N/A']))})",
                magnet=magnet_uri,
                infoHash=info_hash,
                title=f"{magnet_info.get('resolution', 'N/A')} - {', '.join(magnet_info.get('languages', ['N/A']))}",
                description=f"Release: {magnet_info.get('release_group', 'N/A')}, Size: {magnet_info.get('size', 'N/A')}",
                # You can add behaviorHints for external player:
                # behaviorHints={"filename": f"{episode_data['title']} S{season}E{episode}.{magnet_info.get('file_extension', 'mp4')}"}
            ))

        streams_response = StremioStreams(streams=streams).model_dump(by_alias=True, exclude_none=True)
        return web.json_response(streams_response)

    async def search_handler(self, request: web.Request) -> web.Response:
        """Handles /search?q=... and /search/{type}/{id}.json for fuzzy search."""
        query = request.query.get("q")
        if not query:
            # For Stremio's search endpoint with /search/{type}/{id}.json, 'id' is the query
            search_id = request.match_info.get("id")
            if search_id:
                # Stremio addon SDK transforms the search query into `id` in the URL path.
                # It decodes the id like `https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Dtest`
                # So we need to decode the URL-encoded search_id to get the actual query.
                try:
                    # Attempt to extract 'q' parameter if the id resembles a URL-encoded query
                    parsed_id_url = urlparse(search_id)
                    query_params_from_id = parse_qs(parsed_id_url.query)
                    if 'q' in query_params_from_id:
                        query = query_params_from_id['q'][0]
                    else:
                        query = search_id # Fallback if it's just a direct query string
                except Exception as e:
                    logger.warning(f"Could not parse search ID '{search_id}' as URL: {e}. Using as direct query.")
                    query = search_id
            else:
                return web.json_response({"metas": []})

        logger.info(f"Search request for query: {query}")

        all_shows_raw = await self.redis.get_all_shows()
        all_show_titles = [show.get("title") for show in all_shows_raw if show.get("title")]

        matched_title = self.fuzzy_matcher.find_best_match(query, all_show_titles)

        metas: List[StremioMetaPreview] = []
        if matched_title:
            matched_show_data = next((show for show in all_shows_raw if show.get("title") == matched_title), None)
            if matched_show_data:
                show_id = matched_show_data.get("id") or self.normalizer.normalize(matched_show_data.get("title", ""))
                try:
                    metas.append(StremioMetaPreview(
                        id=f"ttb_{show_id}",
                        type="series",
                        name=matched_show_data.get("title", "Unknown Title"),
                        poster=matched_show_data.get("poster", "https://placehold.co/200x300/1E90FF/FFFFFF?text=No+Poster"),
                        releaseInfo=f"Last updated: {time.strftime('%Y-%m-%d', time.localtime(matched_show_data.get('last_updated', 0)))}",
                        genres=matched_show_data.get("languages", ["Tamil"]),
                        description=f"Languages: {', '.join(matched_show_data.get('languages', ['N/A']))}"
                    ))
                except Exception as e:
                    logger.error(f"Error creating StremioMetaPreview for search result {matched_title}: {e}", exc_info=True)
                    await self.redis.add_error_to_queue({
                        "component": "api_search",
                        "message": f"Error formatting search result entry for {matched_title}",
                        "error": str(e),
                        "timestamp": int(time.time())
                    })

        search_response = StremioCatalog(metas=metas).model_dump(by_alias=True, exclude_none=True)
        return web.json_response(search_response)

    async def health_handler(self, request: web.Request) -> web.Response:
        """Handles /health requests."""
        try:
            await self.redis.client.ping()
            status = {"status": "ok", "redis_status": "connected"}
        except Exception as e:
            logger.error(f"Health check failed: Redis not connected: {e}", exc_info=True)
            status = {"status": "error", "redis_status": f"disconnected: {e}"}
            await self.redis.add_error_to_queue({
                "component": "health_check",
                "message": "Health check failed (Redis)",
                "error": str(e),
                "timestamp": int(time.time())
            })
        return web.json_response(status)

    async def debug_streams_handler(self, request: web.Request) -> web.Response:
        """Handles /debug/streams/{show_id} to show all available magnets."""
        stremio_show_id = request.match_info.get("show_id")
        if not stremio_show_id.startswith("ttb_"):
            return web.json_response({"error": "Invalid show ID format, must start with ttb_"}, status=400)
        show_id = stremio_show_id.replace("ttb_", "")
        logger.info(f"Debug stream request for show ID: {show_id}")

        all_episode_keys = await self.redis.client.keys(f"episode:{show_id}:*")
        all_streams_data: Dict[str, Any] = {"show_id": show_id, "episodes": {}}

        episode_fetch_tasks = []
        for ep_key_bytes in all_episode_keys:
            ep_key = ep_key_bytes.decode('utf-8')
            parts = ep_key.split(':')
            if len(parts) == 4: # episode:{show_id}:{season}:{episode}
                try:
                    season = int(parts[2])
                    episode = int(parts[3])
                    episode_fetch_tasks.append(self.redis.get_episode_metadata(show_id, season, episode))
                except ValueError:
                    logger.warning(f"Malformed episode key found: {ep_key}")
                    continue

        fetched_episodes = await asyncio.gather(*episode_fetch_tasks)

        for ep_data in fetched_episodes:
            if ep_data:
                season_ep_key = f"S{ep_data['season']}E{ep_data['episode']}"
                all_streams_data["episodes"][season_ep_key] = {
                    "title": ep_data["title"],
                    "languages": ep_data["languages"],
                    "magnets": ep_data.get("magnets", [])
                }
        return web.json_response(all_streams_data, dumps=self._json_dumps_orjson)


    async def debug_redis_handler(self, request: web.Request) -> web.Response:
        """Handles /debug/redis/{key} to dump Redis key value."""
        key = request.match_info.get("key")
        logger.info(f"Debug Redis key dump request for key: {key}")
        dump_content = await self.redis.get_redis_key_dump(key)
        if dump_content:
            return web.Response(text=dump_content, content_type="text/plain")
        else:
            return web.json_response({"error": f"Key '{key}' not found or could not be dumped."}, status=404)

    def _json_dumps_orjson(self, data: Any) -> str:
        """Custom JSON dumps function using orjson for aiohttp."""
        return self.redis.orjson.dumps(data, option=self.redis.orjson.OPT_INDENT_2).decode('utf-8')

    def _extract_btih_from_magnet(self, magnet_uri: str) -> Optional[str]:
        """
        Extracts BTIH (info_hash) from a magnet URI.
        Performs basic validation that it's a 40-char hex string.
        """
        parsed_uri = urlparse(magnet_uri)
        query_params = parse_qs(parsed_uri.query)
        xt_param = query_params.get("xt", [])

        for param in xt_param:
            if param.startswith("urn:btih:"):
                btih = param.replace("urn:btih:", "")
                # Validate BTIH format (40 hex characters)
                if len(btih) == 40 and re.fullmatch(r"^[0-9a-fA-F]{40}$", btih):
                    return btih.lower() # Return in lowercase for consistency
                # Handle base32 encoded info_hash if present, though less common for torrents
                elif len(btih) == 32 and re.fullmatch(r"^[2-7A-Z]{32}$", btih, re.IGNORECASE):
                    try:
                        # Convert base32 to hex
                        return unhexlify(base64.b32decode(btih.upper())).hex()
                    except Exception:
                        logger.warning(f"Could not decode base32 BTIH: {btih}")
                        return None
        return None

