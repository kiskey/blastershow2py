from aiohttp import web
import json
from loguru import logger
from typing import Dict, Any, List

from db import RedisClient
from normalizer import TitleNormalizer, FuzzyMatcher

class StremioAPI:
    """
    Implements the Stremio Addon API to serve metadata and stream information.
    """
    def __init__(self, redis_client: RedisClient, normalizer: TitleNormalizer, fuzzy_matcher: FuzzyMatcher):
        self.redis = redis_client
        self.normalizer = normalizer
        self.fuzzy_matcher = fuzzy_matcher
        self.app = web.Application()
        self._setup_routes()
        logger.info("Stremio API initialized.")

    def _setup_routes(self):
        """
        Configures the API routes for the Stremio Addon.
        """
        self.app.router.add_get('/manifest.json', self.manifest)
        self.app.router.add_get('/catalog/{type}/{id}.json', self.catalog)
        self.app.router.add_get('/catalog/{type}/{id}/{extra}.json', self.catalog)
        self.app.router.add_get('/meta/{type}/{id}.json', self.meta)
        self.app.router.add_get('/stream/{type}/{id}.json', self.stream)
        # Debug endpoint for listing all show IDs
        self.app.router.add_get('/debug/shows', self.debug_list_shows)
        logger.info("API routes configured.")

    async def manifest(self, request):
        """
        Serves the addon manifest.
        """
        manifest = {
            "id": "org.tamilblasters.stremioaddon",
            "version": "1.0.0",
            "name": "TamilBlasters Addon",
            "description": "Stremio addon for Tamil content from 1tamilmv.boo",
            "resources": ["catalog", "meta", "stream"],
            "types": ["movie", "series"], # Can expand based on content
            "catalogs": [
                {
                    "type": "series", # or "movie"
                    "id": "tamilblasters-webseries",
                    "name": "TamilBlasters Web Series",
                    "extra": [{"name": "search", "isRequired": False}]
                }
            ],
            "idPrefixes": ["tt", "kb"], # Stremio ID prefixes (e.g., IMDB IDs)
            "behaviorHints": {
                "configurable": True,
                "randomize": True
            }
        }
        return web.json_response(manifest)

    async def catalog(self, request):
        """
        Serves the catalog of available content.
        Currently fetches all shows from Redis.
        """
        try:
            catalog_type = request.match_info.get('type')
            catalog_id = request.match_info.get('id')
            extra_query = request.match_info.get('extra', '').split('=', 1) # e.g., search=query

            if catalog_id == "tamilblasters-webseries":
                search_query = None
                if len(extra_query) == 2 and extra_query[0] == "search":
                    search_query = extra_query[1]

                all_show_ids = await self.redis.get_all_show_ids()
                
                metas = []
                for show_id in all_show_ids:
                    show_meta = await self.redis.get_show_metadata(show_id)
                    if show_meta:
                        # Apply search filter if query is present
                        if search_query:
                            best_match_title = self.fuzzy_matcher.find_best_match(search_query, [show_meta["title"]])
                            if not best_match_title:
                                continue # Skip if no good match

                        # Construct meta object for Stremio
                        meta = {
                            "id": f"tb:{show_id}", # Custom ID for Stremio
                            "type": "series", # Or "movie" based on actual content type
                            "name": show_meta["title"],
                            "poster": show_meta.get("poster", "https://placehold.co/200x300/EEE/31343C?text=No+Poster"),
                            "posterShape": "regular",
                            "background": show_meta.get("background", "https://placehold.co/1000x500/EEE/31343C?text=No+Background"),
                            "description": show_meta.get("description", "No description available."),
                            "genres": show_meta.get("genres", ["Tamil", "Web Series"]),
                            "imdb_id": show_meta.get("imdb_id"), # Optional IMDB ID
                            "releaseInfo": show_meta.get("releaseInfo"),
                            "runtime": show_meta.get("runtime"),
                            "year": show_meta.get("year"),
                            "trailer": show_meta.get("trailer")
                        }
                        metas.append(meta)
                
                return web.json_response({"metas": metas})
            
            return web.json_response({"metas": []})

        except Exception as e:
            logger.error(f"Error fetching catalog: {e}", exc_info=True)
            return web.json_response({"metas": []}, status=500)

    async def meta(self, request):
        """
        Serves metadata for a specific content item.
        """
        try:
            item_id = request.match_info.get('id') # e.g., tb:normalized_show_title
            item_type = request.match_info.get('type')

            if item_id.startswith("tb:"):
                normalized_show_id = item_id.split("tb:", 1)[1]
                show_meta = await self.redis.get_show_metadata(normalized_show_id)

                if show_meta:
                    # Fetch all episodes for this show to populate seasons/episodes
                    episodes_data = await self.redis.get_all_episodes_for_show(normalized_show_id)
                    
                    seasons = {} # {season_num: [{"episode": 1, "title": "Ep Title", "overview": "..."}, ...]}
                    for ep_data in episodes_data:
                        season_num = ep_data.get("season", 1)
                        episode_num = ep_data.get("episode", 1)
                        if season_num not in seasons:
                            seasons[season_num] = []
                        
                        seasons[season_num].append({
                            "id": f"tb:{normalized_show_id}:{season_num}:{episode_num}", # Stremio requires unique ID for episode
                            "episode": episode_num,
                            "title": ep_data.get("title", f"Episode {episode_num}"),
                            "overview": ep_data.get("overview", "No overview available."),
                            "released": ep_data.get("last_modified") # Use last_modified as release date
                        })
                    
                    # Sort episodes within each season
                    for s_num in seasons:
                        seasons[s_num].sort(key=lambda x: x["episode"])

                    # Convert seasons dict to sorted list of objects for Stremio format
                    videos = []
                    for s_num in sorted(seasons.keys()):
                        for ep_data in seasons[s_num]:
                            videos.append({
                                "id": ep_data["id"],
                                "title": ep_data["title"],
                                "released": ep_data["released"],
                                "season": s_num,
                                "episode": ep_data["episode"],
                                "overview": ep_data["overview"]
                            })

                    meta = {
                        "id": item_id,
                        "type": item_type,
                        "name": show_meta["title"],
                        "poster": show_meta.get("poster", "https://placehold.co/200x300/EEE/31343C?text=No+Poster"),
                        "posterShape": "regular",
                        "background": show_meta.get("background", "https://placehold.co/1000x500/EEE/31343C?text=No+Background"),
                        "description": show_meta.get("description", "No description available."),
                        "genres": show_meta.get("genres", ["Tamil", "Web Series"]),
                        "imdb_id": show_meta.get("imdb_id"),
                        "releaseInfo": show_meta.get("releaseInfo"),
                        "runtime": show_meta.get("runtime"),
                        "year": show_meta.get("year"),
                        "trailer": show_meta.get("trailer"),
                        "videos": videos # Include all episodes
                    }
                    return web.json_response({"meta": meta})
            
            return web.json_response({"meta": {}}) # Return empty meta if not found

        except Exception as e:
            logger.error(f"Error fetching meta for {item_id}: {e}", exc_info=True)
            return web.json_response({"meta": {}}, status=500)


    async def stream(self, request):
        """
        Serves stream information (magnet links) for a specific episode.
        """
        try:
            item_id = request.match_info.get('id') # Expected format: tb:{show_id}:{season_num}:{episode_num}
            item_type = request.match_info.get('type')

            # Parse the item_id
            parts = item_id.split(":")
            if len(parts) == 4 and parts[0] == "tb":
                _prefix, show_id, season_num_str, episode_num_str = parts
                season_num = int(season_num_str)
                episode_num = int(episode_num_str)

                episode_meta = await self.redis.get_episode_metadata(show_id, season_num, episode_num)
                
                streams = []
                if episode_meta and episode_meta.get("magnets"):
                    trackers = await self.redis.get_trackers() # Get cached trackers

                    for magnet_info in episode_meta["magnets"]:
                        magnet_uri = magnet_info.get("uri")
                        magnet_title = magnet_info.get("title", "Unknown Quality")
                        
                        if magnet_uri:
                            # Append trackers to magnet URI for better discovery
                            if trackers:
                                # Ensure trackers are properly URL-encoded and appended
                                for tracker_url in trackers:
                                    magnet_uri += f"&tr={tracker_url}"

                            streams.append({
                                "name": magnet_title,
                                "description": f"S{season_num}E{episode_num} - {magnet_title}",
                                "infoHash": magnet_info.get("infoHash"), # Add infoHash if available
                                "sources": [magnet_uri], # Magnet URI is the primary source
                                "url": magnet_uri # Stremio can also use 'url' for magnet links
                            })
                
                return web.json_response({"streams": streams})
            
            return web.json_response({"streams": []}) # Return empty streams if ID is invalid

        except Exception as e:
            logger.error(f"Error fetching stream for {item_id}: {e}", exc_info=True)
            return web.json_response({"streams": []}, status=500)

    async def debug_list_shows(self, request):
        """
        Debug endpoint to list all show_ids (normalized titles) currently stored in Redis.
        """
        try:
            show_ids = await self.redis.get_all_show_ids()
            return web.json_response({"show_ids": show_ids, "count": len(show_ids)})
        except Exception as e:
            logger.error(f"Error fetching debug shows: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

