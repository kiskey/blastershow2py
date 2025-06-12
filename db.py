import redis.asyncio as redis
import time
import json
from typing import Dict, Any, List, Optional
from loguru import logger
from config import settings

class RedisClient:
    """
    Handles all Redis interactions for the Stremio Addon.
    Manages connections, and stores/retrieves show, season, and episode metadata.
    """
    def __init__(self):
        self.client = None
        self.redis_url = settings.REDIS_URL
        logger.info(f"Redis client initialized for URL: {self.redis_url}")

    async def connect(self):
        """
        Establishes a connection to the Redis server.
        """
        try:
            self.client = await redis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
            # Ping to verify connection
            await self.client.ping()
            logger.info("Successfully connected to Redis.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
            raise

    async def disconnect(self):
        """
        Closes the connection to the Redis server.
        """
        if self.client:
            await self.client.close()
            logger.info("Disconnected from Redis.")

    async def set_show_metadata(self, show_id: str, metadata: Dict[str, Any]):
        """
        Stores show-level metadata in a Redis HASH.
        Key format: show:{show_id}
        """
        try:
            key = f"show:{show_id}"
            # Ensure complex types like sets/lists are JSON serialized if needed
            # For this metadata, languages is a list, which redis-py handles fine
            await self.client.hset(key, mapping=metadata)
            logger.debug(f"Stored show metadata for '{show_id}'")
        except Exception as e:
            logger.error(f"Error storing show metadata for '{show_id}': {e}", exc_info=True)
            await self.add_error_to_queue({
                "component": "redis",
                "message": f"Failed to set show metadata for {show_id}",
                "error": str(e),
                "timestamp": int(time.time())
            })

    async def get_show_metadata(self, show_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves show-level metadata from Redis.
        """
        try:
            key = f"show:{show_id}"
            data = await self.client.hgetall(key)
            if data:
                # Convert list back from string if necessary (e.g., 'languages')
                if 'languages' in data and isinstance(data['languages'], str):
                    data['languages'] = json.loads(data['languages'])
                return data
            return None
        except Exception as e:
            logger.error(f"Error retrieving show metadata for '{show_id}': {e}", exc_info=True)
            return None

    async def add_season_entry(self, show_id: str, season_num: int, thread_id: str,
                               quality: str, lang: str, timestamp: int):
        """
        Adds/updates an entry in a Redis ZSET for a season.
        Key format: season:{show_id}:{season_num}
        Score: timestamp
        Member: JSON string of relevant details (thread_id, quality, lang)
        """
        try:
            key = f"season:{show_id}:{season_num}"
            member_data = {
                "thread_id": thread_id,
                "quality": quality,
                "lang": lang
            }
            member = json.dumps(member_data)
            await self.client.zadd(key, {member: timestamp})
            logger.debug(f"Added season entry for {show_id} S{season_num} (Thread ID: {thread_id})")
        except Exception as e:
            logger.error(f"Error adding season entry for {show_id} S{season_num}: {e}", exc_info=True)
            await self.add_error_to_queue({
                "component": "redis",
                "message": f"Failed to add season entry for {show_id} S{season_num}",
                "error": str(e),
                "timestamp": int(time.time())
            })

    async def get_season_entries(self, show_id: str, season_num: int) -> List[Dict[str, Any]]:
        """
        Retrieves all entries for a specific season from Redis ZSET.
        """
        try:
            key = f"season:{show_id}:{season_num}"
            # Retrieve all members from the ZSET in ascending order of score
            members_with_scores = await self.client.zrange(key, 0, -1, withscores=True)
            entries = []
            for member, score in members_with_scores:
                data = json.loads(member)
                data['timestamp'] = int(score) # Convert score to int timestamp
                entries.append(data)
            return entries
        except Exception as e:
            logger.error(f"Error retrieving season entries for {show_id} S{season_num}: {e}", exc_info=True)
            return []
    
    async def set_episode_metadata(self, show_id: str, season_num: int, episode_num: int, metadata: Dict[str, Any]):
        """
        Stores episode-level metadata in a Redis HASH.
        Key format: episode:{show_id}:{season_num}:{episode_num}
        """
        try:
            key = f"episode:{show_id}:{season_num}:{episode_num}"
            # Ensure complex types like lists within metadata are JSON serialized
            # before storing in Redis Hash, as redis-py HASH values are strings.
            processed_metadata = {k: json.dumps(v) if isinstance(v, (list, dict)) else v for k, v in metadata.items()}
            await self.client.hset(key, mapping=processed_metadata)
            logger.debug(f"Stored episode metadata for '{show_id}' S{season_num}E{episode_num}")
        except Exception as e:
            logger.error(f"Error storing episode metadata for '{show_id}' S{season_num}E{episode_num}: {e}", exc_info=True)
            await self.add_error_to_queue({
                "component": "redis",
                "message": f"Failed to set episode metadata for {show_id} S{season_num}E{episode_num}",
                "error": str(e),
                "timestamp": int(time.time())
            })

    async def get_episode_metadata(self, show_id: str, season_num: int, episode_num: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves episode-level metadata from Redis.
        """
        try:
            key = f"episode:{show_id}:{season_num}:{episode_num}"
            data = await self.client.hgetall(key)
            if data:
                # Deserialize JSON strings back into Python objects
                for k, v in data.items():
                    try:
                        data[k] = json.loads(v)
                    except (json.JSONDecodeError, TypeError):
                        pass # Not a JSON string, keep as is
                return data
            return None
        except Exception as e:
            logger.error(f"Error retrieving episode metadata for '{show_id}' S{season_num}E{episode_num}: {e}", exc_info=True)
            return None

    async def get_thread_last_visited(self, thread_id: str) -> Optional[int]:
        """
        Retrieves the last visited timestamp for a given thread.
        """
        try:
            key = f"thread:last_visited:{thread_id}"
            timestamp_str = await self.client.get(key)
            return int(timestamp_str) if timestamp_str else None
        except Exception as e:
            logger.error(f"Error getting last visited for thread {thread_id}: {e}", exc_info=True)
            return None

    async def set_thread_last_visited(self, thread_id: str, timestamp: int):
        """
        Sets the last visited timestamp for a given thread.
        """
        try:
            key = f"thread:last_visited:{thread_id}"
            await self.client.set(key, timestamp)
            logger.debug(f"Set last visited for thread {thread_id} to {timestamp}")
        except Exception as e:
            logger.error(f"Error setting last visited for thread {thread_id}: {e}", exc_info=True)
            await self.add_error_to_queue({
                "component": "redis",
                "message": f"Failed to set last visited for thread {thread_id}",
                "error": str(e),
                "timestamp": int(time.time())
            })

    async def purge_all_data(self):
        """
        Deletes all keys managed by this application in Redis.
        Use with caution!
        """
        try:
            # Get all keys matching our application's prefixes
            keys_to_delete = []
            async for key in self.client.scan_iter("show:*"):
                keys_to_delete.append(key)
            async for key in self.client.scan_iter("season:*"):
                keys_to_delete.append(key)
            async for key in self.client.scan_iter("episode:*"):
                keys_to_delete.append(key)
            async for key in self.client.scan_iter("thread:last_visited:*"):
                keys_to_delete.append(key)
            async for key in self.client.scan_iter("bloomfilter:*"):
                keys_to_delete.append(key)
            async for key in self.client.scan_iter("error_queue"):
                keys_to_delete.append(key)
            async for key in self.client.scan_iter("trackers:*"):
                keys_to_delete.append(key)

            if keys_to_delete:
                await self.client.delete(*keys_to_delete)
                logger.warning(f"Purged {len(keys_to_delete)} keys from Redis.")
            else:
                logger.info("No application-specific keys found to purge in Redis.")
        except Exception as e:
            logger.error(f"Error purging Redis data: {e}", exc_info=True)
            await self.add_error_to_queue({
                "component": "redis",
                "message": "Failed to purge Redis data",
                "error": str(e),
                "timestamp": int(time.time())
            })

    async def add_error_to_queue(self, error_data: Dict[str, Any]):
        """
        Adds an error message to a Redis list for debugging and monitoring.
        """
        try:
            # Ensure error_data is JSON serializable
            await self.client.lpush("error_queue", json.dumps(error_data))
            logger.debug(f"Pushed error to queue: {error_data.get('message', 'No message')}")
        except Exception as e:
            logger.error(f"Failed to push error to queue: {e}", exc_info=True)

    async def get_all_show_ids(self) -> List[str]:
        """
        Retrieves all normalized show IDs (keys starting with 'show:') stored in Redis.
        """
        try:
            keys = []
            async for key in self.client.scan_iter("show:*"):
                keys.append(key)
            # Extract the show ID from the key, which is after "show:"
            show_ids = [key.split(':', 1)[1] for key in keys] # keys are already decoded strings due to decode_responses=True
            logger.info(f"Retrieved {len(show_ids)} show IDs from Redis.")
            return show_ids
        except Exception as e:
            logger.error(f"Error retrieving all show IDs from Redis: {e}", exc_info=True)
            await self.add_error_to_queue({
                "component": "redis",
                "message": "Failed to get all show IDs",
                "error": str(e),
                "timestamp": int(time.time())
            })
            return []

    async def get_all_episodes_for_show(self, show_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves all episode metadata for a given show ID.
        """
        try:
            episode_keys = []
            async for key in self.client.scan_iter(f"episode:{show_id}:*"):
                episode_keys.append(key)

            episodes_data = []
            for key in episode_keys:
                # Key format: episode:{show_id}:{season_num}:{episode_num}
                parts = key.split(':')
                if len(parts) == 4:
                    _, _, season_num_str, episode_num_str = parts
                    season_num = int(season_num_str)
                    episode_num = int(episode_num_str)
                    episode_meta = await self.get_episode_metadata(show_id, season_num, episode_num)
                    if episode_meta:
                        episodes_data.append(episode_meta)
            logger.info(f"Retrieved {len(episodes_data)} episodes for show '{show_id}'.")
            return episodes_data
        except Exception as e:
            logger.error(f"Error retrieving episodes for show '{show_id}': {e}", exc_info=True)
            await self.add_error_to_queue({
                "component": "redis",
                "message": f"Failed to get episodes for show {show_id}",
                "error": str(e),
                "timestamp": int(time.time())
            })
            return []

    async def get_all_errors(self) -> List[Dict[str, Any]]:
        """
        Retrieves all errors from the error_queue.
        """
        try:
            errors = await self.client.lrange("error_queue", 0, -1)
            return [json.loads(error) for error in errors]
        except Exception as e:
            logger.error(f"Error retrieving errors from queue: {e}", exc_info=True)
            return []

    async def get_trackers(self) -> List[str]:
        """
        Retrieves the cached tracker list.
        """
        try:
            trackers_json = await self.client.get("trackers:list")
            if trackers_json:
                return json.loads(trackers_json)
            return []
        except Exception as e:
            logger.error(f"Error retrieving trackers: {e}", exc_info=True)
            return []

    async def set_trackers(self, trackers: List[str]):
        """
        Stores the list of trackers in Redis.
        """
        try:
            await self.client.set("trackers:list", json.dumps(trackers))
            logger.info(f"Stored {len(trackers)} trackers.")
        except Exception as e:
            logger.error(f"Error storing trackers: {e}", exc_info=True)
            await self.add_error_to_queue({
                "component": "redis",
                "message": "Failed to set trackers",
                "error": str(e),
                "timestamp": int(time.time())
            })

