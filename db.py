import orjson
import asyncio
from typing import Dict, Any, List, Optional
import redis.asyncio as redis
from redis.asyncio.client import PubSub
from logger import logger
from config import settings

class RedisClient:
    """
    A client for asynchronous Redis operations, including data persistence
    for show, season, episode, thread, and tracker metadata.
    Uses orjson for efficient JSON serialization/deserialization.
    """
    def __init__(self):
        """
        Initializes the Redis connection pool.
        """
        self._redis: Optional[redis.Redis] = None
        self._pubsub: Optional[PubSub] = None
        self._connected = False
        logger.info(f"Redis client initialized for URL: {settings.REDIS_URL}")

    async def connect(self):
        """
        Establishes a connection to Redis.
        """
        if self._connected and self._redis:
            logger.debug("Redis already connected.")
            return

        try:
            self._redis = await redis.from_url(settings.REDIS_URL, decode_responses=False)
            await self._redis.ping() # Test connection
            self._connected = True
            logger.info("Successfully connected to Redis.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Failed to connect to Redis at {settings.REDIS_URL}: {e}")
            self._connected = False
            raise # Re-raise to indicate critical failure

    async def disconnect(self):
        """
        Closes the Redis connection.
        """
        if self._connected and self._redis:
            await self._redis.close()
            self._connected = False
            logger.info("Disconnected from Redis.")

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

    @property
    def client(self) -> redis.Redis:
        """
        Provides the Redis client instance. Raises an error if not connected.
        """
        if not self._connected or not self._redis:
            logger.error("Redis client is not connected.")
            raise ConnectionError("Redis client is not connected.")
        return self._redis

    async def purge_all_data(self):
        """
        Deletes all keys from the current Redis database.
        Use with extreme caution!
        """
        await self.client.flushdb()
        logger.warning("All data purged from Redis database!")

    async def set_show_metadata(self, show_id: str, metadata: Dict[str, Any]):
        """
        Stores show-level metadata (e.g., poster, languages).
        Key: show:{normalized_title}
        Value: HASH of metadata.
        """
        key = f"show:{show_id}"
        await self.client.hset(key, mapping={k: orjson.dumps(v) if isinstance(v, (dict, list)) else v for k, v in metadata.items()})
        logger.debug(f"Stored show metadata for '{show_id}'")

    async def get_show_metadata(self, show_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves show-level metadata.
        """
        key = f"show:{show_id}"
        data = await self.client.hgetall(key)
        if not data:
            return None
        # Decode values and attempt to parse JSON where applicable
        decoded_data = {}
        for k, v in data.items():
            k_str = k.decode('utf-8')
            v_str = v.decode('utf-8')
            try:
                decoded_data[k_str] = orjson.loads(v_str)
            except orjson.JSONDecodeError:
                decoded_data[k_str] = v_str # Not JSON, keep as string
        logger.debug(f"Retrieved show metadata for '{show_id}'")
        return decoded_data

    async def get_all_shows(self) -> List[Dict[str, Any]]:
        """
        Retrieves metadata for all stored shows.
        """
        show_keys = await self.client.keys("show:*")
        all_shows = []
        for key in show_keys:
            show_id = key.decode('utf-8').replace('show:', '')
            meta = await self.get_show_metadata(show_id)
            if meta:
                all_shows.append({"id": show_id, **meta})
        logger.info(f"Retrieved metadata for {len(all_shows)} shows.")
        return all_shows

    async def add_season_entry(self, show_id: str, season: int, thread_id: str, quality: str, lang: str, timestamp: int):
        """
        Adds an entry to a ZSET for a specific season.
        Key: season:{showId}:{season}
        Value: ZSET member: "{timestamp} {thread_id}:{quality}:{lang}"
        Score: timestamp
        """
        key = f"season:{show_id}:{season}"
        member = f"{timestamp} {thread_id}:{quality}:{lang}"
        await self.client.zadd(key, {member: timestamp})
        logger.debug(f"Added season entry for '{show_id}' S{season} from thread {thread_id}")

    async def get_season_entries(self, show_id: str, season: int) -> List[str]:
        """
        Retrieves all season entries for a given show and season, sorted by timestamp.
        """
        key = f"season:{show_id}:{season}"
        entries = await self.client.zrange(key, 0, -1) # Get all members
        logger.debug(f"Retrieved {len(entries)} entries for '{show_id}' S{season}")
        return [e.decode('utf-8') for e in entries]

    async def set_episode_metadata(self, show_id: str, season: int, episode: int, metadata: Dict[str, Any]):
        """
        Stores full episode metadata and magnet links.
        Key: episode:{showId}:{season}:{episode}
        Value: HASH of metadata.
        """
        key = f"episode:{show_id}:{season}:{episode}"
        # Serialize complex objects to JSON strings
        serialized_metadata = {}
        for k, v in metadata.items():
            if isinstance(v, (dict, list)):
                serialized_metadata[k] = orjson.dumps(v).decode('utf-8') # Store as string
            else:
                serialized_metadata[k] = v

        await self.client.hset(key, mapping=serialized_metadata)
        logger.debug(f"Stored episode metadata for '{show_id}' S{season}E{episode}")

    async def get_episode_metadata(self, show_id: str, season: int, episode: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves full episode metadata.
        """
        key = f"episode:{show_id}:{season}:{episode}"
        data = await self.client.hgetall(key)
        if not data:
            return None
        decoded_data = {}
        for k, v in data.items():
            k_str = k.decode('utf-8')
            v_str = v.decode('utf-8')
            try:
                # Attempt to parse JSON values back to Python objects
                decoded_data[k_str] = orjson.loads(v_str)
            except (orjson.JSONDecodeError, TypeError):
                decoded_data[k_str] = v_str # If not JSON, keep as string
        logger.debug(f"Retrieved episode metadata for '{show_id}' S{season}E{episode}")
        return decoded_data

    async def set_thread_last_visited(self, thread_id: str, timestamp: int):
        """
        Stores the last visited timestamp for a given thread.
        Key: thread:{thread_id}
        Value: timestamp (Unix epoch)
        """
        key = f"thread:{thread_id}"
        await self.client.set(key, timestamp)
        logger.debug(f"Stored last visited timestamp for thread '{thread_id}'")

    async def get_thread_last_visited(self, thread_id: str) -> Optional[int]:
        """
        Retrieves the last visited timestamp for a given thread.
        """
        key = f"thread:{thread_id}"
        timestamp = await self.client.get(key)
        if timestamp:
            return int(timestamp.decode('utf-8'))
        return None

    async def set_trackers(self, trackers: List[str]):
        """
        Stores the list of top trackers.
        Key: trackers:latest
        Value: JSON array of tracker URLs.
        """
        key = "trackers:latest"
        await self.client.set(key, orjson.dumps(trackers))
        logger.info(f"Stored {len(trackers)} trackers.")

    async def get_trackers(self) -> List[str]:
        """
        Retrieves the list of top trackers.
        """
        key = "trackers:latest"
        data = await self.client.get(key)
        if data:
            return orjson.loads(data)
        logger.warning("No trackers found in Redis.")
        return []

    async def add_error_to_queue(self, error_data: Dict[str, Any]):
        """
        Pushes structured error information to a Redis list for monitoring.
        """
        key = "error_queue"
        await self.client.rpush(key, orjson.dumps(error_data))
        logger.error(f"Pushed error to queue: {error_data.get('message', 'No message')}")

    async def get_redis_key_dump(self, key: str) -> Optional[str]:
        """
        Retrieves and dumps the content of a Redis key for debugging.
        Handles different Redis data types (string, hash, list, set, zset).
        """
        try:
            _type = await self.client.type(key)
            key_type = _type.decode('utf-8')

            if key_type == "string":
                value = await self.client.get(key)
                return f"TYPE: string\nVALUE: {value.decode('utf-8') if value else 'N/A'}"
            elif key_type == "hash":
                data = await self.client.hgetall(key)
                decoded_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}
                return f"TYPE: hash\nVALUE: {orjson.dumps(decoded_data, option=orjson.OPT_INDENT_2).decode('utf-8')}"
            elif key_type == "list":
                data = await self.client.lrange(key, 0, -1)
                decoded_data = [item.decode('utf-8') for item in data]
                return f"TYPE: list\nVALUE: {orjson.dumps(decoded_data, option=orjson.OPT_INDENT_2).decode('utf-8')}"
            elif key_type == "set":
                data = await self.client.smembers(key)
                decoded_data = [item.decode('utf-8') for item in data]
                return f"TYPE: set\nVALUE: {orjson.dumps(decoded_data, option=orjson.OPT_INDENT_2).decode('utf-8')}"
            elif key_type == "zset":
                data = await self.client.zrange(key, 0, -1, withscores=True)
                decoded_data = [{item.decode('utf-8'): score} for item, score in data]
                return f"TYPE: zset\nVALUE: {orjson.dumps(decoded_data, option=orjson.OPT_INDENT_2).decode('utf-8')}"
            else:
                return f"TYPE: {key_type}\nVALUE: Not supported for direct dump or key does not exist."
        except Exception as e:
            logger.error(f"Error dumping Redis key '{key}': {e}", exc_info=True)
            return f"Error: {e}"

