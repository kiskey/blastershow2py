import math
from typing import Set
import mmh3 # pip install mmh3 (added to requirements.txt indirectly by rapidfuzz)
from logger import logger

class BloomFilter:
    """
    A simple Bloom filter implementation for checking duplicates.
    Stores data in Redis.
    """
    def __init__(self, redis_client, key: str, capacity: int, error_rate: float):
        """
        Initializes the Bloom filter.

        Args:
            redis_client: An instance of redis.asyncio.Redis.
            key (str): The Redis key to store the Bloom filter bit array.
            capacity (int): Expected number of elements to be added.
            error_rate (float): Desired false positive probability (0.0 to 1.0).
        """
        self.redis = redis_client
        self.key = key
        self.capacity = capacity
        self.error_rate = error_rate

        # Calculate optimal size (m) and number of hash functions (k)
        self.m = self._get_optimal_size(capacity, error_rate)
        self.k = self._get_optimal_hash_functions(self.m, capacity)

        logger.info(f"Initialized Bloom Filter '{self.key}' with m={self.m}, k={self.k}")

    async def add(self, item: str):
        """
        Adds an item to the Bloom filter.
        """
        # Get hash indices
        indices = [mmh3.hash(item, seed + 1) % self.m for seed in range(self.k)]

        # Set bits in Redis
        async with self.redis.pipeline() as pipe:
            for i in indices:
                pipe.setbit(self.key, i, 1)
            await pipe.execute()
        logger.debug(f"Added '{item}' to Bloom filter '{self.key}'")


    async def contains(self, item: str) -> bool:
        """
        Checks if an item might be in the Bloom filter (may return false positives).
        """
        # Get hash indices
        indices = [mmh3.hash(item, seed + 1) % self.m for seed in range(self.k)]

        # Check bits in Redis
        async with self.redis.pipeline() as pipe:
            for i in indices:
                pipe.getbit(self.key, i)
            results = await pipe.execute()

        # If all bits are set, the item *might* be in the set
        if all(bit == 1 for bit in results):
            logger.debug(f"Bloom filter '{self.key}' MIGHT contain '{item}'")
            return True
        logger.debug(f"Bloom filter '{self.key}' does NOT contain '{item}'")
        return False

    async def clear(self):
        """
        Clears the Bloom filter by deleting its Redis key.
        """
        await self.redis.delete(self.key)
        logger.info(f"Cleared Bloom filter '{self.key}'")

    @staticmethod
    def _get_optimal_size(capacity: int, error_rate: float) -> int:
        """
        Calculates the optimal size (m) of the bit array.
        m = -(n * ln(p)) / (ln(2)^2)
        """
        return int(math.ceil(-(capacity * math.log(error_rate)) / (math.log(2) ** 2)))

    @staticmethod
    def _get_optimal_hash_functions(m: int, capacity: int) -> int:
        """
        Calculates the optimal number of hash functions (k).
        k = (m/n) * ln(2)
        """
        return int(math.ceil((m / capacity) * math.log(2)))

