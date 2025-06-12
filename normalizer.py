import re
from rapidfuzz import fuzz
from typing import List
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import os
import nltk

from logger import logger
from config import settings

# Set NLTK data path from settings
nltk.data.path.append(settings.NLTK_DATA)

try:
    _ = word_tokenize("test") # Test if punkt is available
    _ = stopwords.words('english') # Test if stopwords is available
    _ = PorterStemmer() # Test if stemmer is available
except LookupError:
    logger.error(f"NLTK data not found at {settings.NLTK_DATA}. Please ensure 'nltk.download' commands run in Dockerfile or manually.")
    logger.error("Attempting to download NLTK data now (may fail if not in writable dir):")
    try:
        nltk.download('punkt', download_dir=settings.NLTK_DATA, quiet=True)
        nltk.download('wordnet', download_dir=settings.NLTK_DATA, quiet=True)
        nltk.download('stopwords', download_dir=settings.NLTK_DATA, quiet=True)
        nltk.download('averaged_perceptron_tagger', download_dir=settings.NLTK_DATA, quiet=True)
        logger.info("NLTK data download attempt completed.")
    except Exception as e:
        logger.error(f"Failed to download NLTK data: {e}. Ensure NLTK_DATA is writable or pre-downloaded.")


class TitleNormalizer:
    """
    Handles normalization of titles for fuzzy matching.
    Includes removing special characters, lowercasing, stemming, and synonym replacement.
    """
    def __init__(self):
        self.stemmer = PorterStemmer()
        self.stop_words = set(stopwords.words('english'))
        # Custom synonym mapping
        self.synonym_map = {
            "season": "s",
            "seasons": "s",
            "episode": "ep",
            "episodes": "ep",
            "webseries": "ws",
            "tvshow": "tv",
            "tvshows": "tv",
            "movie": "mov",
            "film": "mov",
            "series": "ser",
            "part": "pt"
        }
        logger.info("TitleNormalizer initialized.")

    def normalize(self, title: str) -> str:
        """
        Normalizes a given title string.

        Args:
            title (str): The input title.

        Returns:
            str: The normalized title.
        """
        if not title:
            return ""

        # 1. Convert to lowercase
        normalized_title = title.lower()

        # 2. Remove special characters and replace with space
        normalized_title = re.sub(r'[^a-z0-9\s]', ' ', normalized_title)

        # 3. Replace multiple spaces with a single space
        normalized_title = re.sub(r'\s+', ' ', normalized_title).strip()

        # 4. Tokenize, remove stopwords, and apply stemming
        tokens = word_tokenize(normalized_title)
        stemmed_tokens = []
        for word in tokens:
            if word not in self.stop_words:
                # 5. Synonym replacement before stemming for better match
                word = self.synonym_map.get(word, word)
                stemmed_tokens.append(self.stemmer.stem(word))

        normalized_title = " ".join(stemmed_tokens)

        logger.debug(f"Normalized '{title}' to '{normalized_title}'")
        return normalized_title


class FuzzyMatcher:
    """
    Performs fuzzy matching on normalized titles using rapidfuzz.
    """
    def __init__(self, normalizer: TitleNormalizer):
        self.normalizer = normalizer
        self.matching_threshold = 85
        logger.info(f"FuzzyMatcher initialized with threshold: {self.matching_threshold}")

    def find_best_match(self, query: str, titles: List[str]) -> Optional[str]:
        """
        Finds the best fuzzy match for a query within a list of titles.

        Args:
            query (str): The search query.
            titles (List[str]): A list of available titles to match against.

        Returns:
            Optional[str]: The best matching title if the score is above the threshold, else None.
        """
        if not titles:
            return None

        normalized_query = self.normalizer.normalize(query)
        best_match = None
        highest_score = self.matching_threshold # Must be at least this score

        for title in titles:
            normalized_title = self.normalizer.normalize(title)
            score = fuzz.WRatio(normalized_query, normalized_title)
            logger.debug(f"Matching '{query}' ('{normalized_query}') against '{title}' ('{normalized_title}'): Score = {score}")

            if score > highest_score:
                highest_score = score
                best_match = title
        if best_match:
            logger.info(f"Best fuzzy match for query '{query}': '{best_match}' with score {highest_score}")
        else:
            logger.info(f"No fuzzy match found for query '{query}' above threshold {self.matching_threshold}")
        return best_match

