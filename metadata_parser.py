import re
from typing import Dict, Any, Optional, List
from guessit import guessit
from logger import logger

class MetadataParser:
    """
    Parses magnet URI titles to extract structured metadata using `guessit`
    and a regex fallback.
    """
    def __init__(self):
        self.lang_map = {
            "tam": "ta", "tamil": "ta",
            "eng": "en", "english": "en",
            "mal": "ml", "malayalam": "ml",
            "hin": "hi", "hindi": "hi",
            "tel": "te", "telugu": "te",
            "kan": "kn", "kannada": "kn",
            "kor": "ko", "korean": "ko",
            # Add more as needed
        }
        # Regex fallback for magnet parsing
        # Groups: title, season, episodeStart, episodeEnd, res, lang
        self.fallback_regex = re.compile(
            r"(?P<title>.+?)\s+"
            r"(?:S(?P<season>\d+))?\s*"
            r"(?:(?:EP)?(?P<episodeStart>\d+)(?:-(?P<episodeEnd>\d+))?)?\s*.*?"
            r"\[(?P<res>\d{3,4}p).*?"
            r"(?P<lang>(?:tam|mal|hin|eng|kor|tel|kan)(?:\s?[+]\s?[a-z]{3})*)" # Can match multiple langs like 'tam+eng'
            r"\]",
            re.IGNORECASE
        )
        logger.info("MetadataParser initialized.")

    def _normalize_languages(self, languages: List[str]) -> List[str]:
        """Normalizes language codes (e.g., 'tam' -> 'ta')."""
        normalized = []
        for lang in languages:
            # Handle combined languages like 'tam+eng'
            parts = [self.lang_map.get(p.strip().lower(), p.strip().lower()) for p in lang.split('+')]
            normalized.extend(parts)
        return sorted(list(set(normalized))) # Remove duplicates and sort

    def parse_magnet_title(self, title: str) -> Dict[str, Any]:
        """
        Parses a magnet title for video metadata using guessit, with regex fallback.

        Args:
            title (str): The magnet URI display name or torrent file name.

        Returns:
            Dict[str, Any]: A dictionary containing extracted metadata.
        """
        metadata: Dict[str, Any] = {
            "title": title,
            "year": None,
            "season": None,
            "episode_start": None,
            "episode_end": None,
            "resolution": None,
            "languages": [],
            "video_codec": None,
            "audio_codec": None,
            "release_group": None,
            "file_extension": None,
            "size": None,
        }

        # Try guessit first
        try:
            guessed_data = guessit(title)
            logger.debug(f"Guessit parsed '{title}': {guessed_data}")

            metadata["title"] = guessed_data.get("title", title)
            if "series" in guessed_data.get("type", "").lower() or "episode" in guessed_data.get("type", "").lower():
                 metadata["season"] = guessed_data.get("season")
                 metadata["episode_start"] = guessed_data.get("episode")
                 if guessed_data.get("episode_count"): # guessit can return episode_count for multi-episode packs
                    # If episode is a range, like 1-5, guessit might return episode=1, episode_count=5
                    # This needs careful handling. Assuming single episode or episode_start/end
                    if guessed_data.get("episode_count") > 1 and metadata["episode_start"] is not None:
                        metadata["episode_end"] = metadata["episode_start"] + guessed_data["episode_count"] - 1
            elif "movie" in guessed_data.get("type", "").lower():
                # For movies, assign episode-related fields to None to ensure consistency
                metadata["season"] = None
                metadata["episode_start"] = None
                metadata["episode_end"] = None

            metadata["year"] = guessed_data.get("year")
            metadata["resolution"] = guessed_data.get("screen_size") # e.g., "1080p", "720p"
            if isinstance(metadata["resolution"], list): # guessit can return list
                metadata["resolution"] = metadata["resolution"][0] if metadata["resolution"] else None
            if metadata["resolution"]:
                metadata["resolution"] = str(metadata["resolution"]).lower().replace('p', 'p') # Ensure 'p' suffix

            languages = guessed_data.get("languages", [])
            if isinstance(languages, str): # sometimes guessit returns string for single lang
                languages = [languages]
            metadata["languages"] = self._normalize_languages(languages)

            metadata["video_codec"] = guessed_data.get("video_codec")
            metadata["audio_codec"] = guessed_data.get("audio_codec")
            metadata["release_group"] = guessed_data.get("release_group")
            metadata["file_extension"] = guessed_data.get("container")
            metadata["size"] = guessed_data.get("size") # This might be less reliable from guessit

        except Exception as e:
            logger.warning(f"Guessit failed for '{title}': {e}. Falling back to regex.")

        # Fallback to regex if guessit fails or provides insufficient data
        if not metadata.get("resolution") or not metadata.get("languages") or not metadata.get("title") or not metadata.get("episode_start"):
            match = self.fallback_regex.search(title)
            if match:
                regex_data = match.groupdict()
                logger.debug(f"Regex parsed '{title}': {regex_data}")

                if not metadata.get("title") and regex_data.get("title"):
                    metadata["title"] = regex_data["title"].strip()
                if not metadata.get("season") and regex_data.get("season"):
                    try: metadata["season"] = int(regex_data["season"])
                    except (ValueError, TypeError): pass
                if not metadata.get("episode_start") and regex_data.get("episodeStart"):
                    try: metadata["episode_start"] = int(regex_data["episodeStart"])
                    except (ValueError, TypeError): pass
                if not metadata.get("episode_end") and regex_data.get("episodeEnd"):
                    try: metadata["episode_end"] = int(regex_data["episodeEnd"])
                    except (ValueError, TypeError): pass
                if not metadata.get("resolution") and regex_data.get("res"):
                    metadata["resolution"] = regex_data["res"].lower()
                if not metadata.get("languages") and regex_data.get("lang"):
                    langs = re.split(r'\s*\+\s*', regex_data["lang"])
                    metadata["languages"] = self._normalize_languages(langs)

        # Final cleanup and type conversion
        if metadata.get("season") is not None:
            metadata["season"] = int(metadata["season"])
        if metadata.get("episode_start") is not None:
            metadata["episode_start"] = int(metadata["episode_start"])
        if metadata.get("episode_end") is not None:
            metadata["episode_end"] = int(metadata["episode_end"])

        # Ensure title is not just whitespace if extracted from regex
        if isinstance(metadata["title"], str):
            metadata["title"] = metadata["title"].strip()
            if not metadata["title"]: # If it became empty after strip
                metadata["title"] = title # Fallback to original title

        logger.info(f"Final parsed metadata for '{title}': {metadata.get('title')} S{metadata.get('season')}E{metadata.get('episode_start')} [{metadata.get('resolution')} {metadata.get('languages')}]")
        return metadata

