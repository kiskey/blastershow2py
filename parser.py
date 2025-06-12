from bs4 import BeautifulSoup, Tag
from typing import Dict, Any, List, Optional
import re
from urllib.parse import urljoin
from datetime import datetime
from logger import logger
from metadata_parser import MetadataParser

class ThreadParser:
    """
    Parses the HTML content of a forum thread page to extract show metadata
    and magnet links.
    """
    def __init__(self):
        self.metadata_parser = MetadataParser()
        logger.info("ThreadParser initialized.")

    def parse_thread_page(self, html_content: str, base_url: str) -> Optional[Dict[str, Any]]:
        """
        Parses the HTML content of a single thread page.

        Args:
            html_content (str): The HTML content of the page.
            base_url (str): The base URL of the forum for resolving relative links.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing parsed show and episode data,
                                      or None if essential data cannot be extracted.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        data: Dict[str, Any] = {
            "title": None,
            "poster": None,
            "magnets": [],
            "last_modified_timestamp": None,
            "raw_magnets_metadata": [] # To store parsed magnet details
        }

        # 1. Extract thread title
        try:
            title_tag = soup.find("span", class_="ipsType_break ipsContained")
            if title_tag:
                data["title"] = title_tag.get_text(strip=True)
                logger.debug(f"Extracted title: {data['title']}")
            else:
                logger.warning("Could not find thread title tag.")
        except Exception as e:
            logger.error(f"Error extracting title: {e}", exc_info=True)

        # 2. Extract poster (first img with class ipsImage within the content)
        # Search for images within the main content area, not header/footer
        try:
            content_block = soup.find("div", class_="ipsType_normal ipsType_richText")
            if content_block:
                poster_img_tag = content_block.find("img", class_="ipsImage")
                if poster_img_tag and poster_img_tag.get("src"):
                    data["poster"] = urljoin(base_url, poster_img_tag["src"])
                    logger.debug(f"Extracted poster URL: {data['poster']}")
            else:
                logger.warning("Could not find main content block for poster.")
        except Exception as e:
            logger.error(f"Error extracting poster: {e}", exc_info=True)


        # 3. Extract all magnet URIs
        try:
            magnet_links = soup.find_all("a", class_="magnet-plugin")
            for link_tag in magnet_links:
                magnet_uri = link_tag.get("href")
                magnet_title = link_tag.get_text(strip=True) or (link_tag.get("title", "") if link_tag.get("title") else magnet_uri) # Fallback to title attr or URI
                if magnet_uri and magnet_uri.startswith("magnet:?"):
                    data["magnets"].append({"uri": magnet_uri, "title": magnet_title})
                    logger.debug(f"Found magnet: {magnet_title} -> {magnet_uri}")
                    # Parse magnet title immediately
                    parsed_magnet_meta = self.metadata_parser.parse_magnet_title(magnet_title)
                    # Add magnet URI to parsed metadata
                    parsed_magnet_meta["magnet_uri"] = magnet_uri
                    data["raw_magnets_metadata"].append(parsed_magnet_meta)

            if not data["magnets"]:
                logger.warning("No magnet links found on the page.")
        except Exception as e:
            logger.error(f"Error extracting magnet links: {e}", exc_info=True)

        # 4. Extract thread last modified timestamp
        # Look for <time> tags with specific attributes or within metadata blocks
        try:
            # Common patterns for last modified/updated timestamp
            # 1. Look for 'data-short=' or 'datetime=' attributes in <time> tags
            time_tag = soup.find("time", attrs={"datetime": True})
            if not time_tag:
                # Fallback to search for time within post content, often in a footer or byline
                # This depends heavily on forum structure, so a generic search might be needed
                time_tag = soup.find("time", class_="ipsType_break") # Example for last post time
            if not time_tag:
                # Try finding in specific meta information areas like 'last updated'
                time_tag = soup.find("span", class_="ipsType_normal ipsType_light ipsType_noMargin").find("time")


            if time_tag and time_tag.get("datetime"):
                dt_str = time_tag["datetime"]
                # Parse ISO 8601 string to datetime object
                dt_object = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                data["last_modified_timestamp"] = int(dt_object.timestamp())
                logger.debug(f"Extracted last modified timestamp: {dt_str} -> {data['last_modified_timestamp']}")
            elif time_tag and time_tag.get("data-time"): # Sometimes uses data-time for Unix timestamp
                data["last_modified_timestamp"] = int(time_tag["data-time"])
                logger.debug(f"Extracted last modified timestamp from data-time: {data['last_modified_timestamp']}")
            else:
                logger.warning("Could not find a reliable last modified timestamp on the page.")
                data["last_modified_timestamp"] = int(datetime.now().timestamp()) # Default to now

        except Exception as e:
            logger.error(f"Error extracting last modified timestamp: {e}", exc_info=True)
            data["last_modified_timestamp"] = int(datetime.now().timestamp()) # Default to now on error


        # Validate essential data
        if not data["title"] or not data["magnets"]:
            logger.warning(f"Skipping thread due to missing essential data: Title={data['title']}, Magnets present={bool(data['magnets'])}")
            return None

        return data

