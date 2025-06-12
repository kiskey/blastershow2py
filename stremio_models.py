from typing import List, Literal, Dict, Any, Optional
from pydantic import BaseModel, Field, HttpUrl

# --- Common Models ---

class StremioImage(BaseModel):
    """Represents an image in Stremio metadata."""
    src: HttpUrl
    type: str # e.g., "poster", "banner", "background"

class StremioLink(BaseModel):
    """Represents a link in Stremio metadata (e.g., IMDb)."""
    name: str
    category: str
    url: HttpUrl

# --- Manifest Models ---

class StremioExtraProperty(BaseModel):
    """Defines extra properties for the manifest (e.g., search queries)."""
    name: str
    options: Optional[List[str]] = None
    isRequired: Optional[bool] = False # camelCase for Stremio API

class StremioManifest(BaseModel):
    """
    The main manifest for a Stremio addon.
    https://stremio.github.io/stremio-addon-sdk/docs/api/responses/manifest.html
    """
    id: str = "org.tamilblasters.addon"
    version: str = "0.0.1"
    name: str = "Tamil Blasters Addon"
    description: str = "Provides Tamil Web Series and TV Shows from Tamil Blasters forum."
    resources: List[str | Dict[str, Any]] = Field(
        [
            "catalog",
            "meta",
            "stream",
            {"name": "meta", "types": ["series"]}, # Explicitly declare meta for series
            {"name": "stream", "types": ["series"]} # Explicitly declare stream for series
        ],
        description="Resources served by the addon"
    )
    types: List[Literal["movie", "series", "channel", "tv", "other"]] = ["series"]
    catalogs: List[Dict[str, Any]] = [
        {
            "type": "series",
            "id": "tamil-web",
            "name": "Tamil Web Series (Tamil Blasters)",
            "extra": [
                {"name": "search", "isRequired": False}, # For /search?q=...
                {"name": "skip", "isRequired": False}, # For pagination
                {"name": "genre", "isRequired": False} # Example, if we ever add genres
            ]
        }
    ]
    # No "addon_uri" or "behaviorHints" unless specific requirements
    idPrefixes: Optional[List[str]] = ["ttb_"] # To avoid conflicts with other addons
    behaviorHints: Dict[str, Any] = {"configurable": False} # Not configurable through Stremio UI
    logo: Optional[str] = "https://i.imgur.com/your_logo_here.png" # Placeholder logo URL
    background: Optional[str] = "https://i.imgur.com/your_background_here.png" # Placeholder background URL

# --- Catalog Models ---

class StremioMetaPreview(BaseModel):
    """
    Preview of a meta item used in catalogs.
    https://stremio.github.io/stremio-addon-sdk/docs/api/responses/meta_preview.html
    """
    id: str
    type: Literal["series"]
    name: str
    poster: HttpUrl
    posterShape: Literal["poster", "landscape"] = "poster"
    releaseInfo: Optional[str] = None # e.g., "2023" or "2023 - Present"
    imdbId: Optional[str] = None
    background: Optional[HttpUrl] = None
    logo: Optional[HttpUrl] = None
    description: Optional[str] = None
    genres: Optional[List[str]] = None
    runtime: Optional[str] = None
    watched: Optional[str] = None
    country: Optional[str] = None
    trailers: Optional[List[Dict[str, Any]]] = None
    links: Optional[List[StremioLink]] = None
    popularity: Optional[float] = None
    averageRating: Optional[float] = None
    # For series
    status: Optional[str] = None # e.g., "Ended", "Continuing"

class StremioCatalog(BaseModel):
    """
    Response for a catalog request.
    https://stremio.github.io/stremio-addon-sdk/docs/api/responses/catalog.html
    """
    metas: List[StremioMetaPreview] = Field(..., alias="metas") # Use alias for camelCase

# --- Meta Models ---

class StremioEpisode(BaseModel):
    """Represents an episode of a series."""
    season: int
    episode: int
    title: str
    overview: Optional[str] = None
    thumbnail: Optional[HttpUrl] = None
    aired: Optional[str] = None # Date string
    imdbSeason: Optional[int] = None
    imdbEpisode: Optional[int] = None

class StremioMeta(BaseModel):
    """
    Full metadata for a content item.
    https://stremio.github.io/stremio-addon-sdk/docs/api/responses/meta.html
    """
    id: str
    type: Literal["series"]
    name: str
    poster: HttpUrl
    posterShape: Literal["poster", "landscape"] = "poster"
    background: Optional[HttpUrl] = None
    logo: Optional[HttpUrl] = None
    description: Optional[str] = None
    releaseInfo: Optional[str] = None
    genres: Optional[List[str]] = None
    imdbId: Optional[str] = None
    # For series
    videos: List[StremioEpisode] # Episodes are listed here for series
    runtime: Optional[str] = None # e.g., "20m" (average episode length)
    links: Optional[List[StremioLink]] = None
    trailers: Optional[List[Dict[str, Any]]] = None
    country: Optional[str] = None
    director: Optional[List[str]] = None
    writers: Optional[List[str]] = None
    cast: Optional[List[str]] = None
    popularity: Optional[float] = None
    averageRating: Optional[float] = None
    reviews: Optional[List[Dict[str, Any]]] = None
    status: Optional[str] = None # e.g., "Ended", "Continuing"

# --- Stream Models ---

class StremioStream(BaseModel):
    """
    Represents a stream (e.g., magnet link, http link).
    https://stremio.github.io/stremio-addon-sdk/docs/api/responses/stream.html
    """
    # At least one of these is required:
    url: Optional[HttpUrl] = None # Direct HTTP stream URL
    ytId: Optional[str] = None # YouTube video ID
    infoHash: Optional[str] = None # BTIH for torrent magnet link
    magnet: Optional[HttpUrl] = None # Full magnet link (preferred for torrents)

    title: Optional[str] = None # e.g., "720p H.264 [Tamil]"
    name: Optional[str] = None # Same as title, often used interchangeably
    description: Optional[str] = None # Additional info, e.g., "Source: YTS"
    thumbnail: Optional[HttpUrl] = None # Thumbnail for the stream
    dash: Optional[HttpUrl] = None # DASH manifest URL
    hls: Optional[HttpUrl] = None # HLS manifest URL
    subtitles: Optional[List[Dict[str, Any]]] = None # [{url: "...", lang: "..."}]
    behaviorHints: Optional[Dict[str, Any]] = None # e.g., {"bingeGroup": "...", "filename": "..."}

class StremioStreams(BaseModel):
    """
    Response for a stream request.
    https://stremio.github.io/stremio-addon-sdk/docs/api/responses/streams.html
    """
    streams: List[StremioStream]
