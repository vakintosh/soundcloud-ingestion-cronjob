import feedparser
import uuid
import os
import sqlite3
from datetime import datetime, timezone
from stateManager import SoundcloudStateStore
from pydantic import BaseModel, Field
from typing import Set, List, Dict, Any
import logging
import sys

FEEDS_CONFIG_PATH = os.getenv("FEEDS_CONFIG_PATH", "feeds.txt")
DB_PATH = os.getenv("DB_PATH", "soundcloud_state.db")
BATCH_SIZE = 100

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(levelname)s event=%(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("sc-ingest")

class Track(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    track_id: str
    source_url: str
    title: str
    artist: str
    track_url: str
    published_at: datetime
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

def get_feed_urls(file_path: str) -> List[str]:
    """Read feed URLs from config file, filtering out comments and empty lines."""
    feed_urls = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                feed_urls.append(line)
    return feed_urls


def parse_entry_data(entry, feed_url: str) -> Dict[str, Any]:
    """Extract track data from feed entry."""
    raw_id = entry.id.split('/')[-1]
    
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        naive_dt = datetime(*entry.published_parsed[:6])
        utc_dt = naive_dt.replace(tzinfo=timezone.utc)
    else:
        utc_dt = datetime.now(timezone.utc)
        logger.warning(
            "published_missing track_id=%s using_now=true",
            raw_id,
        )
    
    return {
        'track_id': raw_id,
        'source_url': feed_url,
        'title': entry.title,
        'artist': entry.author,
        'track_url': entry.link,
        'published_at': utc_dt,
    }


def process_feed_entries(store: SoundcloudStateStore, entries, feed_url: str, seen_tracks: Set[str]) -> tuple[int, int]:
    """
    Process feed entries in batches for better performance.
    Returns (new_tracks_count, skipped_tracks_count)
    """
    new_tracks = []
    new_count = 0
    skipped = 0
    
    for entry in entries:
        try:
            data = parse_entry_data(entry, feed_url)
            track_id = data['track_id']
            

            if track_id in seen_tracks:
                logger.info("track_seen track_id=%s", track_id)
                skipped += 1
                continue
            
            utc_dt = data['published_at']
            store.mark_track_as_seen(track_id, utc_dt.isoformat())
            seen_tracks.add(track_id)
            
            track = Track(**data)
            new_tracks.append(track)
            new_count += 1

            logger.info("track_new track_id=%s", track_id)

            if len(new_tracks) >= BATCH_SIZE:
                output_tracks(new_tracks)
                new_tracks = []
                
        except sqlite3.IntegrityError:
            logger.info("track_seen track_id=%s", track_id)
            seen_tracks.add(track_id)
            skipped += 1
        except (AttributeError, KeyError, ValueError) as e:
            track_id = entry.id.split('/')[-1] if hasattr(entry, 'id') else 'unknown'
            logger.error(
                "parse_error track_id=%s err=%s",
                track_id,
                e,
            )
            continue
        except Exception as e:
            track_id = entry.id.split('/')[-1] if hasattr(entry, 'id') else 'unknown'
            logger.error(
                "unexpected_error track_id=%s err=%s",
                track_id,
                e,
            )
            continue
    
    if new_tracks:
        output_tracks(new_tracks)
    
    return new_count, skipped


def output_tracks(tracks: List[Track]) -> None:
    """Output a batch of tracks."""
    for track in tracks:
        logger.info("track_output track_id=%s", track.track_id)
        print(track.model_dump_json(indent=2))


def main():
    """Main ingestion process."""

    logger.info("ingest_start")

    try:
        with SoundcloudStateStore(DB_PATH) as store:
            logger.info("cache_load_start")
            seen_tracks = store.get_all_seen_track_ids()
            logger.info("cache_loaded size=%d", len(seen_tracks))
            
            feed_urls = get_feed_urls(FEEDS_CONFIG_PATH)
            logger.info("feeds_loaded count=%d", len(feed_urls))
            
            total_new = 0
            total_skipped = 0
            
            for idx, feed_url in enumerate(feed_urls, 1):
                logger.info(
                    "feed_start feed=%s index=%d total=%d",
                    feed_url,
                    idx,
                    len(feed_urls),
                )
                
                try:
                    feed = feedparser.parse(feed_url)
                    
                    if not feed.entries:
                        logger.warning(
                            "feed_empty feed=%s",
                            feed_url,
                        )
                        continue
                    
                    logger.info(
                        "feed_entries feed=%s count=%d",
                        feed_url,
                        len(feed.entries),
                    )
                    new_count, skipped_count = process_feed_entries(store, feed.entries, feed_url, seen_tracks)
                    
                    total_new += new_count
                    total_skipped += skipped_count
                    logger.info(
                        "feed_done feed=%s new=%d skipped=%d",
                        feed_url,
                        new_count,
                        skipped_count,
                    )
                        
                except Exception as e:
                    logger.error(
                        "feed_failed feed=%s err=%s",
                        feed_url,
                        e,
                    )

            logger.info(
                "ingest_done total_new=%d total_skipped=%d total_processed=%d",
                total_new,
                total_skipped,
                total_new + total_skipped,
            )
            sys.exit(0)

    except FileNotFoundError:
        logger.error(
            "config_error path=%s",
            FEEDS_CONFIG_PATH,
        )
        sys.exit(1)

    except Exception as e:
        logger.error(
            "ingest_failed err=%s",
            e,
        )
        sys.exit(2)


if __name__ == "__main__":
    main()