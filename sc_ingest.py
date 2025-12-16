# sc_ingest.py

import feedparser
import uuid
import os
import sqlite3
from datetime import datetime, timezone
from stateManager import SoundcloudStateStore
from pydantic import BaseModel, Field

FEEDS_CONFIG_PATH = os.getenv("FEEDS_CONFIG_PATH", "feeds.txt")
DB_PATH = os.getenv("DB_PATH", "soundcloud_state.db")

class Track(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    track_id: str
    source_url: str
    title: str
    artist: str
    track_url: str
    published_at: datetime
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

def get_feed_urls(file_path: str) -> list[str]:
    """Read feed URLs from config file, filtering out comments and empty lines."""
    feed_urls = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                feed_urls.append(line)
    return feed_urls

with SoundcloudStateStore(DB_PATH) as store:
    feed_urls = get_feed_urls(FEEDS_CONFIG_PATH)
    print(f"[INGEST] Processing {len(feed_urls)} feed(s)...")
    
    for feed_url in feed_urls:
        print(f"\n[INGEST] feed={feed_url}")
        
        try:
            feed = feedparser.parse(feed_url)
            
            if not feed.entries:
                print(f"[WARN] feed={feed_url} entries=0")
                continue
                
        except Exception as e:
            print(f"[ERROR] feed={feed_url} err={e}")
            continue

        for entry in feed.entries:
            try:
                raw_id = entry.id.split('/')[-1]
                
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    naive_dt = datetime(*entry.published_parsed[:6])
                    utc_dt = naive_dt.replace(tzinfo=timezone.utc)
                else:
                    utc_dt = datetime.now(timezone.utc)
                    print(f"[WARN] track_id={raw_id} published_parsed=missing using_now=true")
                
                try:
                    store.mark_track_as_seen(raw_id, utc_dt.isoformat())
                    
                    data = {
                        'track_id': raw_id,
                        'source_url': feed_url,
                        'title': entry.title,
                        'artist': entry.author,
                        'track_url': entry.link,
                        'published_at': utc_dt,
                    }

                    track = Track(**data)
                    print(f"[INGEST] seen=false track_id={raw_id}")
                    print(track.model_dump_json(indent=2))
                    
                except sqlite3.IntegrityError:
                    print(f"[INGEST] seen=true track_id={raw_id}")
                    
            except Exception as e:
                print(f"[ERROR] track_id={raw_id if 'raw_id' in locals() else 'unknown'} err={e}")
                continue