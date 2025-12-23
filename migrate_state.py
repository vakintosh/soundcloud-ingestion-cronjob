# migrate_state.py

import sqlite3
from stateManager import SoundcloudStateStore
import os
from datetime import datetime, timezone
import logging
import sys

OLD_STATE_FILE = os.getenv("OLD_STATE_FILE", "downloaded.txt")
DB_PATH = os.getenv("DB_PATH", "soundcloud_state.db")
MIGRATION_TIME = datetime.now(timezone.utc).isoformat()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(levelname)s event=%(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("sc-ingest")


def get_track_data(file):
    """Generator that yields (track_id, ingested_at) tuples."""
    seen = set()
    for line in file:
        parts = line.strip().split()
        if len(parts) >= 2:
            track_id = parts[1]
            if track_id not in seen:
                seen.add(track_id)
                yield (track_id, MIGRATION_TIME)

def process_batch(store, batch):
    """Process a batch of tracks and return (migrated_count, skipped_count)."""
    migrated = 0
    skipped = 0
    
    for track_id, ingested_at in batch:
        try:
            store.mark_track_as_seen(track_id, ingested_at)
            migrated += 1
        except sqlite3.IntegrityError:
            skipped += 1
    
    return migrated, skipped

def main():
    BATCH_SIZE = 1000
    PROGRESS_INTERVAL = 5000
    
    try:
        with SoundcloudStateStore(DB_PATH) as store:
            logger.info(f"Starting migration from {OLD_STATE_FILE} to {DB_PATH}")
            
            if not os.path.exists(OLD_STATE_FILE):
                logger.error(f"File not found: {OLD_STATE_FILE}")
                sys.exit(1)
            
            with open(OLD_STATE_FILE, 'r') as f:
                migrated = 0
                skipped = 0
                batch = []
                
                for track_id, ingested_at in get_track_data(f):
                    batch.append((track_id, ingested_at))
                    
                    if len(batch) >= BATCH_SIZE:
                        batch_migrated, batch_skipped = process_batch(store, batch)
                        migrated += batch_migrated
                        skipped += batch_skipped
                        batch = []
                        
                        if (migrated + skipped) % PROGRESS_INTERVAL == 0:
                            logger.info(f"Progress: {migrated + skipped} tracks processed...")
                
                if batch:
                    batch_migrated, batch_skipped = process_batch(store, batch)
                    migrated += batch_migrated
                    skipped += batch_skipped
                
                if migrated + skipped > 0:
                    logger.info(f"Migration complete. migrated={migrated} skipped={skipped} total={migrated + skipped}")
                else:
                    logger.info("No tracks found in the old state file.")
                
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()