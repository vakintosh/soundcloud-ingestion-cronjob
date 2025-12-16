# migrate_state.py

import sqlite3
from stateManager import SoundcloudStateStore
import os
from datetime import datetime, timezone

OLD_STATE_FILE = os.getenv("OLD_STATE_FILE", "merged_downloaded.txt")
DB_PATH = os.getenv("DB_PATH", "soundcloud_state.db")
MIGRATION_TIME = datetime.now(timezone.utc).isoformat()


def get_track_data(file):
    """
    Reads the file and yields tuples (track_id, ingested_at) 
    ready for database insertion.
    """
    track_ids = set()
    for line in file:
        parts = line.strip().split()
        if len(parts) >= 2:
            track_id = parts[1]
            track_ids.add(track_id)
    
    return [(track_id, MIGRATION_TIME) for track_id in track_ids]

if __name__ == "__main__":
    try:
        with SoundcloudStateStore(DB_PATH) as store:
            print(f"[MIGRATE] Reading old state from {OLD_STATE_FILE}...")
            
            if not os.path.exists(OLD_STATE_FILE):
                print(f"[ERROR] File not found: {OLD_STATE_FILE}")
                exit(1)
            
            with open(OLD_STATE_FILE, 'r') as f:
                tracks_to_insert = get_track_data(f)
            
            if tracks_to_insert:
                print(f"[MIGRATE] Found {len(tracks_to_insert)} unique tracks to migrate.")
                
                migrated = 0
                skipped = 0
                for track_id, ingested_at in tracks_to_insert:
                    try:
                        store.mark_track_as_seen(track_id, ingested_at)
                        migrated += 1
                    except sqlite3.IntegrityError:
                        skipped += 1
                
                print(f"[MIGRATE] Complete. migrated={migrated} skipped={skipped}")
            else:
                print("[MIGRATE] No tracks found in the old state file.")
                
    except Exception as e:
        print(f"[ERROR] Migration failed: {e}")
        exit(1)