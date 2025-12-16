# stateManager.py

import sqlite3

class SoundcloudStateStore:
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.getenv("DB_PATH", "soundcloud_state.db")
        
        self.conn = sqlite3.connect(db_path, timeout=30)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.cur = self.conn.cursor()
        self._initialize_db()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def _initialize_db(self):
        with self.conn:
            self.cur.execute('''
                CREATE TABLE IF NOT EXISTS seen_tracks (
                    track_id TEXT PRIMARY KEY,
                    ingested_at TEXT
                )
            ''')

    def track_has_been_seen(self, track_id: str) -> bool :
        cursor = self.cur.execute('SELECT 1 FROM seen_tracks WHERE track_id = ?', (track_id,))
        return cursor.fetchone() is not None

    def mark_track_as_seen(self, track_id: str , ingested_at: str ) -> None:
        with self.conn:
            self.cur.execute('''
                INSERT INTO seen_tracks (track_id, ingested_at) VALUES (?, ?)
            ''', (track_id, ingested_at)
            )

    def batch_mark_as_seen(self, tracks: list[tuple[str, str]]) -> None:
        with self.conn:
            self.cur.executemany('''
                INSERT INTO seen_tracks (track_id, ingested_at) VALUES (?, ?)
            ''', tracks
            )
            self.conn.commit()


