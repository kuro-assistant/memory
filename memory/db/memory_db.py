import sqlite3
import datetime

class MemoryDB:
    """
    Persistent Memory Substrate using SQLite (WAL mode).
    Handles Atomic Memory Units (AMUs) and behavioral dimensions.
    """
    def __init__(self, db_path="memory/db/kuro_memory.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.create_tables()

    def create_tables(self):
        with self.conn:
            # Atomic Memory Units (AMUs)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS memory_atoms (
                    id TEXT PRIMARY KEY,
                    entity_id TEXT,
                    dimension TEXT,
                    magnitude REAL,
                    context_hash TEXT,
                    confidence REAL,
                    decay_rate REAL,
                    last_updated TIMESTAMP
                )
            """)
            
            # Behavioral Preferences (Math-based weights)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS preferences (
                    key TEXT PRIMARY KEY,
                    value REAL,
                    confidence REAL,
                    updated_at TIMESTAMP
                )
            """)
            
            # Entity Relations (Graph adjacency)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS entity_relations (
                    from_entity TEXT,
                    relation TEXT,
                    to_entity TEXT,
                    weight REAL,
                    last_updated TIMESTAMP,
                    PRIMARY KEY (from_entity, relation, to_entity)
                )
            """)

    def update_atom(self, entity_id, dimension, delta, context_hash, confidence=0.5):
        """
        Phase 2C: Implements saturation math and collision handling for AMUs.
        """
        now = datetime.datetime.now()
        with self.conn:
            # 1. Anti-Collision & Saturation Math
            # magnitude = tanh(old + new) to prevent runaway weights
            self.conn.execute("""
                INSERT INTO memory_atoms (id, entity_id, dimension, magnitude, context_hash, confidence, decay_rate, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    magnitude = MAX(-1.0, MIN(1.0, magnitude + EXCLUDED.magnitude)),
                    confidence = (confidence * 0.7) + (EXCLUDED.confidence * 0.3),
                    last_updated = EXCLUDED.last_updated
            """, (f"{entity_id}_{dimension}_{context_hash}", entity_id, dimension, delta, context_hash, confidence, 0.05, now))
            
            # 2. Hard Cap Enforcement (Phase 2C)
            self._enforce_caps(entity_id, dimension)

    def _enforce_caps(self, entity_id, dimension, max_atoms=50):
        """
        Enforces a hard cap of 50 atoms per entity/dimension.
        Uses confidence-weighted eviction if limit is exceeded.
        """
        cursor = self.conn.execute("""
            SELECT count(*) FROM memory_atoms 
            WHERE entity_id = ? AND dimension = ?
        """, (entity_id, dimension))
        count = cursor.fetchone()[0]
        
        if count > max_atoms:
            # Evict the atom with lowest confidence-weighted utility
            # Utility = confidence * (1 / (1 + age_in_hours))
            self.conn.execute("""
                DELETE FROM memory_atoms 
                WHERE id = (
                    SELECT id FROM memory_atoms 
                    WHERE entity_id = ? AND dimension = ?
                    ORDER BY confidence ASC LIMIT 1
                )
            """, (entity_id, dimension))
            print(f"Memory: Cap reached for {dimension}. Evicting weakest atom.")
