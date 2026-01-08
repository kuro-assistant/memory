import sqlite3
import datetime

class MemoryDB:
    """
    Persistent Memory Substrate using SQLite (WAL mode).
    Handles Atomic Memory Units (AMUs) and behavioral dimensions.
    """
    def __init__(self, db_path="memory/db/kuro_memory.db"):
        self.conn = sqlite3.connect(db_path)
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
        # Implementation of AMU merging and confidence adjustment
        now = datetime.datetime.now()
        with self.conn:
            # Simple merge: old_val + delta (clamped)
            # In real implementation, this would use a more complex math model
            self.conn.execute("""
                INSERT INTO memory_atoms (id, entity_id, dimension, magnitude, context_hash, confidence, decay_rate, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    magnitude = magnitude + EXCLUDED.magnitude,
                    confidence = (confidence + EXCLUDED.confidence) / 2.0,
                    last_updated = EXCLUDED.last_updated
            """, (f"{entity_id}_{dimension}_{context_hash}", entity_id, dimension, delta, context_hash, confidence, 0.05, now))
