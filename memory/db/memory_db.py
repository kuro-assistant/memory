import sqlite3
import datetime
import os

class MemoryDB:
    """
    Persistent Memory Substrate using SQLite (WAL mode).
    Hardenened for Phase 3.5: Per-thread connection safety.
    """
    def __init__(self, db_path="memory/db/kuro_memory.db"):
        self.db_path = db_path
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.db_path or "memory/db/"), exist_ok=True)
        # Setup schema using a transient connection
        with self.get_conn() as conn:
            self._create_tables(conn)

    def get_conn(self):
        """ Returns a fresh, thread-local connection with WAL enabled. """
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _create_tables(self, conn):
        with conn:
            # Atomic Memory Units (AMUs)
            conn.execute("""
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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS preferences (
                    key TEXT PRIMARY KEY,
                    value REAL,
                    confidence REAL,
                    updated_at TIMESTAMP
                )
            """)
            
            # Entity Relations (Graph adjacency)
            conn.execute("""
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
        now = datetime.datetime.now()
        with self.get_conn() as conn:
            with conn:
                conn.execute("""
                    INSERT INTO memory_atoms (id, entity_id, dimension, magnitude, context_hash, confidence, decay_rate, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        magnitude = MAX(-1.0, MIN(1.0, magnitude + EXCLUDED.magnitude)),
                        confidence = (confidence * 0.7) + (EXCLUDED.confidence * 0.3),
                        last_updated = EXCLUDED.last_updated
                """, (f"{entity_id}_{dimension}_{context_hash}", entity_id, dimension, delta, context_hash, confidence, 0.05, now))
                self._enforce_caps(conn, entity_id, dimension)

    def _enforce_caps(self, conn, entity_id, dimension, max_atoms=50):
        cursor = conn.execute("""
            SELECT count(*) FROM memory_atoms 
            WHERE entity_id = ? AND dimension = ?
        """, (entity_id, dimension))
        count = cursor.fetchone()[0]
        
        if count > max_atoms:
            conn.execute("""
                DELETE FROM memory_atoms 
                WHERE id = (
                    SELECT id FROM memory_atoms 
                    WHERE entity_id = ? AND dimension = ?
                    ORDER BY confidence ASC LIMIT 1
                )
            """, (entity_id, dimension))
            print(f"Memory: Cap reached for {dimension}. Evicting weakest atom.")

    def get_memory_summaries(self, entities):
        with self.get_conn() as conn:
            summaries = []
            for ent in entities:
                cursor = conn.execute("SELECT dimension, magnitude FROM memory_atoms WHERE entity_id = ?", (ent,))
                atoms = cursor.fetchall()
                if atoms:
                    sum_str = f"Entity: {ent} | " + ", ".join([f"{d}: {m:.2f}" for d, m in atoms])
                    summaries.append(sum_str)
            return summaries

    def get_preferences(self):
        with self.get_conn() as conn:
            cursor = conn.execute("SELECT key, value FROM preferences")
            return {row[0]: row[1] for row in cursor.fetchall()}
