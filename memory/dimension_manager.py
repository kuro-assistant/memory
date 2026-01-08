import sqlite3
import datetime
from memory.db.memory_db import MemoryDB

class DimensionManager:
    """
    Manages the health and density of memory dimensions in VM 3.
    Hardened for Phase 3.5: Per-thread connection safety.
    """
    def __init__(self, db: MemoryDB, pruning_threshold=0.1):
        self.db = db
        self.pruning_threshold = pruning_threshold

    def prune_weak_atoms(self):
        """
        Deletes memory atoms where magnitude or confidence is too low.
        """
        with self.db.get_conn() as conn:
            with conn:
                cursor = conn.execute("""
                    DELETE FROM memory_atoms 
                    WHERE abs(magnitude) < ? OR confidence < ?
                """, (self.pruning_threshold, self.pruning_threshold))
                print(f"Pruned {cursor.rowcount} weak memory atoms.")

    def collapse_redundant_dimensions(self):
        pass

    def get_dimension_report(self):
        with self.db.get_conn() as conn:
            cursor = conn.execute("""
                SELECT dimension, count(*), sum(abs(magnitude)) 
                FROM memory_atoms 
                GROUP BY dimension
            """)
            return cursor.fetchall()
