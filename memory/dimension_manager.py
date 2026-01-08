import sqlite3
import datetime
from memory.db.memory_db import MemoryDB

class DimensionManager:
    """
    Manages the health and density of memory dimensions in VM 3.
    Automatically prunes weak atoms and collapses redundant dimensions.
    """
    def __init__(self, db: MemoryDB, pruning_threshold=0.1):
        self.db = db
        self.pruning_threshold = pruning_threshold

    def prune_weak_atoms(self):
        """
        Deletes memory atoms where magnitude or confidence is too low.
        """
        with self.db.conn:
            cursor = self.db.conn.execute("""
                DELETE FROM memory_atoms 
                WHERE abs(magnitude) < ? OR confidence < ?
            """, (self.pruning_threshold, self.pruning_threshold))
            print(f"Pruned {cursor.rowcount} weak memory atoms.")

    def collapse_redundant_dimensions(self):
        """
        Logic to merge dimensions that correlate strongly (Placeholder for advancement).
        For now, it just ensures no duplicate dimension/context pairs.
        """
        # In a real system, this would analyze correlation matrices
        pass

    def get_dimension_report(self):
        """
        Returns a summary of all active dimensions and their total weight.
        """
        with self.db.conn:
            cursor = self.db.conn.execute("""
                SELECT dimension, count(*), sum(abs(magnitude)) 
                FROM memory_atoms 
                GROUP BY dimension
            """)
            return cursor.fetchall()
