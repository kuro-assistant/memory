import time
import math
import datetime
import threading
import sqlite3
from memory.db.memory_db import MemoryDB

class DecayEngine:
    """
    Exponential Decay Daemon for VM 3.
    Applies S(t) = S0 * e^(-lambda * t) to all memory atoms.
    Hardenened for Phase 3.5: Per-thread connection safety.
    """
    def __init__(self, db: MemoryDB, interval_sec=3600):
        self.db = db
        self.interval_sec = interval_sec
        self.running = False
        self._thread = None

    def start(self):
        self.running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(f"Decay Engine started (Interval: {self.interval_sec}s)")

    def stop(self):
        self.running = False
        if self._thread:
            self._thread.join()

    def _run_loop(self):
        while self.running:
            try:
                self.apply_decay()
            except Exception as e:
                print(f"Decay Engine Error: {e}")
            time.sleep(self.interval_sec)

    def apply_decay(self):
        """
        Iterate through all memory atoms and reduce magnitude based on time delta.
        """
        now = datetime.datetime.now()
        # Audit Fix: Open a fresh connection for the decay thread
        with sqlite3.connect(self.db.db_path) as conn:
            with conn:
                cursor = conn.execute("SELECT id, magnitude, last_updated, decay_rate FROM memory_atoms")
                atoms = cursor.fetchall()
                
                for atom_id, magnitude, last_updated_str, decay_rate in atoms:
                    last_updated = datetime.datetime.fromisoformat(last_updated_str)
                    delta_t = (now - last_updated).total_seconds() / 3600.0 # Time in hours
                    
                    # S(t) = S0 * e^(-lambda * t)
                    new_magnitude = magnitude * math.exp(-decay_rate * delta_t)
                    
                    if abs(new_magnitude) < 0.01:
                        conn.execute("DELETE FROM memory_atoms WHERE id = ?", (atom_id,))
                    else:
                        conn.execute("""
                            UPDATE memory_atoms 
                            SET magnitude = ?, last_updated = ? 
                            WHERE id = ?
                        """, (new_magnitude, now.isoformat(), atom_id))
                
                print(f"[{now}] Applied decay to {len(atoms)} memory atoms.")

class ReinforcementEngine:
    """
    Updates behavioral weights based on explicit or implicit feedback.
    """
    def __init__(self, db: MemoryDB):
        self.db = db

    def reinforce(self, key: str, choice: bool, magnitude=0.1):
        delta = magnitude if choice else -magnitude
        now = datetime.datetime.now()
        
        with sqlite3.connect(self.db.db_path) as conn:
            with conn:
                conn.execute("""
                    INSERT INTO preferences (key, value, confidence, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = value + EXCLUDED.value,
                        confidence = MIN(1.0, confidence + 0.05),
                        updated_at = EXCLUDED.updated_at
                """, (key, delta, 0.5, now.isoformat()))
                print(f"Reinforced '{key}': {delta}")
