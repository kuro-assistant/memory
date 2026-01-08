import grpc
from concurrent import futures
import datetime
from memory.db.memory_db import MemoryDB
from memory.decay_engine import DecayEngine, ReinforcementEngine
from memory.dimension_manager import DimensionManager
from common.utils.health import HealthServicer
from common.proto import kuro_pb2
from common.proto import kuro_pb2_grpc
from google.protobuf import struct_pb2

class MemoryServicer(kuro_pb2_grpc.MemoryServiceServicer):
    """
    gRPC Service for Persistent Memory (VM 3).
    """
    def __init__(self):
        self.db = MemoryDB()
        self.decay_engine = DecayEngine(self.db)
        self.reinforce_engine = ReinforcementEngine(self.db)
        self.dim_manager = DimensionManager(self.db)
        # Prune every time server starts (or could be periodic)
        self.dim_manager.prune_weak_atoms()
        self.decay_engine.start()

    def GetContext(self, request, context):
        """
        Retrieve memory summaries and preferences from the real SQLite substrate.
        """
        entities = list(request.entities) if request.entities else ["user"]
        summaries = self.db.get_memory_summaries(entities)
        prefs = self.db.get_preferences()
        
        response = kuro_pb2.ContextResponse()
        response.memory_summaries.extend(summaries)
        for k, v in prefs.items():
            response.preferences[k] = v
            
        return response

    def ProposeMemory(self, request, context):
        """
        Store a new memory atom after validation by VM 1.
        """
        try:
            self.db.update_atom(
                entity_id=request.entity_id,
                dimension=request.dimension,
                delta=request.delta,
                context_hash=request.context_hash,
                confidence=request.confidence
            )
            return kuro_pb2.MemoryStatus(success=True, message="Memory atom stored.")
        except Exception as e:
            return kuro_pb2.MemoryStatus(success=False, message=str(e))

    def UpdatePreference(self, request, context):
        """
        Update specific behavioral preferences based on reinforcement signals.
        """
        # We assume VM 1 sends a reinforcement signal (True/False)
        # This would usually come from the 'Analyst' or 'Reinforcement' layer
        # For now, we mock the magnitude.
        self.reinforce_engine.reinforce(request.key, request.value > 0.5)
        return kuro_pb2.MemoryStatus(success=True, message=f"Preference '{request.key}' reinforced.")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kuro_pb2_grpc.add_MemoryServiceServicer_to_server(MemoryServicer(), server)
    kuro_pb2_grpc.add_HealthServiceServicer_to_server(HealthServicer("Memory"), server)
    server.add_insecure_port('[::]:50053')
    print("Memory Substrate (VM 3) starting on port 50053...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
