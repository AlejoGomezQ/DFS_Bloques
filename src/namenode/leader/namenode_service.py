import logging
import grpc
from concurrent import futures
from typing import Optional

from src.common.proto import namenode_pb2, namenode_pb2_grpc
from src.namenode.leader.leader_election import LeaderElection
from src.namenode.sync.metadata_sync import MetadataSync
from src.namenode.metadata.manager import MetadataManager

class NameNodeServicer(namenode_pb2_grpc.NameNodeServiceServicer):
    """
    Implementación del servicio gRPC para comunicación entre NameNodes.
    """
    def __init__(self, leader_election: LeaderElection, metadata_sync: MetadataSync):
        self.leader_election = leader_election
        self.metadata_sync = metadata_sync
        self.logger = logging.getLogger("NameNodeServicer")
    
    def RequestVote(self, request, context):
        """
        Maneja solicitudes de voto para la elección de líder.
        """
        self.logger.info(f"Received vote request from {request.candidate_id}")
        return self.leader_election.handle_vote_request(request)
    
    def Heartbeat(self, request, context):
        """
        Maneja heartbeats del líder.
        """
        self.logger.debug(f"Received heartbeat from leader {request.leader_id}")
        return self.leader_election.handle_heartbeat(request)
    
    def SyncMetadata(self, request, context):
        """
        Maneja solicitudes de sincronización de metadatos.
        """
        self.logger.info(f"Received metadata sync request from {request.source_id}")
        return self.metadata_sync.handle_sync_request(request)

def serve(node_id: str, hostname: str, port: int, metadata_manager: MetadataManager, 
          leader_election: Optional[LeaderElection] = None, 
          metadata_sync: Optional[MetadataSync] = None):
    """
    Inicia el servidor gRPC para el servicio de NameNode.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Crear o usar instancias existentes de LeaderElection y MetadataSync
    if leader_election is None:
        leader_election = LeaderElection(node_id, hostname, port)
    
    if metadata_sync is None:
        metadata_sync = MetadataSync(metadata_manager)
    
    # Registrar el servicio
    servicer = NameNodeServicer(leader_election, metadata_sync)
    namenode_pb2_grpc.add_NameNodeServiceServicer_to_server(servicer, server)
    
    # Iniciar el servidor
    server_address = f"{hostname}:{port}"
    server.add_insecure_port(server_address)
    server.start()
    
    logging.info(f"NameNode gRPC server running at {server_address}")
    return server, leader_election, metadata_sync
