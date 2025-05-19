import logging
import threading
import time
import grpc
from concurrent import futures
from typing import Optional, Callable

from src.common.proto import namenode_pb2, namenode_pb2_grpc

class LeaderElection:
    def __init__(self, node_id: str, hostname: str, port: int, 
                 election_timeout: int = 5, heartbeat_interval: int = 1):
        """
        Inicializa el sistema de elección de líder.
        
        Args:
            node_id: ID único del NameNode
            hostname: Hostname del NameNode
            port: Puerto del NameNode
            election_timeout: Tiempo máximo para esperar respuesta en elección
            heartbeat_interval: Intervalo entre heartbeats
        """
        self.node_id = node_id
        self.hostname = hostname
        self.port = port
        self.election_timeout = election_timeout
        self.heartbeat_interval = heartbeat_interval
        
        self.is_leader = False
        self.current_leader: Optional[str] = None
        self.known_nodes = set()
        self._stop_event = threading.Event()
        self._election_thread = None
        self._heartbeat_thread = None
        
        self.logger = logging.getLogger("LeaderElection")
        
        # Callbacks
        self.on_leader_elected: Optional[Callable] = None
        self.on_leader_lost: Optional[Callable] = None
    
    def start(self):
        """Inicia el proceso de elección de líder."""
        if self._election_thread is not None and self._election_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        
        self._election_thread.start()
        self._heartbeat_thread.start()
        self.logger.info("Leader election system started")
    
    def stop(self):
        """Detiene el proceso de elección de líder."""
        if self._election_thread is None:
            return
        
        self._stop_event.set()
        self._election_thread.join()
        self._heartbeat_thread.join()
        self._election_thread = None
        self._heartbeat_thread = None
        self.logger.info("Leader election system stopped")
    
    def add_node(self, node_id: str, hostname: str, port: int):
        """Añade un nodo conocido al sistema."""
        self.known_nodes.add((node_id, hostname, port))
    
    def remove_node(self, node_id: str):
        """Elimina un nodo conocido del sistema."""
        self.known_nodes = {(nid, host, port) for nid, host, port in self.known_nodes if nid != node_id}
    
    def _election_loop(self):
        """Bucle principal de elección de líder."""
        while not self._stop_event.is_set():
            try:
                if not self.is_leader and not self.current_leader:
                    self._start_election()
                time.sleep(self.election_timeout)
            except Exception as e:
                self.logger.error(f"Error in election loop: {str(e)}")
    
    def _heartbeat_loop(self):
        """Bucle de envío de heartbeats."""
        while not self._stop_event.is_set():
            try:
                if self.is_leader:
                    self._send_heartbeat()
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                self.logger.error(f"Error in heartbeat loop: {str(e)}")
    
    def _start_election(self):
        """Inicia una nueva elección de líder."""
        self.logger.info("Starting leader election")
        
        # Enviar solicitud de voto a todos los nodos conocidos
        votes_received = 0
        for node_id, hostname, port in self.known_nodes:
            try:
                channel = grpc.insecure_channel(f"{hostname}:{port}")
                stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
                
                request = namenode_pb2.VoteRequest(
                    candidate_id=self.node_id,
                    term=1  # Implementar lógica de términos más adelante
                )
                
                response = stub.RequestVote(request)
                if response.vote_granted:
                    votes_received += 1
                
                channel.close()
            except Exception as e:
                self.logger.error(f"Error requesting vote from {node_id}: {str(e)}")
        
        # Si recibimos mayoría de votos, nos convertimos en líder
        if votes_received >= len(self.known_nodes) // 2:
            self._become_leader()
    
    def _become_leader(self):
        """Convierte este nodo en líder."""
        self.is_leader = True
        self.current_leader = self.node_id
        self.logger.info(f"Node {self.node_id} became leader")
        
        if self.on_leader_elected:
            self.on_leader_elected()
    
    def _send_heartbeat(self):
        """Envía heartbeat a todos los nodos conocidos."""
        for node_id, hostname, port in self.known_nodes:
            try:
                channel = grpc.insecure_channel(f"{hostname}:{port}")
                stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
                
                request = namenode_pb2.HeartbeatRequest(
                    leader_id=self.node_id,
                    term=1  # Implementar lógica de términos más adelante
                )
                
                stub.Heartbeat(request)
                channel.close()
            except Exception as e:
                self.logger.error(f"Error sending heartbeat to {node_id}: {str(e)}")
    
    def handle_vote_request(self, request: namenode_pb2.VoteRequest) -> namenode_pb2.VoteResponse:
        """Maneja una solicitud de voto de otro nodo."""
        # Por ahora, concedemos el voto si no somos líder
        vote_granted = not self.is_leader
        
        return namenode_pb2.VoteResponse(
            vote_granted=vote_granted,
            term=1  # Implementar lógica de términos más adelante
        )
    
    def handle_heartbeat(self, request: namenode_pb2.HeartbeatRequest) -> namenode_pb2.HeartbeatResponse:
        """Maneja un heartbeat del líder."""
        if request.leader_id != self.current_leader:
            self.current_leader = request.leader_id
            self.is_leader = False
            
            if self.on_leader_lost:
                self.on_leader_lost()
        
        return namenode_pb2.HeartbeatResponse(
            success=True,
            term=1  # Implementar lógica de términos más adelante
        ) 