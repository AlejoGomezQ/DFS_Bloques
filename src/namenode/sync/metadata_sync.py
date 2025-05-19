import logging
import threading
import time
import grpc
from concurrent import futures
from typing import Optional, Callable

from src.namenode.metadata.manager import MetadataManager
from src.common.proto import namenode_pb2, namenode_pb2_grpc

class MetadataSync:
    def __init__(self, metadata_manager: MetadataManager, sync_interval: int = 5):
        """
        Inicializa el servicio de sincronización de metadatos.
        
        Args:
            metadata_manager: Instancia del gestor de metadatos
            sync_interval: Intervalo entre sincronizaciones en segundos
        """
        self.metadata_manager = metadata_manager
        self.sync_interval = sync_interval
        self._stop_event = threading.Event()
        self._sync_thread = None
        self.logger = logging.getLogger("MetadataSync")
        
        # Callbacks
        self.on_sync_complete: Optional[Callable] = None
    
    def start(self):
        """Inicia el proceso de sincronización."""
        if self._sync_thread is not None and self._sync_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._sync_thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._sync_thread.start()
        self.logger.info("Metadata sync service started")
    
    def stop(self):
        """Detiene el proceso de sincronización."""
        if self._sync_thread is None:
            return
        
        self._stop_event.set()
        self._sync_thread.join()
        self._sync_thread = None
        self.logger.info("Metadata sync service stopped")
    
    def _sync_loop(self):
        """Bucle principal de sincronización."""
        while not self._stop_event.is_set():
            try:
                self._sync_metadata()
                time.sleep(self.sync_interval)
            except Exception as e:
                self.logger.error(f"Error in sync loop: {str(e)}")
    
    def _sync_metadata(self):
        """Sincroniza los metadatos con otros NameNodes."""
        try:
            # Serializar metadatos
            metadata = self.metadata_manager.serialize_metadata()
            
            # Enviar metadatos a todos los nodos conocidos
            for node_id, hostname, port in self.metadata_manager.get_known_nodes():
                try:
                    channel = grpc.insecure_channel(f"{hostname}:{port}")
                    stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
                    
                    request = namenode_pb2.SyncRequest(
                        source_id=self.metadata_manager.node_id,
                        term=1,  # Implementar lógica de términos más adelante
                        metadata=metadata
                    )
                    
                    response = stub.SyncMetadata(request)
                    if response.success:
                        self.logger.info(f"Successfully synced metadata with {node_id}")
                    else:
                        self.logger.warning(f"Failed to sync metadata with {node_id}")
                    
                    channel.close()
                except Exception as e:
                    self.logger.error(f"Error syncing with {node_id}: {str(e)}")
            
            if self.on_sync_complete:
                self.on_sync_complete()
                
        except Exception as e:
            self.logger.error(f"Error in metadata sync: {str(e)}")
    
    def handle_sync_request(self, request: namenode_pb2.SyncRequest) -> namenode_pb2.SyncResponse:
        """
        Maneja una solicitud de sincronización de metadatos.
        
        Args:
            request: Solicitud de sincronización
            
        Returns:
            Respuesta de sincronización
        """
        try:
            # Deserializar y aplicar metadatos
            self.metadata_manager.deserialize_metadata(request.metadata)
            
            return namenode_pb2.SyncResponse(
                success=True,
                term=1  # Implementar lógica de términos más adelante
            )
        except Exception as e:
            self.logger.error(f"Error handling sync request: {str(e)}")
            return namenode_pb2.SyncResponse(
                success=False,
                term=1  # Implementar lógica de términos más adelante
            ) 