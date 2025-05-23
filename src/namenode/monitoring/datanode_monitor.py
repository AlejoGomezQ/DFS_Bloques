import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List

from src.namenode.metadata.manager import MetadataManager
from src.namenode.api.models import DataNodeStatus
from src.namenode.replication.block_replicator import BlockReplicator

class DataNodeMonitor:
    def __init__(self, metadata_manager: MetadataManager, heartbeat_timeout: int = 60, cleanup_interval: int = 3600, min_inactive_time: int = 7200):
        """
        Inicializa el monitor de DataNodes.
        
        Args:
            metadata_manager: Instancia del gestor de metadatos
            heartbeat_timeout: Tiempo en segundos para considerar un DataNode como caído
            cleanup_interval: Intervalo en segundos para ejecutar la limpieza automática
            min_inactive_time: Tiempo mínimo en segundos que un DataNode debe estar inactivo para ser eliminado
        """
        self.metadata_manager = metadata_manager
        self.heartbeat_timeout = heartbeat_timeout
        self.cleanup_interval = cleanup_interval
        self.min_inactive_time = min_inactive_time
        self.logger = logging.getLogger("DataNodeMonitor")
        self._stop_event = threading.Event()
        self._monitor_thread = None
        self._cleanup_thread = None
        self.block_replicator = BlockReplicator(metadata_manager)
    
    def start(self):
        """Inicia los hilos de monitoreo y limpieza."""
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        
        self._monitor_thread.start()
        self._cleanup_thread.start()
        
        self.logger.info("DataNode monitor and cleanup threads started")
    
    def stop(self):
        """Detiene los hilos de monitoreo y limpieza."""
        if self._monitor_thread is None:
            return
        
        self._stop_event.set()
        self._monitor_thread.join()
        if self._cleanup_thread:
            self._cleanup_thread.join()
        
        self._monitor_thread = None
        self._cleanup_thread = None
        self.logger.info("DataNode monitor and cleanup threads stopped")
    
    def _monitor_loop(self):
        """Bucle principal de monitoreo."""
        while not self._stop_event.is_set():
            try:
                self._check_datanodes()
                time.sleep(10)  # Verificar cada 10 segundos
            except Exception as e:
                self.logger.error(f"Error in monitor loop: {str(e)}")
    
    def _check_datanodes(self):
        """Verifica el estado de todos los DataNodes."""
        datanodes = self.metadata_manager.list_datanodes()
        current_time = datetime.now()
        
        for datanode in datanodes:
            try:
                if datanode.status == DataNodeStatus.ACTIVE:
                    # Verificar si el último heartbeat está dentro del timeout
                    if datanode.last_heartbeat:
                        time_since_heartbeat = current_time - datanode.last_heartbeat
                        if time_since_heartbeat > timedelta(seconds=self.heartbeat_timeout):
                            self.logger.warning(f"DataNode {datanode.node_id} heartbeat timeout")
                            self._handle_datanode_failure(datanode.node_id)
            except Exception as e:
                self.logger.error(f"Error checking DataNode {datanode.node_id}: {str(e)}")
    
    def _handle_datanode_failure(self, node_id: str):
        """
        Maneja el fallo de un DataNode.
        
        Args:
            node_id: ID del DataNode que falló
        """
        try:
            # Marcar el DataNode como inactivo
            self.metadata_manager.update_datanode_status(node_id, DataNodeStatus.INACTIVE)
            self.logger.info(f"DataNode {node_id} marked as inactive")
            
            # Obtener los bloques almacenados en el DataNode
            blocks = self.metadata_manager.get_blocks_by_datanode(node_id)
            
            # Re-replicar cada bloque afectado
            for block in blocks:
                self.logger.info(f"Re-replicating block {block.block_id} due to DataNode {node_id} failure")
                success = self.block_replicator.handle_block_replication(block.block_id, node_id)
                
                if success:
                    self.logger.info(f"Block {block.block_id} successfully re-replicated")
                else:
                    self.logger.error(f"Failed to re-replicate block {block.block_id}")
        except Exception as e:
            self.logger.error(f"Error handling DataNode {node_id} failure: {str(e)}")
    
    def get_datanode_health(self, node_id: str) -> Dict:
        """
        Obtiene el estado de salud de un DataNode.
        
        Args:
            node_id: ID del DataNode
            
        Returns:
            Diccionario con información del estado de salud
        """
        try:
            datanode = self.metadata_manager.get_datanode(node_id)
            if not datanode:
                return {
                    "status": "unknown",
                    "last_heartbeat": None,
                    "time_since_heartbeat": None
                }
            
            current_time = datetime.now()
            time_since_heartbeat = None
            if datanode.last_heartbeat:
                time_since_heartbeat = (current_time - datanode.last_heartbeat).total_seconds()
            
            return {
                "status": datanode.status,
                "last_heartbeat": datanode.last_heartbeat,
                "time_since_heartbeat": time_since_heartbeat,
                "is_healthy": time_since_heartbeat is None or time_since_heartbeat <= self.heartbeat_timeout
            }
        except Exception as e:
            self.logger.error(f"Error getting DataNode {node_id} health: {str(e)}")
            return {
                "status": "error",
                "last_heartbeat": None,
                "time_since_heartbeat": None,
                "is_healthy": False
            }
    
    def _cleanup_loop(self):
        """Bucle de limpieza automática de DataNodes inactivos."""
        while not self._stop_event.is_set():
            try:
                self._cleanup_inactive_datanodes()
                # Esperar el intervalo de limpieza o hasta que se solicite la detención
                self._stop_event.wait(self.cleanup_interval)
            except Exception as e:
                self.logger.error(f"Error in cleanup loop: {str(e)}")
    
    def _cleanup_inactive_datanodes(self):
        """Elimina los DataNodes que han estado inactivos por más del tiempo mínimo."""
        try:
            datanodes = self.metadata_manager.list_datanodes()
            current_time = datetime.now()
            deleted_count = 0
            
            for datanode in datanodes:
                if datanode.status == DataNodeStatus.INACTIVE and datanode.last_heartbeat:
                    inactive_time = (current_time - datanode.last_heartbeat).total_seconds()
                    if inactive_time >= self.min_inactive_time:
                        self.metadata_manager.delete_datanode(datanode.node_id)
                        deleted_count += 1
                        self.logger.info(f"Auto-deleted inactive DataNode {datanode.node_id}")
            
            if deleted_count > 0:
                self.logger.info(f"Automatic cleanup completed. Deleted {deleted_count} inactive DataNodes")
        
        except Exception as e:
            self.logger.error(f"Error during automatic DataNode cleanup: {str(e)}") 