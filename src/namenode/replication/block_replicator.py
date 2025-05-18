import logging
from typing import List, Dict, Optional
import grpc
from concurrent import futures

from src.namenode.metadata.manager import MetadataManager
from src.namenode.api.models import BlockInfo, DataNodeInfo
from src.common.proto import datanode_pb2, datanode_pb2_grpc

class BlockReplicator:
    def __init__(self, metadata_manager: MetadataManager, replication_factor: int = 2):
        """
        Inicializa el replicador de bloques.
        
        Args:
            metadata_manager: Instancia del gestor de metadatos
            replication_factor: Número de copias que se deben mantener de cada bloque
        """
        self.metadata_manager = metadata_manager
        self.replication_factor = replication_factor
        self.logger = logging.getLogger("BlockReplicator")
    
    def handle_block_replication(self, block_id: str, failed_node_id: str) -> bool:
        """
        Maneja la re-replicación de un bloque después de un fallo.
        
        Args:
            block_id: ID del bloque a re-replicar
            failed_node_id: ID del DataNode que falló
            
        Returns:
            bool: True si la re-replicación fue exitosa, False en caso contrario
        """
        try:
            # Obtener información del bloque
            block_info = self.metadata_manager.get_block_info(block_id)
            if not block_info:
                self.logger.error(f"Block {block_id} not found")
                return False
            
            # Obtener los DataNodes activos que tienen el bloque
            active_locations = [
                loc for loc in block_info.locations 
                if loc.datanode_id != failed_node_id
            ]
            
            if not active_locations:
                self.logger.error(f"No active locations found for block {block_id}")
                return False
            
            # Seleccionar un DataNode fuente (preferentemente el líder)
            source_location = next(
                (loc for loc in active_locations if loc.is_leader),
                active_locations[0]
            )
            
            # Obtener información del DataNode fuente
            source_datanode = self.metadata_manager.get_datanode(source_location.datanode_id)
            if not source_datanode:
                self.logger.error(f"Source DataNode {source_location.datanode_id} not found")
                return False
            
            # Seleccionar un nuevo DataNode destino
            target_datanode = self._select_target_datanode(block_info.size)
            if not target_datanode:
                self.logger.error("No suitable target DataNode found")
                return False
            
            # Realizar la re-replicación
            success = self._replicate_block(
                block_id=block_id,
                source_datanode=source_datanode,
                target_datanode=target_datanode
            )
            
            if success:
                # Actualizar la ubicación del bloque en el metadata
                self.metadata_manager.add_block_location(
                    block_id=block_id,
                    datanode_id=target_datanode.node_id,
                    is_leader=False
                )
                self.logger.info(f"Block {block_id} successfully re-replicated to {target_datanode.node_id}")
                return True
            else:
                self.logger.error(f"Failed to re-replicate block {block_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error handling block replication: {str(e)}")
            return False
    
    def _select_target_datanode(self, block_size: int) -> Optional[DataNodeInfo]:
        """
        Selecciona un DataNode adecuado para almacenar el bloque.
        
        Args:
            block_size: Tamaño del bloque en bytes
            
        Returns:
            Optional[DataNodeInfo]: DataNode seleccionado o None si no hay uno adecuado
        """
        # Obtener todos los DataNodes activos
        datanodes = self.metadata_manager.list_datanodes(status="active")
        
        # Filtrar los DataNodes que tienen suficiente espacio disponible
        eligible_datanodes = [
            dn for dn in datanodes 
            if dn.available_space >= block_size
        ]
        
        if not eligible_datanodes:
            return None
        
        # Ordenar por espacio disponible (de mayor a menor)
        eligible_datanodes.sort(key=lambda dn: dn.available_space, reverse=True)
        
        # Seleccionar el DataNode con más espacio disponible
        return eligible_datanodes[0]
    
    def _replicate_block(
        self,
        block_id: str,
        source_datanode: DataNodeInfo,
        target_datanode: DataNodeInfo
    ) -> bool:
        """
        Realiza la replicación del bloque entre DataNodes.
        
        Args:
            block_id: ID del bloque a replicar
            source_datanode: DataNode fuente
            target_datanode: DataNode destino
            
        Returns:
            bool: True si la replicación fue exitosa, False en caso contrario
        """
        try:
            # Establecer conexión con el DataNode fuente
            source_channel = grpc.insecure_channel(f"{source_datanode.hostname}:{source_datanode.port}")
            source_stub = datanode_pb2_grpc.DataNodeServiceStub(source_channel)
            
            # Crear la solicitud de replicación
            replication_request = datanode_pb2.ReplicationRequest(
                block_id=block_id,
                target_datanode_id=target_datanode.node_id,
                target_hostname=target_datanode.hostname,
                target_port=target_datanode.port
            )
            
            # Realizar la replicación
            response = source_stub.ReplicateBlock(replication_request)
            
            # Cerrar la conexión
            source_channel.close()
            
            return response.status == datanode_pb2.BlockResponse.SUCCESS
            
        except Exception as e:
            self.logger.error(f"Error during block replication: {str(e)}")
            return False 