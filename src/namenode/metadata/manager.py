from typing import List, Dict, Any, Optional, Tuple, Set
import os
import json
import pickle
import uuid
import logging
from datetime import datetime
from pathlib import Path

from src.namenode.metadata.database import MetadataDatabase
from src.namenode.api.models import (
    DataNodeInfo, 
    DataNodeStatus,
    BlockInfo, 
    BlockLocation,
    FileMetadata, 
    FileType,
    DirectoryListing
)

class MetadataManager:
    def __init__(self, db_path: str = None, node_id: str = None):
        self.db = MetadataDatabase(db_path)
        self.node_id = node_id or str(uuid.uuid4())
        self.known_nodes = set()  # Set of (node_id, hostname, port) tuples
        self._ensure_root_directory_exists()
    
    def _ensure_root_directory_exists(self):
        """
        Asegura que el directorio raíz existe en el sistema.
        """
        try:
            # Verificar si existe el directorio raíz con path "/"
            root = self.db.get_file_by_path("/")
            if not root:
                # Crear el directorio raíz
                root = self.db.create_file(
                    name="",  # El directorio raíz no tiene nombre
                    path="/",
                    file_type=FileType.DIRECTORY,
                    owner="system"
                )
                logging.info("Directorio raíz creado correctamente")
            else:
                logging.debug("El directorio raíz ya existe")
            return root
        except Exception as e:
            logging.error(f"Error al crear el directorio raíz: {e}")
            raise
    
    # Métodos para gestionar DataNodes
    
    def register_datanode(self, hostname: str, port: int, storage_capacity: int, available_space: int) -> DataNodeInfo:
        node_id = self.db.register_datanode(hostname, port, storage_capacity, available_space)
        return DataNodeInfo(
            node_id=node_id,
            hostname=hostname,
            port=port,
            status=DataNodeStatus.ACTIVE,
            storage_capacity=storage_capacity,
            available_space=available_space,
            last_heartbeat=datetime.now(),
            blocks_stored=0
        )
    
    def get_datanode(self, node_id: str) -> Optional[DataNodeInfo]:
        datanode = self.db.get_datanode(node_id)
        if not datanode:
            return None
        
        return DataNodeInfo(
            node_id=datanode["node_id"],
            hostname=datanode["hostname"],
            port=datanode["port"],
            status=datanode["status"],
            storage_capacity=datanode["storage_capacity"],
            available_space=datanode["available_space"],
            last_heartbeat=datanode["last_heartbeat"],
            blocks_stored=datanode["blocks_stored"]
        )
    
    def list_datanodes(self, status: Optional[str] = None) -> List[DataNodeInfo]:
        datanodes = self.db.list_datanodes(status)
        return [
            DataNodeInfo(
                node_id=dn["node_id"],
                hostname=dn["hostname"],
                port=dn["port"],
                status=dn["status"],
                storage_capacity=dn["storage_capacity"],
                available_space=dn["available_space"],
                last_heartbeat=dn["last_heartbeat"],
                blocks_stored=dn["blocks_stored"]
            )
            for dn in datanodes
        ]
    
    def update_datanode_heartbeat(self, node_id: str, available_space: int) -> bool:
        return self.db.update_datanode_heartbeat(node_id, available_space)
    
    def update_datanode_status(self, node_id: str, status: str) -> bool:
        return self.db.update_datanode_status(node_id, status)
    
    def delete_datanode(self, node_id: str) -> bool:
        """
        Elimina un DataNode del sistema.
        
        Args:
            node_id: ID del DataNode a eliminar
            
        Returns:
            True si se eliminó correctamente, False si no
        """
        try:
            # Verificar que el DataNode existe
            datanode = self.get_datanode(node_id)
            if not datanode:
                return False
            
            # Eliminar el DataNode y sus referencias
            success = self.db.delete_datanode(node_id)
            
            if success:
                logging.info(f"DataNode {node_id} eliminado correctamente")
            else:
                logging.error(f"Error al eliminar el DataNode {node_id}")
            
            return success
        except Exception as e:
            logging.error(f"Error al eliminar el DataNode {node_id}: {str(e)}")
            return False
    
    # Métodos para gestionar archivos y directorios
    
    def create_file(self, name: str, path: str, file_type: FileType, size: int = 0, owner: Optional[str] = None) -> Optional[FileMetadata]:
        parent_path = os.path.dirname(path)
        if parent_path and not self.db.get_file_by_path(parent_path):
            return None  # El directorio padre no existe
        
        file_id = self.db.create_file(name, path, file_type, size, owner)
        
        return FileMetadata(
            file_id=file_id,
            name=name,
            path=path,
            type=file_type,
            size=size,
            blocks=[],
            created_at=datetime.now(),
            modified_at=datetime.now(),
            owner=owner
        )
    
    def get_file(self, file_id: str) -> Optional[FileMetadata]:
        file_data = self.db.get_file(file_id)
        if not file_data:
            return None
        
        blocks = self.db.get_file_blocks(file_id)
        block_ids = [block["block_id"] for block in blocks]
        
        return FileMetadata(
            file_id=file_data["file_id"],
            name=file_data["name"],
            path=file_data["path"],
            type=file_data["type"],
            size=file_data["size"],
            blocks=block_ids,
            created_at=file_data["created_at"],
            modified_at=file_data["modified_at"],
            owner=file_data["owner"]
        )
    
    def get_file_by_path(self, path: str) -> Optional[FileMetadata]:
        file_data = self.db.get_file_by_path(path)
        if not file_data:
            return None
        
        file_id = file_data["file_id"]
        blocks = self.db.get_file_blocks(file_id)
        block_ids = [block["block_id"] for block in blocks]
        
        return FileMetadata(
            file_id=file_data["file_id"],
            name=file_data["name"],
            path=file_data["path"],
            type=file_data["type"],
            size=file_data["size"],
            blocks=block_ids,
            created_at=file_data["created_at"],
            modified_at=file_data["modified_at"],
            owner=file_data["owner"]
        )
    
    def list_directory(self, directory_path: str) -> DirectoryListing:
        files_data = self.db.list_directory(directory_path)
        
        contents = []
        for file_data in files_data:
            file_id = file_data["file_id"]
            blocks = self.db.get_file_blocks(file_id)
            block_ids = [block["block_id"] for block in blocks]
            
            contents.append(FileMetadata(
                file_id=file_data["file_id"],
                name=file_data["name"],
                path=file_data["path"],
                type=file_data["type"],
                size=file_data["size"],
                blocks=block_ids,
                created_at=file_data["created_at"],
                modified_at=file_data["modified_at"],
                owner=file_data["owner"]
            ))
        
        return DirectoryListing(
            path=directory_path,
            contents=contents
        )
    
    def update_file(self, file_id: str, **kwargs) -> bool:
        return self.db.update_file(file_id, **kwargs)
    
    def delete_file(self, file_id: str) -> bool:
        file = self.db.get_file(file_id)
        if not file:
            return False
        
        if file["type"] == FileType.DIRECTORY:
            return False  # No se puede eliminar un directorio con este método
        
        # Eliminar todos los bloques asociados al archivo
        blocks = self.db.get_file_blocks(file_id)
        for block in blocks:
            self.db.delete_block(block["block_id"])
        
        return self.db.delete_file(file_id)
    
    def delete_directory(self, directory_path: str, recursive: bool = False) -> bool:
        """
        Elimina un directorio y todo su contenido.
        
        Args:
            directory_path: Ruta del directorio a eliminar
            recursive: Si es True, elimina el directorio y todo su contenido
            
        Returns:
            bool: True si se eliminó correctamente, False en caso contrario
        """
        # Verificar que el directorio existe y es un directorio
        dir_info = self.get_file_by_path(directory_path)
        if not dir_info or dir_info.type != FileType.DIRECTORY:
            return False
        
        # Obtener el contenido del directorio
        listing = self.list_directory(directory_path)
        if listing.contents and not recursive:
            # El directorio no está vacío y no es recursivo
            return False
        
        if recursive:
            # Eliminar recursivamente todo el contenido
            for item in listing.contents:
                if item.type == FileType.DIRECTORY:
                    # Eliminar subdirectorios recursivamente
                    self.delete_directory(item.path, recursive=True)
                else:
                    # Eliminar archivos
                    self.delete_file(item.file_id)
        
        # Eliminar el directorio actual
        return self.db.delete_file(dir_info.file_id)
    
    # Métodos para gestionar bloques
    
    def create_block(self, file_id: str, size: int, checksum: Optional[str] = None, block_id: Optional[str] = None) -> str:
        """
        Crea un nuevo bloque en el sistema.
        
        Args:
            file_id: ID del archivo al que pertenece el bloque
            size: Tamaño del bloque en bytes
            checksum: Checksum del bloque (opcional)
            block_id: ID específico para el bloque (opcional)
            
        Returns:
            str: ID del bloque creado
        """
        block_id = block_id or str(uuid.uuid4())
        success = self.db.create_block(block_id, file_id, size, checksum)
        
        if success:
            # Actualizar el tamaño del archivo
            file = self.db.get_file(file_id)
            if file:
                new_size = file["size"] + size
                self.db.update_file(file_id, size=new_size)
            
            return block_id
        return None
    
    def get_block_info(self, block_id: str) -> Optional[BlockInfo]:
        block_data = self.db.get_block_with_locations(block_id)
        if not block_data:
            return None
        
        locations = [
            BlockLocation(
                block_id=block_id,
                datanode_id=loc["datanode_id"],
                is_leader=loc["is_leader"]
            )
            for loc in block_data["locations"]
        ]
        
        return BlockInfo(
            block_id=block_data["block_id"],
            file_id=block_data["file_id"],
            size=block_data["size"],
            locations=locations,
            checksum=block_data["checksum"]
        )
    
    def get_file_blocks(self, file_id: str) -> List[BlockInfo]:
        blocks_data = self.db.get_file_blocks(file_id)
        
        result = []
        for block_data in blocks_data:
            block_id = block_data["block_id"]
            locations = self.db.get_block_locations(block_id)
            
            block_locations = [
                BlockLocation(
                    block_id=block_id,
                    datanode_id=loc["datanode_id"],
                    is_leader=loc["is_leader"]
                )
                for loc in locations
            ]
            
            result.append(BlockInfo(
                block_id=block_data["block_id"],
                file_id=block_data["file_id"],
                size=block_data["size"],
                locations=block_locations,
                checksum=block_data["checksum"]
            ))
        
        return result
    
    def add_block_location(self, block_id: str, datanode_id: str, is_leader: bool = False) -> bool:
        success = self.db.add_block_location(block_id, datanode_id, is_leader)
        if success:
            self.db.update_datanode_blocks_count(datanode_id)
        return success
    
    def remove_block_location(self, block_id: str, datanode_id: str) -> bool:
        success = self.db.remove_block_location(block_id, datanode_id)
        if success:
            self.db.update_datanode_blocks_count(datanode_id)
        return success
    
    def get_blocks_by_datanode(self, node_id: str) -> List[BlockInfo]:
        """
        Obtiene todos los bloques almacenados en un DataNode específico.
        
        Args:
            node_id: ID del DataNode
            
        Returns:
            Lista de bloques almacenados en el DataNode
        """
        blocks = self.db.get_blocks_by_datanode(node_id)
        result = []
        
        for block in blocks:
            # Obtener todas las ubicaciones del bloque
            locations = self.db.get_block_locations(block["block_id"])
            block_locations = [
                BlockLocation(
                    block_id=block["block_id"],
                    datanode_id=loc["datanode_id"],
                    is_leader=loc["is_leader"]
                )
                for loc in locations
            ]
            
            result.append(BlockInfo(
                block_id=block["block_id"],
                file_id=block["file_id"],
                size=block["size"],
                checksum=block.get("checksum"),
                locations=block_locations
            ))
        
        return result
    
    def update_block(self, block_id: str, **kwargs) -> bool:
        """Actualiza la información de un bloque en la base de datos.
        
        Args:
            block_id: ID del bloque a actualizar
            **kwargs: Campos a actualizar (size, checksum, etc.)
            
        Returns:
            True si la actualización fue exitosa, False en caso contrario
        """
        return self.db.update_block(block_id, **kwargs)
    
    def close(self):
        self.db.close_connection()
        
    # Métodos para gestión de nodos conocidos
    
    def add_known_node(self, node_id: str, hostname: str, port: int) -> None:
        """
        Añade un nodo conocido a la lista de nodos del cluster.
        
        Args:
            node_id: ID único del nodo
            hostname: Hostname del nodo
            port: Puerto gRPC del nodo
        """
        self.known_nodes.add((node_id, hostname, port))
    
    def remove_known_node(self, node_id: str) -> None:
        """
        Elimina un nodo conocido de la lista de nodos del cluster.
        
        Args:
            node_id: ID único del nodo a eliminar
        """
        self.known_nodes = {(nid, host, port) for nid, host, port in self.known_nodes if nid != node_id}
    
    def get_known_nodes(self) -> Set[Tuple[str, str, int]]:
        """
        Obtiene la lista de nodos conocidos en el cluster.
        
        Returns:
            Conjunto de tuplas (node_id, hostname, port)
        """
        return self.known_nodes
    
    # Métodos para serialización y deserialización de metadatos
    
    def serialize_metadata(self) -> bytes:
        """
        Serializa los metadatos para sincronización entre nodos.
        
        Returns:
            Datos serializados en formato binario
        """
        # Obtener datos relevantes para sincronización
        datanodes = self.list_datanodes()
        files = []
        blocks = []
        
        # Obtener todos los archivos y sus bloques
        for file in self.db.list_all_files():
            files.append(file)
            file_blocks = self.get_file_blocks(file['file_id'])
            blocks.extend([block.dict() for block in file_blocks])
        
        # Crear diccionario con todos los metadatos
        metadata = {
            'datanodes': [dn.dict() for dn in datanodes],
            'files': files,
            'blocks': blocks
        }
        
        # Serializar usando pickle para mantener tipos complejos
        return pickle.dumps(metadata)
    
    def deserialize_metadata(self, data: bytes) -> bool:
        """
        Deserializa y aplica metadatos recibidos de otro nodo.
        
        Args:
            data: Datos serializados en formato binario
            
        Returns:
            True si la deserialización fue exitosa, False en caso contrario
        """
        try:
            metadata = pickle.loads(data)
            
            # Aplicar metadatos de DataNodes
            for dn_data in metadata.get('datanodes', []):
                # Verificar si el DataNode ya existe
                existing_dn = self.get_datanode(dn_data.get('node_id'))
                if not existing_dn:
                    # Registrar nuevo DataNode
                    self.db.register_datanode(
                        dn_data.get('hostname'),
                        dn_data.get('port'),
                        dn_data.get('storage_capacity'),
                        dn_data.get('available_space')
                    )
                else:
                    # Actualizar DataNode existente
                    self.db.update_datanode_status(dn_data.get('node_id'), dn_data.get('status'))
                    self.db.update_datanode_heartbeat(dn_data.get('node_id'), dn_data.get('available_space'))
            
            # Aplicar metadatos de archivos
            for file_data in metadata.get('files', []):
                existing_file = self.get_file(file_data.get('file_id'))
                if not existing_file:
                    # Crear nuevo archivo
                    self.db.create_file(
                        file_data.get('name'),
                        file_data.get('path'),
                        file_data.get('type'),
                        file_data.get('size'),
                        file_data.get('owner')
                    )
                else:
                    # Actualizar archivo existente
                    self.db.update_file(
                        file_data.get('file_id'),
                        size=file_data.get('size')
                    )
            
            # Aplicar metadatos de bloques
            for block_data in metadata.get('blocks', []):
                existing_block = self.get_block_info(block_data.get('block_id'))
                if not existing_block:
                    # Crear nuevo bloque
                    self.db.create_block(
                        block_data.get('file_id'),
                        block_data.get('size'),
                        block_data.get('checksum')
                    )
                
                # Actualizar ubicaciones del bloque
                for location in block_data.get('locations', []):
                    self.add_block_location(
                        block_data.get('block_id'),
                        location.get('datanode_id'),
                        location.get('is_leader', False)
                    )
            
            return True
        except Exception as e:
            logging.error(f"Error deserializing metadata: {str(e)}")
            return False

    def get_files_stats(self) -> Dict:
        """
        Obtiene estadísticas sobre los archivos en el sistema.
        
        Returns:
            Dict con estadísticas de archivos
        """
        try:
            total_files = self.db.count_files()
            total_size = self.db.get_total_files_size()
            
            return {
                "total_files": total_files,
                "total_size": total_size
            }
        except Exception as e:
            self.logger.error(f"Error al obtener estadísticas de archivos: {e}")
            return {
                "total_files": 0,
                "total_size": 0
            }

    def get_blocks_stats(self) -> Dict:
        """
        Obtiene estadísticas sobre los bloques en el sistema.
        
        Returns:
            Dict con estadísticas de bloques
        """
        try:
            total_blocks = self.db.count_blocks()
            total_size = self.db.get_total_blocks_size()
            replicated_blocks = self.db.count_replicated_blocks()
            
            return {
                "total_blocks": total_blocks,
                "total_size": total_size,
                "replicated_blocks": replicated_blocks,
                "replication_factor": self.replication_factor
            }
        except Exception as e:
            self.logger.error(f"Error al obtener estadísticas de bloques: {e}")
            return {
                "total_blocks": 0,
                "total_size": 0,
                "replicated_blocks": 0,
                "replication_factor": self.replication_factor
            }

    def get_file_info(self, path: str) -> Optional[Dict]:
        """
        Obtiene información detallada de un archivo incluyendo sus bloques y ubicaciones.
        
        Args:
            path: Ruta del archivo
            
        Returns:
            Diccionario con la información del archivo o None si no existe
        """
        try:
            # Obtener información básica del archivo
            file_data = self.get_file_by_path(path)
            if not file_data:
                return None
            
            # Obtener información de los bloques
            blocks = self.get_file_blocks(file_data.file_id)
            block_info = []
            
            for block in blocks:
                # Obtener ubicaciones del bloque
                locations = self.db.get_block_locations(block.block_id)
                active_locations = []
                
                for loc in locations:
                    try:
                        # Verificar si el DataNode está activo
                        datanode = self.get_datanode(loc["datanode_id"])
                        if datanode and datanode["status"] == "active":
                            active_locations.append({
                                "datanode_id": loc["datanode_id"],
                                "is_leader": loc["is_leader"],
                                "hostname": datanode["hostname"],
                                "port": datanode["port"]
                            })
                    except Exception as e:
                        print(f"Error al obtener información del DataNode {loc['datanode_id']}: {str(e)}")
                        continue
                
                block_info.append({
                    "block_id": block.block_id,
                    "size": block.size,
                    "checksum": block.checksum,
                    "locations": active_locations
                })
            
            # Construir la respuesta
            return {
                "file_id": file_data.file_id,
                "name": file_data.name,
                "path": file_data.path,
                "type": file_data.type,
                "size": file_data.size,
                "created_at": file_data.created_at.isoformat() if file_data.created_at else None,
                "modified_at": file_data.modified_at.isoformat() if file_data.modified_at else None,
                "owner": file_data.owner,
                "blocks": block_info,
                "total_blocks": len(block_info),
                "active_replicas": sum(len(block["locations"]) for block in block_info),
                "is_healthy": all(len(block["locations"]) >= 1 for block in block_info)
            }
            
        except Exception as e:
            import traceback
            error_detail = f"Error al obtener información del archivo {path}: {str(e)}\n{traceback.format_exc()}"
            print(error_detail)
            return None
