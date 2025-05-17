from typing import List, Dict, Any, Optional, Tuple
import os
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
    def __init__(self, db_path: str = None):
        self.db = MetadataDatabase(db_path)
        self._ensure_root_directory_exists()
    
    def _ensure_root_directory_exists(self):
        # Verificar si existe el directorio raíz con path "/"
        root = self.db.get_file_by_path("/")
        if not root:
            # Crear el directorio raíz
            self.db.create_file(name="/", path="/", file_type=FileType.DIRECTORY)
    
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
    
    def delete_directory(self, directory_path: str) -> bool:
        dir_info = self.db.get_file_by_path(directory_path)
        if not dir_info or dir_info["type"] != FileType.DIRECTORY:
            return False
        
        # Obtener todos los archivos en el directorio y sus subdirectorios
        files = self.db.list_directory(directory_path)
        
        # Eliminar todos los bloques asociados a los archivos
        for file_data in files:
            if file_data["type"] == FileType.FILE:
                blocks = self.db.get_file_blocks(file_data["file_id"])
                for block in blocks:
                    self.db.delete_block(block["block_id"])
        
        # Eliminar el directorio y todo su contenido
        return self.db.delete_directory(directory_path) > 0
    
    # Métodos para gestionar bloques
    
    def create_block(self, file_id: str, size: int, checksum: Optional[str] = None) -> str:
        block_id = self.db.create_block(file_id, size, checksum)
        
        # Actualizar el tamaño del archivo
        file = self.db.get_file(file_id)
        if file:
            new_size = file["size"] + size
            self.db.update_file(file_id, size=new_size)
        
        return block_id
    
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
    
    def get_blocks_by_datanode(self, datanode_id: str) -> List[BlockInfo]:
        blocks_data = self.db.get_blocks_by_datanode(datanode_id)
        
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
