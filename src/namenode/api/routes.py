from fastapi import APIRouter, HTTPException, Path, Query, Body, Depends
from typing import List, Optional
import os

# Importación absoluta en lugar de relativa
from src.namenode.api.models import (
    DataNodeRegistration, 
    DataNodeInfo, 
    BlockInfo, 
    BlockLocation,
    FileMetadata, 
    DirectoryListing,
    ErrorResponse,
    FileType,
    HeartbeatRequest,
    BlockStatusInfo
)
from src.namenode.metadata.manager import MetadataManager

# Singleton instance of MetadataManager
metadata_manager = MetadataManager()

# Dependency to get the metadata manager
def get_metadata_manager():
    return metadata_manager

# Routers
files_router = APIRouter(prefix="/files", tags=["Files"])
blocks_router = APIRouter(prefix="/blocks", tags=["Blocks"])
datanodes_router = APIRouter(prefix="/datanodes", tags=["DataNodes"])
directories_router = APIRouter(prefix="/directories", tags=["Directories"])

# Files Endpoints
@files_router.post("/", response_model=FileMetadata, status_code=201)
async def create_file(file_metadata: FileMetadata, manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Register a new file in the system.
    """
    # Verificar si ya existe un archivo con la misma ruta
    existing_file = manager.get_file_by_path(file_metadata.path)
    if existing_file:
        raise HTTPException(status_code=409, detail=f"File already exists at path: {file_metadata.path}")
    
    # Verificar que el directorio padre existe
    parent_path = os.path.dirname(file_metadata.path)
    if parent_path and not manager.get_file_by_path(parent_path):
        raise HTTPException(status_code=404, detail=f"Parent directory does not exist: {parent_path}")
    
    # Crear el archivo en el sistema
    created_file = manager.create_file(
        name=file_metadata.name,
        path=file_metadata.path,
        file_type=file_metadata.type,
        size=file_metadata.size,
        owner=file_metadata.owner
    )
    
    if not created_file:
        raise HTTPException(status_code=500, detail="Failed to create file")
    
    return created_file

@files_router.get("/{file_id}", response_model=FileMetadata)
async def get_file(file_id: str = Path(..., description="The ID of the file to retrieve"), manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Get file metadata by ID.
    """
    file = manager.get_file(file_id)
    if not file:
        raise HTTPException(status_code=404, detail=f"File not found with ID: {file_id}")
    
    return file

@files_router.delete("/{file_id}", status_code=204)
async def delete_file(file_id: str = Path(..., description="The ID of the file to delete"), manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Delete a file and its associated blocks.
    """
    file = manager.get_file(file_id)
    if not file:
        raise HTTPException(status_code=404, detail=f"File not found with ID: {file_id}")
    
    if file.type == FileType.DIRECTORY:
        raise HTTPException(status_code=400, detail="Cannot delete directory using this endpoint. Use /directories endpoint instead.")
    
    success = manager.delete_file(file_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete file")
    
    return None

@files_router.get("/path/{path:path}", response_model=FileMetadata)
async def get_file_by_path(path: str = Path(..., description="The path of the file to retrieve"), manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Get file metadata by path.
    """
    file = manager.get_file_by_path(path)
    if not file:
        raise HTTPException(status_code=404, detail=f"File not found at path: {path}")
    
    return file

# Blocks Endpoints
@blocks_router.get("/{block_id}", response_model=BlockInfo)
async def get_block_info(block_id: str = Path(..., description="The ID of the block to retrieve"), manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Get information about a specific block.
    """
    block = manager.get_block_info(block_id)
    if not block:
        raise HTTPException(status_code=404, detail=f"Block not found with ID: {block_id}")
    
    return block

@blocks_router.get("/file/{file_id}", response_model=List[BlockInfo])
async def get_file_blocks(file_id: str = Path(..., description="The ID of the file to retrieve blocks for"), manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Get all blocks associated with a file.
    """
    file = manager.get_file(file_id)
    if not file:
        raise HTTPException(status_code=404, detail=f"File not found with ID: {file_id}")
    
    if file.type == FileType.DIRECTORY:
        raise HTTPException(status_code=400, detail="Directories do not have blocks")
    
    return manager.get_file_blocks(file_id)

@blocks_router.post("/", response_model=BlockInfo, status_code=201)
async def create_block(block_info: BlockInfo = Body(..., description="Block information"), manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Register a new block in the system.
    """
    # Verificar que el archivo existe
    file = manager.get_file(block_info.file_id)
    if not file:
        raise HTTPException(status_code=404, detail=f"File not found with ID: {block_info.file_id}")
    
    if file.type == FileType.DIRECTORY:
        raise HTTPException(status_code=400, detail="Cannot add blocks to directories")
    
    # Crear el bloque
    block_id = manager.create_block(
        file_id=block_info.file_id,
        size=block_info.size,
        checksum=block_info.checksum
    )
    
    # Si se proporcionaron ubicaciones, registrarlas
    for location in block_info.locations:
        # Verificar que el DataNode existe
        datanode = manager.get_datanode(location.datanode_id)
        if not datanode:
            continue
        
        # Añadir la ubicación del bloque
        manager.add_block_location(
            block_id=block_id,
            datanode_id=location.datanode_id,
            is_leader=location.is_leader
        )
    
    # Devolver la información del bloque creado
    return manager.get_block_info(block_id)

@blocks_router.post("/report", status_code=204)
async def report_block_status(block_reports: List[BlockInfo], manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Report the status of blocks from a DataNode.
    """
    for block_report in block_reports:
        # Verificar si el bloque existe
        existing_block = manager.get_block_info(block_report.block_id)
        if not existing_block:
            # Crear el bloque si no existe
            manager.create_block(
                file_id=block_report.file_id,
                size=block_report.size,
                checksum=block_report.checksum
            )
        
        # Actualizar las ubicaciones del bloque
        for location in block_report.locations:
            # Verificar si el DataNode existe
            datanode = manager.get_datanode(location.datanode_id)
            if not datanode:
                continue
            
            # Añadir la ubicación del bloque
            manager.add_block_location(
                block_id=block_report.block_id,
                datanode_id=location.datanode_id,
                is_leader=location.is_leader
            )
    
    return None

@blocks_router.put("/{block_id}", response_model=BlockInfo)
async def update_block_info(block_id: str = Path(..., description="The ID of the block to update"), 
                           block_info: BlockInfo = Body(..., description="Updated block information"),
                           manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Update information about a specific block.
    """
    # Verificar que el bloque existe
    existing_block = manager.get_block_info(block_id)
    if not existing_block:
        raise HTTPException(status_code=404, detail=f"Block not found with ID: {block_id}")
    
    # Verificar que el ID del bloque coincide
    if block_id != block_info.block_id:
        raise HTTPException(status_code=400, detail="Block ID in path does not match block ID in request body")
    
    # Actualizar la información del bloque
    manager.update_block(block_id, size=block_info.size, checksum=block_info.checksum)
    
    # Devolver la información actualizada del bloque
    return manager.get_block_info(block_id)

@blocks_router.post("/{block_id}/locations", status_code=201)
async def add_block_location(block_id: str = Path(..., description="The ID of the block"),
                            location: BlockLocation = Body(..., description="Location information"),
                            manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Add a new location for a block.
    """
    # Verificar que el bloque existe
    existing_block = manager.get_block_info(block_id)
    if not existing_block:
        raise HTTPException(status_code=404, detail=f"Block not found with ID: {block_id}")
    
    # Verificar que el ID del bloque coincide
    if block_id != location.block_id:
        raise HTTPException(status_code=400, detail="Block ID in path does not match block ID in request body")
    
    # Verificar que el DataNode existe
    datanode = manager.get_datanode(location.datanode_id)
    if not datanode:
        raise HTTPException(status_code=404, detail=f"DataNode not found with ID: {location.datanode_id}")
    
    # Añadir la ubicación del bloque
    success = manager.add_block_location(
        block_id=block_id,
        datanode_id=location.datanode_id,
        is_leader=location.is_leader
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to add block location")
    
    # Devolver la información actualizada del bloque
    return manager.get_block_info(block_id)

@blocks_router.delete("/{block_id}/locations/{datanode_id}", status_code=204)
async def remove_block_location(block_id: str = Path(..., description="The ID of the block"),
                              datanode_id: str = Path(..., description="The ID of the DataNode"),
                              manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Remove a location for a block.
    """
    # Verificar que el bloque existe
    existing_block = manager.get_block_info(block_id)
    if not existing_block:
        raise HTTPException(status_code=404, detail=f"Block not found with ID: {block_id}")
    
    # Verificar que el DataNode existe
    datanode = manager.get_datanode(datanode_id)
    if not datanode:
        raise HTTPException(status_code=404, detail=f"DataNode not found with ID: {datanode_id}")
    
    # Eliminar la ubicación del bloque
    success = manager.remove_block_location(block_id, datanode_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Block location not found for block {block_id} and DataNode {datanode_id}")
    
    return None

# DataNodes Endpoints
@datanodes_router.post("/register", response_model=DataNodeInfo, status_code=201)
async def register_datanode(registration: DataNodeRegistration, manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Register a new DataNode in the system.
    """
    datanode = manager.register_datanode(
        hostname=registration.hostname,
        port=registration.port,
        storage_capacity=registration.storage_capacity,
        available_space=registration.available_space
    )
    
    return datanode

@datanodes_router.get("/", response_model=List[DataNodeInfo])
async def list_datanodes(status: Optional[str] = Query(None, description="Filter by DataNode status"), manager: MetadataManager = Depends(get_metadata_manager)):
    """
    List all registered DataNodes.
    """
    return manager.list_datanodes(status)

@datanodes_router.get("/{node_id}", response_model=DataNodeInfo)
async def get_datanode(node_id: str = Path(..., description="The ID of the DataNode to retrieve"), manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Get information about a specific DataNode.
    """
    datanode = manager.get_datanode(node_id)
    if not datanode:
        raise HTTPException(status_code=404, detail=f"DataNode not found with ID: {node_id}")
    
    return datanode

@datanodes_router.post("/{node_id}/heartbeat", status_code=204)
async def datanode_heartbeat(
    node_id: str = Path(..., description="The ID of the DataNode sending the heartbeat"),
    heartbeat: HeartbeatRequest = Body(..., description="Heartbeat information including available space and blocks"),
    manager: MetadataManager = Depends(get_metadata_manager)
):
    """
    Process a heartbeat from a DataNode.
    """
    datanode = manager.get_datanode(node_id)
    if not datanode:
        raise HTTPException(status_code=404, detail=f"DataNode not found with ID: {node_id}")
    
    # Verificar que el node_id en la ruta coincide con el del cuerpo
    if node_id != heartbeat.node_id:
        raise HTTPException(status_code=400, detail="Node ID in path does not match node ID in request body")
    
    # Actualizar el heartbeat y el espacio disponible
    success = manager.update_datanode_heartbeat(node_id, heartbeat.available_space)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update DataNode heartbeat")
    
    # Procesar la información de los bloques
    for block_id, block_info in heartbeat.blocks.items():
        # Verificar si el bloque ya existe en el sistema
        existing_block = manager.get_block_info(block_id)
        
        if not existing_block:
            # Si el bloque no existe, lo registramos como un bloque huérfano
            # En una implementación completa, podríamos verificar si pertenece a algún archivo
            continue
        
        # Actualizar la ubicación del bloque
        manager.add_block_location(block_id, node_id, False)  # Por ahora, no es líder
    
    return None

# Directories Endpoints
@directories_router.post("/", response_model=FileMetadata, status_code=201)
async def create_directory(directory: FileMetadata, manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Create a new directory.
    """
    if directory.type != FileType.DIRECTORY:
        raise HTTPException(status_code=400, detail="Type must be 'directory'")
    
    # Verificar si ya existe un directorio con la misma ruta
    existing_dir = manager.get_file_by_path(directory.path)
    if existing_dir:
        raise HTTPException(status_code=409, detail=f"Directory already exists at path: {directory.path}")
    
    # Verificar que el directorio padre existe, excepto para el directorio raíz
    if directory.path == "/":
        # Caso especial para el directorio raíz
        pass
    else:
        parent_path = os.path.dirname(directory.path)
        if parent_path and not manager.get_file_by_path(parent_path):
            raise HTTPException(status_code=404, detail=f"Parent directory does not exist: {parent_path}")
    
    # Crear el directorio en el sistema
    created_dir = manager.create_file(
        name=directory.name,
        path=directory.path,
        file_type=FileType.DIRECTORY,
        owner=directory.owner
    )
    
    if not created_dir:
        raise HTTPException(status_code=500, detail="Failed to create directory")
    
    return created_dir

@directories_router.get("/{path:path}", response_model=DirectoryListing)
async def list_directory(path: str = Path(..., description="The path of the directory to list"), manager: MetadataManager = Depends(get_metadata_manager)):
    """
    List the contents of a directory.
    """
    # Verificar si el directorio existe
    dir_info = manager.get_file_by_path(path)
    if not dir_info:
        # Si es la raíz y no existe, intentar crearla
        if path == "" or path == "/":
            manager.create_file(name="", path="", file_type=FileType.DIRECTORY)
        else:
            raise HTTPException(status_code=404, detail=f"Directory not found at path: {path}")
    elif dir_info.type != FileType.DIRECTORY:
        raise HTTPException(status_code=400, detail=f"Path is not a directory: {path}")
    
    return manager.list_directory(path)

@directories_router.delete("/{path:path}", status_code=204)
async def delete_directory(path: str = Path(..., description="The path of the directory to delete"), manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Delete a directory and all its contents.
    """
    # Verificar si el directorio existe
    dir_info = manager.get_file_by_path(path)
    if not dir_info:
        raise HTTPException(status_code=404, detail=f"Directory not found at path: {path}")
    
    if dir_info.type != FileType.DIRECTORY:
        raise HTTPException(status_code=400, detail=f"Path is not a directory: {path}")
    
    # No permitir eliminar el directorio raíz
    if path == "" or path == "/":
        raise HTTPException(status_code=400, detail="Cannot delete root directory")
    
    success = manager.delete_directory(path)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete directory")
    
    return None
