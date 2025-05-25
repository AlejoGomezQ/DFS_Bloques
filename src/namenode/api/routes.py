from fastapi import FastAPI, APIRouter, HTTPException, Path, Query, Body, Depends
from typing import List, Optional
import os
from datetime import datetime

app = FastAPI()  # Crear instancia de FastAPI

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
    BlockStatusInfo,
    DataNodeStatus
)
from src.namenode.metadata.manager import MetadataManager
from src.namenode.api.dependencies import get_metadata_manager

# Routers
files_router = APIRouter(prefix="/files", tags=["Files"])
blocks_router = APIRouter(prefix="/blocks", tags=["Blocks"])
datanodes_router = APIRouter(prefix="/datanodes", tags=["DataNodes"])
directories_router = APIRouter(prefix="/directories", tags=["Directories"])
system_router = APIRouter(tags=["System"])  # Sin prefijo para que funcione con la ruta /system/stats

# Registrar los routers
app.include_router(files_router)
app.include_router(blocks_router)
app.include_router(datanodes_router)
app.include_router(directories_router)
app.include_router(system_router)

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

@files_router.get("/info/{path:path}")
async def get_file_info(path: str, manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Obtiene información detallada de un archivo.
    """
    try:
        # Primero verificar si el archivo existe
        file = manager.get_file_by_path(path)
        if not file:
            raise HTTPException(
                status_code=404, 
                detail=f"El archivo {path} no existe en el sistema"
            )
        
        # Obtener información detallada
        file_info = manager.get_file_info(path)
        if not file_info:
            raise HTTPException(
                status_code=500,
                detail=f"Error al obtener información detallada del archivo {path}"
            )
        
        # Agregar información temporal si no existe
        if 'created_at' not in file_info:
            file_info['created_at'] = datetime.now().isoformat()
        if 'modified_at' not in file_info:
            file_info['modified_at'] = datetime.now().isoformat()
        
        return file_info
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = f"Error interno al procesar la solicitud: {str(e)}\n{traceback.format_exc()}"
        print(error_detail)
        raise HTTPException(status_code=500, detail=error_detail)

@files_router.get("/blocks/{path:path}")
async def get_file_blocks(path: str, manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Obtiene información de los bloques de un archivo.
    """
    try:
        # Primero obtener el archivo
        file = manager.get_file_by_path(path)
        if not file:
            raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {path}")
        
        # Obtener los bloques del archivo
        blocks = manager.get_file_blocks(file.file_id)
        if not blocks:
            return {"blocks": []}
        
        # Convertir los bloques a diccionarios
        blocks_dict = []
        for block in blocks:
            block_dict = {
                "block_id": getattr(block, 'block_id', None),
                "file_id": getattr(block, 'file_id', None),
                "size": getattr(block, 'size', 0),
                "checksum": getattr(block, 'checksum', None),
                "locations": []
            }
            
            # Obtener las ubicaciones del bloque
            try:
                locations = manager.db.get_block_locations(block.block_id)
                if locations:
                    block_dict["locations"] = [
                        {
                            "datanode_id": loc.datanode_id,
                            "is_leader": loc.is_leader
                        }
                        for loc in locations
                    ]
            except Exception as e:
                print(f"Error al obtener ubicaciones del bloque {block.block_id}: {e}")
            
            blocks_dict.append(block_dict)
        
        return {"blocks": blocks_dict}
    except Exception as e:
        import traceback
        error_detail = f"{str(e)}\n{traceback.format_exc()}"
        print(f"Error en get_file_blocks: {error_detail}")
        raise HTTPException(status_code=500, detail=error_detail)

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
    try:
        # Verificar que el archivo existe
        file = manager.get_file(block_info.file_id)
        if not file:
            raise HTTPException(status_code=404, detail=f"File not found with ID: {block_info.file_id}")
        
        if file.type == FileType.DIRECTORY:
            raise HTTPException(status_code=400, detail="Cannot add blocks to directories")
        
        # Verificar que el bloque no existe ya
        existing_block = manager.get_block_info(block_info.block_id)
        if existing_block:
            # Si el bloque ya existe, actualizamos su información
            manager.update_block(block_info.block_id, size=block_info.size, checksum=block_info.checksum)
        else:
            # Crear el bloque nuevo
            manager.create_block(
                file_id=block_info.file_id,
                size=block_info.size,
                checksum=block_info.checksum,
                block_id=block_info.block_id
            )
        
        # Procesar las ubicaciones del bloque
        if block_info.locations:
            for location in block_info.locations:
                # Verificar que el DataNode existe y está activo
                datanode = manager.get_datanode(location.datanode_id)
                if not datanode or datanode.status != "active":
                    continue
                
                # Añadir la ubicación del bloque
                try:
                    manager.add_block_location(
                        block_id=location.block_id,
                        datanode_id=location.datanode_id,
                        is_leader=location.is_leader
                    )
                except Exception as e:
                    print(f"Error al añadir ubicación para bloque {location.block_id}: {str(e)}")
                    continue
        
        # Devolver la información actualizada del bloque
        return manager.get_block_info(block_info.block_id)
    except Exception as e:
        import traceback
        error_detail = f"Error creating block: {str(e)}\n{traceback.format_exc()}"
        print(error_detail)
        raise HTTPException(status_code=500, detail=error_detail)

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
    if status:
        status = status.lower()
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
            continue
        
        # Actualizar la ubicación del bloque
        manager.add_block_location(block_id, node_id, False)
    
    return None

# Directories Endpoints
@directories_router.post("/", response_model=FileMetadata, status_code=201)
async def create_directory(directory: FileMetadata, manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Create a new directory.
    """
    if directory.type != FileType.DIRECTORY:
        raise HTTPException(status_code=400, detail="Type must be 'directory'")
    
    # Normalizar la ruta
    normalized_path = os.path.normpath(directory.path).replace("\\", "/")
    if not normalized_path.startswith("/"):
        normalized_path = "/" + normalized_path
    
    # Verificar si ya existe un directorio con la misma ruta
    existing_dir = manager.get_file_by_path(normalized_path)
    if existing_dir:
        raise HTTPException(status_code=409, detail=f"Directory already exists at path: {normalized_path}")
    
    # Caso especial para el directorio raíz
    if normalized_path == "/":
        created_dir = manager.create_file(
            name="",
            path="/",
            file_type=FileType.DIRECTORY,
            owner=directory.owner or "system"
        )
        if not created_dir:
            raise HTTPException(status_code=500, detail="Failed to create root directory")
        return created_dir
    
    # Para otros directorios, verificar que el directorio padre existe
    parent_path = os.path.dirname(normalized_path)
    parent_dir = manager.get_file_by_path(parent_path)
    
    if not parent_dir:
        raise HTTPException(status_code=404, detail=f"Parent directory does not exist: {parent_path}")
    
    if parent_dir.type != FileType.DIRECTORY:
        raise HTTPException(status_code=400, detail=f"Parent path is not a directory: {parent_path}")
    
    # Crear el directorio en el sistema
    created_dir = manager.create_file(
        name=os.path.basename(normalized_path),
        path=normalized_path,
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
    # Normalizar la ruta
    if not path or path == "/":
        normalized_path = "/"
    else:
        normalized_path = "/" + path.strip("/")
    
    # Verificar si el directorio existe
    dir_info = manager.get_file_by_path(normalized_path)
    
    # Caso especial para el directorio raíz
    if normalized_path == "/" and not dir_info:
        # Intentar inicializar el directorio raíz
        from src.namenode.init_root import init_root_directory
        if not init_root_directory(manager):
            raise HTTPException(status_code=500, detail="Failed to initialize root directory")
        dir_info = manager.get_file_by_path("/")
    
    if not dir_info:
        raise HTTPException(status_code=404, detail=f"Directory not found at path: {normalized_path}")
    
    if dir_info.type != FileType.DIRECTORY:
        raise HTTPException(status_code=400, detail=f"Path is not a directory: {normalized_path}")
    
    return manager.list_directory(normalized_path)

@directories_router.delete("/{path:path}")
async def delete_directory(
    path: str = Path(..., description="The path of the directory to delete"),
    recursive: bool = Query(False, description="Whether to delete recursively"),
    manager: MetadataManager = Depends(get_metadata_manager)
):
    """
    Delete a directory.
    """
    # Normalizar la ruta
    path = "/" + path.strip("/")
    
    # Verificar que el directorio existe
    dir_info = manager.get_file_by_path(path)
    if not dir_info:
        raise HTTPException(status_code=404, detail=f"Directory {path} not found")
    
    # Verificar que es un directorio
    if dir_info.type != FileType.DIRECTORY:
        raise HTTPException(status_code=400, detail=f"{path} is not a directory")
    
    # Verificar que el directorio está vacío o se solicitó borrado recursivo
    contents = manager.list_directory(path)
    if contents.contents and not recursive:
        raise HTTPException(
            status_code=400, 
            detail=f"Directory {path} is not empty. Delete contents first or use recursive deletion"
        )
    
    # Eliminar el directorio
    try:
        success = manager.delete_directory(path, recursive=recursive)
        if not success:
            raise HTTPException(status_code=500, detail=f"Failed to delete directory {path}")
        return {"message": f"Directory {path} deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@system_router.get("/stats")
async def get_system_stats(manager: MetadataManager = Depends(get_metadata_manager)):
    """
    Obtiene estadísticas generales del sistema.
    """
    try:
        # Limpiar DataNodes inactivos primero
        manager.cleanup_inactive_datanodes()
        
        # Obtener DataNodes activos
        datanodes = manager.list_datanodes()
        active_datanodes = [dn for dn in datanodes if dn.status == DataNodeStatus.ACTIVE]
        
        # Obtener estadísticas de archivos y bloques
        files_stats = manager.get_files_stats()
        blocks_stats = manager.get_blocks_stats()
        
        # Calcular estadísticas de almacenamiento
        total_capacity = sum(dn.storage_capacity for dn in datanodes)
        available_space = sum(dn.available_space for dn in datanodes)
        
        return {
            "namenode_active": True,
            "total_files": files_stats.get("total_files", 0),
            "total_blocks": blocks_stats.get("total_blocks", 0),
            "total_block_instances": blocks_stats.get("total_block_instances", 0),
            "active_datanodes": len(active_datanodes),
            "total_datanodes": len(datanodes),
            "storage_capacity": total_capacity,
            "available_space": available_space,
            "replication_factor": blocks_stats.get("replication_factor", 2)
        }
    except Exception as e:
        import traceback
        error_detail = f"Error al obtener estadísticas del sistema: {str(e)}\n{traceback.format_exc()}"
        print(error_detail)
        raise HTTPException(status_code=500, detail=error_detail)
