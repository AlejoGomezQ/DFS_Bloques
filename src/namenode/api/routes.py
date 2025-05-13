from fastapi import APIRouter, HTTPException, Path, Query, Body
from typing import List, Optional

# Importación absoluta en lugar de relativa
from src.namenode.api.models import (
    DataNodeRegistration, 
    DataNodeInfo, 
    BlockInfo, 
    FileMetadata, 
    DirectoryListing,
    ErrorResponse
)

# Routers
files_router = APIRouter(prefix="/files", tags=["Files"])
blocks_router = APIRouter(prefix="/blocks", tags=["Blocks"])
datanodes_router = APIRouter(prefix="/datanodes", tags=["DataNodes"])
directories_router = APIRouter(prefix="/directories", tags=["Directories"])

# Files Endpoints
@files_router.post("/", response_model=FileMetadata, status_code=201)
async def create_file(file_metadata: FileMetadata):
    """
    Register a new file in the system.
    """
    return file_metadata

@files_router.get("/{file_id}", response_model=FileMetadata)
async def get_file(file_id: str = Path(..., description="The ID of the file to retrieve")):
    """
    Get file metadata by ID.
    """
    return FileMetadata(file_id=file_id, name="example", path="/example", type="file", size=1024)

@files_router.delete("/{file_id}", status_code=204)
async def delete_file(file_id: str = Path(..., description="The ID of the file to delete")):
    """
    Delete a file and its associated blocks.
    """
    return None

@files_router.get("/path/{path:path}", response_model=FileMetadata)
async def get_file_by_path(path: str = Path(..., description="The path of the file to retrieve")):
    """
    Get file metadata by path.
    """
    return FileMetadata(file_id="123", name="example", path=path, type="file", size=1024)

# Blocks Endpoints
@blocks_router.get("/{block_id}", response_model=BlockInfo)
async def get_block_info(block_id: str = Path(..., description="The ID of the block to retrieve")):
    """
    Get information about a specific block.
    """
    return BlockInfo(block_id=block_id, file_id="123", size=1024, locations=[])

@blocks_router.get("/file/{file_id}", response_model=List[BlockInfo])
async def get_file_blocks(file_id: str = Path(..., description="The ID of the file to retrieve blocks for")):
    """
    Get all blocks associated with a file.
    """
    return [BlockInfo(block_id="block1", file_id=file_id, size=1024, locations=[])]

@blocks_router.post("/report", status_code=204)
async def report_block_status(block_reports: List[BlockInfo]):
    """
    Report the status of blocks from a DataNode.
    """
    return None

# DataNodes Endpoints
@datanodes_router.post("/register", response_model=DataNodeInfo, status_code=201)
async def register_datanode(registration: DataNodeRegistration):
    """
    Register a new DataNode in the system.
    """
    return DataNodeInfo(
        node_id="generated_id",
        hostname=registration.hostname,
        port=registration.port,
        status="active",
        storage_capacity=registration.storage_capacity,
        available_space=registration.available_space
    )

@datanodes_router.get("/", response_model=List[DataNodeInfo])
async def list_datanodes(status: Optional[str] = Query(None, description="Filter by DataNode status")):
    """
    List all registered DataNodes.
    """
    return [
        DataNodeInfo(
            node_id="node1",
            hostname="localhost",
            port=50051,
            status="active",
            storage_capacity=1024*1024*1024,
            available_space=512*1024*1024,
            blocks_stored=10
        )
    ]

@datanodes_router.get("/{node_id}", response_model=DataNodeInfo)
async def get_datanode(node_id: str = Path(..., description="The ID of the DataNode to retrieve")):
    """
    Get information about a specific DataNode.
    """
    return DataNodeInfo(
        node_id=node_id,
        hostname="localhost",
        port=50051,
        status="active",
        storage_capacity=1024*1024*1024,
        available_space=512*1024*1024,
        blocks_stored=10
    )

@datanodes_router.post("/{node_id}/heartbeat", status_code=204)
async def datanode_heartbeat(
    node_id: str = Path(..., description="The ID of the DataNode sending the heartbeat"),
    available_space: int = Body(..., embed=True, description="Available space in bytes")
):
    """
    Process a heartbeat from a DataNode.
    """
    return None

# Directories Endpoints
@directories_router.post("/", response_model=FileMetadata, status_code=201)
async def create_directory(directory: FileMetadata):
    """
    Create a new directory.
    """
    return directory

@directories_router.get("/{path:path}", response_model=DirectoryListing)
async def list_directory(path: str = Path(..., description="The path of the directory to list")):
    """
    List the contents of a directory.
    """
    return DirectoryListing(
        path=path,
        contents=[
            FileMetadata(file_id="123", name="example.txt", path=f"{path}/example.txt", type="file", size=1024),
            FileMetadata(file_id="456", name="subdir", path=f"{path}/subdir", type="directory", size=0)
        ]
    )

@directories_router.delete("/{path:path}", status_code=204)
async def delete_directory(path: str = Path(..., description="The path of the directory to delete")):
    """
    Delete a directory and all its contents.
    """
    return None
