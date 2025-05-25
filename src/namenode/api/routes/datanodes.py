from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional

from src.namenode.api.models import DataNodeInfo, DataNodeRegistration, HeartbeatRequest
from src.namenode.metadata.manager import MetadataManager
from src.namenode.api.dependencies import get_metadata_manager_dependency

router = APIRouter(prefix="/datanodes", tags=["datanodes"])

@router.post("/register", response_model=DataNodeInfo)
async def register_datanode(
    registration: DataNodeRegistration,
    metadata_manager: MetadataManager = Depends(get_metadata_manager_dependency)
):
    """
    Registra un nuevo DataNode en el sistema.
    """
    try:
        datanode = metadata_manager.register_datanode(
            hostname=registration.hostname,
            port=registration.port,
            storage_capacity=registration.storage_capacity,
            available_space=registration.available_space
        )
        return datanode
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/heartbeat/{node_id}")
async def update_heartbeat(
    node_id: str,
    heartbeat: HeartbeatRequest,
    metadata_manager: MetadataManager = Depends(get_metadata_manager_dependency)
):
    """
    Actualiza el heartbeat de un DataNode.
    """
    success = metadata_manager.update_datanode_heartbeat(
        node_id=node_id,
        available_space=heartbeat.available_space
    )
    if not success:
        raise HTTPException(status_code=404, detail=f"DataNode {node_id} not found")
    return {"status": "ok"}

@router.get("/list", response_model=List[DataNodeInfo])
async def list_datanodes(
    status: Optional[str] = None,
    metadata_manager: MetadataManager = Depends(get_metadata_manager_dependency)
):
    """
    Lista todos los DataNodes registrados.
    """
    return metadata_manager.list_datanodes(status)

@router.get("/{node_id}", response_model=DataNodeInfo)
async def get_datanode(
    node_id: str,
    metadata_manager: MetadataManager = Depends(get_metadata_manager_dependency)
):
    """
    Obtiene información de un DataNode específico.
    """
    datanode = metadata_manager.get_datanode(node_id)
    if not datanode:
        raise HTTPException(status_code=404, detail=f"DataNode {node_id} not found")
    return datanode

@router.delete("/{node_id}")
async def delete_datanode(
    node_id: str,
    metadata_manager: MetadataManager = Depends(get_metadata_manager_dependency)
):
    """
    Elimina un DataNode del sistema.
    """
    success = metadata_manager.delete_datanode(node_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"DataNode {node_id} not found")
    return {"status": "ok"} 