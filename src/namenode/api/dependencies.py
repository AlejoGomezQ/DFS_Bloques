from typing import Optional
from fastapi import Depends, HTTPException
from src.namenode.metadata.manager import MetadataManager

# Variable global para el gestor de metadatos
_metadata_manager: Optional[MetadataManager] = None

def set_metadata_manager(manager: MetadataManager) -> None:
    """
    Configura el gestor de metadatos global.
    
    Args:
        manager: Instancia del gestor de metadatos
    """
    global _metadata_manager
    _metadata_manager = manager

def get_metadata_manager() -> MetadataManager:
    """
    Obtiene el gestor de metadatos global.
    
    Returns:
        MetadataManager: Instancia del gestor de metadatos
        
    Raises:
        HTTPException: Si el gestor de metadatos no está inicializado
    """
    if _metadata_manager is None:
        raise HTTPException(
            status_code=500,
            detail="MetadataManager not initialized"
        )
    return _metadata_manager

# Dependencia para FastAPI
async def get_metadata_manager_dependency() -> MetadataManager:
    """
    Dependencia de FastAPI para obtener el gestor de metadatos.
    
    Returns:
        MetadataManager: Instancia del gestor de metadatos
    """
    return get_metadata_manager() 