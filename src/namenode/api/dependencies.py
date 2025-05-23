from typing import Optional
from src.namenode.metadata.manager import MetadataManager

# Variable global para el gestor de metadatos
_metadata_manager: Optional[MetadataManager] = None

def get_metadata_manager() -> MetadataManager:
    """
    Obtiene la instancia global del gestor de metadatos.
    """
    global _metadata_manager
    return _metadata_manager

def set_metadata_manager(manager: MetadataManager) -> None:
    """
    Establece la instancia global del gestor de metadatos.
    """
    global _metadata_manager
    _metadata_manager = manager 