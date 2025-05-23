from src.namenode.metadata.manager import MetadataManager
from src.namenode.api.models import FileType
import logging

logger = logging.getLogger(__name__)

def init_root_directory(manager: MetadataManager) -> bool:
    """
    Inicializa el directorio raíz si no existe.
    
    Args:
        manager: Instancia del gestor de metadatos
        
    Returns:
        bool: True si el directorio raíz existe o fue creado correctamente, False en caso contrario
    
    Raises:
        Exception: Si hay un error crítico al inicializar el directorio raíz
    """
    try:
        # Normalizar la ruta raíz
        root_path = "/"
        
        # Verificar si el directorio raíz ya existe
        root = manager.get_file_by_path(root_path)
        
        if not root:
            # Crear el directorio raíz
            root = manager.create_file(
                name="",
                path=root_path,
                file_type=FileType.DIRECTORY,
                owner="system"
            )
            
            if not root:
                logger.error("Falló la creación del directorio raíz")
                return False
                
            logger.info("Directorio raíz creado correctamente")
        else:
            # Verificar que el directorio raíz es del tipo correcto
            if root.type != FileType.DIRECTORY:
                logger.error("La ruta raíz existe pero no es un directorio")
                return False
            logger.info("El directorio raíz ya existe")
        
        return True
        
    except Exception as e:
        logger.error(f"Error crítico al inicializar el directorio raíz: {str(e)}")
        raise 