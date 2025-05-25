import logging
import os
import glob

class StorageManager:
    def __init__(self, storage_dir: str):
        """
        Inicializa el gestor de almacenamiento.
        
        Args:
            storage_dir: Directorio base para almacenar los bloques
        """
        self.storage_dir = storage_dir
        self.blocks = {}  # Mapeo de block_id a metadata
        self.logger = logging.getLogger("StorageManager")
        
        # Crear el directorio de almacenamiento si no existe
        os.makedirs(storage_dir, exist_ok=True)
        
        # Cargar bloques existentes
        self._load_existing_blocks()
    
    def _load_existing_blocks(self):
        """
        Carga los bloques existentes en el directorio de almacenamiento.
        """
        try:
            # Listar todos los archivos .block en el directorio
            block_files = glob.glob(os.path.join(self.storage_dir, "*.block"))
            
            for block_file in block_files:
                try:
                    # Obtener el block_id del nombre del archivo
                    block_id = os.path.splitext(os.path.basename(block_file))[0]
                    
                    # Obtener el tamaño del bloque
                    block_size = os.path.getsize(block_file)
                    
                    # Registrar el bloque en memoria
                    self.blocks[block_id] = {
                        'size': block_size,
                        'path': block_file
                    }
                    
                    self.logger.info(f"Bloque cargado: {block_id} ({block_size} bytes)")
                except Exception as e:
                    self.logger.error(f"Error al cargar bloque {block_file}: {e}")
            
            self.logger.info(f"Total de bloques cargados: {len(self.blocks)}")
        except Exception as e:
            self.logger.error(f"Error al cargar bloques existentes: {e}") 