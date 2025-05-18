import os
import hashlib
import logging
import shutil
from typing import Dict, Optional, List, Tuple

class BlockStorage:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        self.logger = logging.getLogger("BlockStorage")
        os.makedirs(self.storage_dir, exist_ok=True)
        
    def _get_block_path(self, block_id: str) -> str:
        return os.path.join(self.storage_dir, block_id)
    
    def _calculate_checksum(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()
    
    def store_block(self, block_id: str, data: bytes) -> Tuple[bool, str]:
        """
        Almacena un bloque y devuelve una tupla (success, checksum).
        """
        try:
            block_path = self._get_block_path(block_id)
            
            # Calcular checksum antes de almacenar
            checksum = self._calculate_checksum(data)
            
            # Almacenar el bloque
            with open(block_path, 'wb') as f:
                f.write(data)
            
            # Verificar integridad después de almacenar
            with open(block_path, 'rb') as f:
                stored_data = f.read()
                stored_checksum = self._calculate_checksum(stored_data)
                
                if stored_checksum != checksum:
                    self.logger.error(f"Block integrity check failed for {block_id}")
                    os.remove(block_path)
                    return False, ""
            
            return True, checksum
        except Exception as e:
            self.logger.error(f"Error storing block {block_id}: {str(e)}")
            return False, ""
    
    def retrieve_block(self, block_id: str) -> Optional[bytes]:
        """
        Recupera un bloque y verifica su integridad.
        """
        try:
            block_path = self._get_block_path(block_id)
            if not os.path.exists(block_path):
                return None
            
            with open(block_path, 'rb') as f:
                data = f.read()
            
            return data
        except Exception as e:
            self.logger.error(f"Error retrieving block {block_id}: {str(e)}")
            return None
    
    def block_exists(self, block_id: str) -> bool:
        return os.path.exists(self._get_block_path(block_id))
    
    def get_block_info(self, block_id: str) -> Dict:
        """
        Obtiene información del bloque incluyendo su checksum.
        """
        try:
            block_path = self._get_block_path(block_id)
            if not os.path.exists(block_path):
                return {
                    "exists": False,
                    "size": 0,
                    "checksum": ""
                }
            
            with open(block_path, 'rb') as f:
                data = f.read()
            
            return {
                "exists": True,
                "size": len(data),
                "checksum": self._calculate_checksum(data)
            }
        except Exception as e:
            self.logger.error(f"Error getting block info for {block_id}: {str(e)}")
            return {
                "exists": False,
                "size": 0,
                "checksum": ""
            }
    
    def delete_block(self, block_id: str) -> bool:
        try:
            block_path = self._get_block_path(block_id)
            if os.path.exists(block_path):
                os.remove(block_path)
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error deleting block {block_id}: {str(e)}")
            return False
    
    def get_storage_stats(self) -> Dict:
        """
        Obtiene estadísticas de almacenamiento incluyendo información de bloques.
        """
        try:
            total_size = 0
            blocks = []
            block_sizes = {}
            block_checksums = {}
            
            for filename in os.listdir(self.storage_dir):
                block_path = os.path.join(self.storage_dir, filename)
                if os.path.isfile(block_path):
                    with open(block_path, 'rb') as f:
                        data = f.read()
                        size = len(data)
                        checksum = self._calculate_checksum(data)
                        
                        total_size += size
                        blocks.append(filename)
                        block_sizes[filename] = size
                        block_checksums[filename] = checksum
            
            return {
                "total_size": total_size,
                "blocks": blocks,
                "block_sizes": block_sizes,
                "block_checksums": block_checksums
            }
        except Exception as e:
            self.logger.error(f"Error getting storage stats: {str(e)}")
            return {
                "total_size": 0,
                "blocks": [],
                "block_sizes": {},
                "block_checksums": {}
            }
    
    def get_block_size(self, block_id: str) -> Optional[int]:
        block_path = self._get_block_path(block_id)
        if not os.path.exists(block_path):
            return None
        
        return os.path.getsize(block_path)
    
    def calculate_checksum(self, block_id: str) -> Optional[str]:
        block_path = self._get_block_path(block_id)
        if not os.path.exists(block_path):
            return None
        
        sha256 = hashlib.sha256()
        try:
            with open(block_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    sha256.update(chunk)
            return sha256.hexdigest()
        except Exception:
            return None
    
    def get_all_blocks(self) -> List[str]:
        if not os.path.exists(self.storage_dir):
            return []
        
        return [f for f in os.listdir(self.storage_dir) 
                if os.path.isfile(os.path.join(self.storage_dir, f))]
    
    def get_available_space(self) -> int:
        if not os.path.exists(self.storage_dir):
            return 0
        
        stats = shutil.disk_usage(self.storage_dir)
        return stats.free
    
    def stream_block(self, block_id: str, chunk_size: int = 4096) -> Optional[List[Tuple[bytes, int, int]]]:
        block_path = self._get_block_path(block_id)
        if not os.path.exists(block_path):
            return None
        
        total_size = os.path.getsize(block_path)
        chunks = []
        
        try:
            with open(block_path, 'rb') as f:
                offset = 0
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    chunks.append((chunk, offset, total_size))
                    offset += len(chunk)
            return chunks
        except Exception:
            return None
