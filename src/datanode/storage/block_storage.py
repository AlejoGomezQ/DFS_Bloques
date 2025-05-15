import os
import hashlib
import shutil
from typing import Dict, Optional, List, Tuple

class BlockStorage:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        
    def get_block_path(self, block_id: str) -> str:
        return os.path.join(self.storage_dir, block_id)
    
    def store_block(self, block_id: str, data: bytes) -> bool:
        try:
            with open(self.get_block_path(block_id), 'wb') as f:
                f.write(data)
            return True
        except Exception:
            return False
    
    def retrieve_block(self, block_id: str) -> Optional[bytes]:
        block_path = self.get_block_path(block_id)
        if not os.path.exists(block_path):
            return None
        
        try:
            with open(block_path, 'rb') as f:
                return f.read()
        except Exception:
            return None
    
    def delete_block(self, block_id: str) -> bool:
        block_path = self.get_block_path(block_id)
        if not os.path.exists(block_path):
            return False
        
        try:
            os.remove(block_path)
            return True
        except Exception:
            return False
    
    def block_exists(self, block_id: str) -> bool:
        return os.path.exists(self.get_block_path(block_id))
    
    def get_block_size(self, block_id: str) -> Optional[int]:
        block_path = self.get_block_path(block_id)
        if not os.path.exists(block_path):
            return None
        
        return os.path.getsize(block_path)
    
    def calculate_checksum(self, block_id: str) -> Optional[str]:
        block_path = self.get_block_path(block_id)
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
    
    def get_storage_stats(self) -> Dict:
        total_blocks = 0
        total_size = 0
        blocks = self.get_all_blocks()
        block_sizes = {}
        block_checksums = {}
        
        for block_id in blocks:
            size = self.get_block_size(block_id)
            checksum = self.calculate_checksum(block_id)
            
            if size is not None:
                total_blocks += 1
                total_size += size
                block_sizes[block_id] = size
            
            if checksum is not None:
                block_checksums[block_id] = checksum
        
        return {
            "total_blocks": total_blocks,
            "total_size": total_size,
            "blocks": blocks,
            "block_sizes": block_sizes,
            "block_checksums": block_checksums,
            "available_space": self.get_available_space()
        }
    
    def get_available_space(self) -> int:
        if not os.path.exists(self.storage_dir):
            return 0
        
        stats = shutil.disk_usage(self.storage_dir)
        return stats.free
    
    def stream_block(self, block_id: str, chunk_size: int = 4096) -> Optional[List[Tuple[bytes, int, int]]]:
        block_path = self.get_block_path(block_id)
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
