import os
import uuid
import hashlib
from typing import List, Dict, Tuple, BinaryIO


class FileSplitter:
    """
    Clase encargada de particionar archivos en bloques de tamaño fijo.
    """
    def __init__(self, block_size: int = 4 * 1024 * 1024):  # 4MB por defecto
        """
        Inicializa el particionador de archivos.
        
        Args:
            block_size: Tamaño de cada bloque en bytes (4MB por defecto)
        """
        self.block_size = block_size
    
    def split_file(self, file_path: str) -> List[Dict]:
        """
        Divide un archivo en bloques de tamaño fijo.
        
        Args:
            file_path: Ruta al archivo a dividir
            
        Returns:
            Lista de diccionarios con información de cada bloque:
            - block_id: Identificador único del bloque
            - data: Contenido binario del bloque
            - size: Tamaño del bloque en bytes
            - checksum: Hash SHA-256 del contenido del bloque
            - index: Índice del bloque en el archivo (0-based)
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no existe")
        
        file_size = os.path.getsize(file_path)
        total_blocks = (file_size + self.block_size - 1) // self.block_size  # Redondeo hacia arriba
        
        blocks = []
        
        with open(file_path, 'rb') as file:
            for i in range(total_blocks):
                # Generar un ID único para el bloque
                block_id = self._generate_block_id()
                
                # Leer el bloque
                data = file.read(self.block_size)
                
                # Calcular el checksum
                checksum = self._calculate_checksum(data)
                
                blocks.append({
                    'block_id': block_id,
                    'data': data,
                    'size': len(data),
                    'checksum': checksum,
                    'index': i
                })
        
        return blocks
    
    def split_file_stream(self, file: BinaryIO, file_size: int) -> List[Dict]:
        """
        Divide un archivo en bloques de tamaño fijo a partir de un stream.
        
        Args:
            file: Stream de archivo abierto en modo binario
            file_size: Tamaño del archivo en bytes
            
        Returns:
            Lista de diccionarios con información de cada bloque
        """
        total_blocks = (file_size + self.block_size - 1) // self.block_size
        blocks = []
        
        for i in range(total_blocks):
            # Generar un ID único para el bloque
            block_id = self._generate_block_id()
            
            # Leer el bloque
            data = file.read(self.block_size)
            
            # Calcular el checksum
            checksum = self._calculate_checksum(data)
            
            blocks.append({
                'block_id': block_id,
                'data': data,
                'size': len(data),
                'checksum': checksum,
                'index': i
            })
        
        return blocks
    
    def join_blocks(self, blocks: List[Dict], output_path: str) -> bool:
        """
        Une los bloques para reconstruir el archivo original.
        
        Args:
            blocks: Lista de diccionarios con información de cada bloque
            output_path: Ruta donde se guardará el archivo reconstruido
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            # Ordenar los bloques por índice
            sorted_blocks = sorted(blocks, key=lambda x: x['index'])
            
            with open(output_path, 'wb') as output_file:
                for block in sorted_blocks:
                    output_file.write(block['data'])
            
            return True
        except Exception as e:
            print(f"Error al unir los bloques: {e}")
            return False
    
    def _generate_block_id(self) -> str:
        """
        Genera un identificador único para un bloque.
        
        Returns:
            Identificador único como string
        """
        return str(uuid.uuid4())
    
    def _calculate_checksum(self, data: bytes) -> str:
        """
        Calcula el checksum SHA-256 de los datos del bloque.
        
        Args:
            data: Datos binarios del bloque
            
        Returns:
            Checksum SHA-256 como string hexadecimal
        """
        sha256 = hashlib.sha256()
        sha256.update(data)
        return sha256.hexdigest()
