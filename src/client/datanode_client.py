import grpc
import io
import time
from typing import List, Optional, Iterator, Tuple, Dict, Any

# Importamos los módulos generados por gRPC
from src.common.proto import datanode_pb2, datanode_pb2_grpc
from src.common.compression import DataCompressor, COMPRESSION_BALANCED, COMPRESSION_FAST, COMPRESSION_MAX

class DataNodeClient:
    def __init__(self, hostname: str, port: int, use_compression: bool = True, 
                 compression_level: int = COMPRESSION_BALANCED):
        self.hostname = hostname
        self.port = port
        self.channel = None
        self.stub = None
        self.use_compression = use_compression
        self.compression_level = compression_level
        self.compressor = DataCompressor(auto_select=True)
        self.transfer_stats = {
            "bytes_sent": 0,
            "bytes_received": 0,
            "compressed_bytes_sent": 0,
            "compressed_bytes_received": 0,
            "transfer_time": 0,
            "compression_time": 0,
            "decompression_time": 0
        }
    
    def connect(self):
        """Establece la conexión con el DataNode."""
        self.channel = grpc.insecure_channel(f"{self.hostname}:{self.port}")
        self.stub = datanode_pb2_grpc.DataNodeServiceStub(self.channel)
        return self
    
    def close(self):
        """Cierra la conexión con el DataNode."""
        if self.channel:
            self.channel.close()
    
    def __enter__(self):
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def store_block(self, block_id: str, data: bytes) -> bool:
        """
        Almacena un bloque en el DataNode.
        
        Args:
            block_id: Identificador único del bloque.
            data: Datos del bloque.
            
        Returns:
            bool: True si el bloque se almacenó correctamente, False en caso contrario.
        """
        start_time = time.time()
        original_size = len(data)
        compression_metadata = {}
        
        # Comprimir los datos si está habilitada la compresión
        if self.use_compression and original_size > 512:  # Solo comprimir bloques de más de 512 bytes
            compressed_data, compression_metadata = self.compressor.compress(
                data, level=self.compression_level
            )
            
            # Usar datos comprimidos si la compresión fue efectiva
            if compression_metadata.get("compressed", False):
                data = compressed_data
                self.transfer_stats["compression_time"] += compression_metadata.get("time", 0)
        
        def block_data_iterator():
            # Tamaño de chunk optimizado para transferencia
            chunk_size = 64 * 1024  # 64KB por chunk
            total_size = len(data)
            
            # Primer chunk con metadatos de compresión
            metadata_bytes = str(compression_metadata).encode('utf-8')
            first_chunk = data[:chunk_size-len(metadata_bytes)]
            
            yield datanode_pb2.BlockData(
                block_id=block_id,
                data=first_chunk,
                offset=0,
                total_size=total_size,
                original_size=original_size,
                compressed=compression_metadata.get("compressed", False),
                compression_metadata=metadata_bytes
            )
            
            # Resto de chunks
            for i in range(len(first_chunk), total_size, chunk_size):
                chunk = data[i:i+chunk_size]
                yield datanode_pb2.BlockData(
                    block_id=block_id,
                    data=chunk,
                    offset=i,
                    total_size=total_size
                )
        
        try:
            # Registrar estadísticas de transferencia
            self.transfer_stats["bytes_sent"] += original_size
            self.transfer_stats["compressed_bytes_sent"] += len(data)
            
            # Enviar el bloque
            response = self.stub.StoreBlock(block_data_iterator())
            
            # Actualizar tiempo de transferencia
            self.transfer_stats["transfer_time"] += time.time() - start_time
            
            # Mostrar estadísticas si la compresión fue efectiva
            if compression_metadata.get("compressed", False):
                ratio = compression_metadata.get("ratio", 1.0)
                algorithm = compression_metadata.get("algorithm", "unknown")
                print(f"Bloque {block_id} comprimido: {original_size/1024:.1f}KB → "
                      f"{len(data)/1024:.1f}KB (ratio: {ratio:.1f}x, algoritmo: {algorithm})")
            
            return response.status == datanode_pb2.BlockResponse.SUCCESS
        except grpc.RpcError as e:
            print(f"Error al almacenar el bloque: {e}")
            return False
    
    def retrieve_block(self, block_id: str) -> Optional[bytes]:
        """
        Recupera un bloque del DataNode.
        
        Args:
            block_id: Identificador único del bloque.
            
        Returns:
            Optional[bytes]: Datos del bloque o None si no se encontró.
        """
        start_time = time.time()
        
        try:
            request = datanode_pb2.BlockRequest(block_id=block_id)
            response_iterator = self.stub.RetrieveBlock(request)
            
            data_chunks = []
            compression_metadata = {}
            is_compressed = False
            original_size = 0
            
            # Procesar los chunks recibidos
            for i, chunk in enumerate(response_iterator):
                # El primer chunk contiene metadatos de compresión
                if i == 0 and hasattr(chunk, 'compression_metadata') and chunk.compression_metadata:
                    try:
                        metadata_str = chunk.compression_metadata.decode('utf-8')
                        compression_metadata = eval(metadata_str)
                        is_compressed = chunk.compressed
                        original_size = chunk.original_size
                    except Exception as e:
                        print(f"Error al procesar metadatos de compresión: {e}")
                
                data_chunks.append(chunk.data)
            
            if not data_chunks:
                return None
            
            # Unir los chunks
            compressed_data = b''.join(data_chunks)
            
            # Registrar estadísticas de transferencia
            self.transfer_stats["compressed_bytes_received"] += len(compressed_data)
            
            # Descomprimir si es necesario
            if is_compressed:
                decompression_start = time.time()
                decompressed_data = self.compressor.decompress(compressed_data, compression_metadata)
                decompression_time = time.time() - decompression_start
                
                self.transfer_stats["decompression_time"] += decompression_time
                self.transfer_stats["bytes_received"] += len(decompressed_data)
                
                # Mostrar estadísticas de descompresión
                ratio = len(decompressed_data) / len(compressed_data) if len(compressed_data) > 0 else 1.0
                algorithm = compression_metadata.get("algorithm", "unknown")
                print(f"Bloque {block_id} descomprimido: {len(compressed_data)/1024:.1f}KB → "
                      f"{len(decompressed_data)/1024:.1f}KB (ratio: {ratio:.1f}x, algoritmo: {algorithm})")
                
                result = decompressed_data
            else:
                self.transfer_stats["bytes_received"] += len(compressed_data)
                result = compressed_data
            
            # Actualizar tiempo de transferencia
            self.transfer_stats["transfer_time"] += time.time() - start_time
            
            return result
        except grpc.RpcError as e:
            print(f"Error al recuperar el bloque: {e}")
            return None
    
    def get_transfer_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas de transferencia.
        
        Returns:
            Dict: Estadísticas de transferencia
        """
        stats = self.transfer_stats.copy()
        
        # Calcular estadísticas adicionales
        if stats["bytes_sent"] > 0:
            stats["compression_ratio_sent"] = stats["bytes_sent"] / stats["compressed_bytes_sent"] if stats["compressed_bytes_sent"] > 0 else 1.0
        else:
            stats["compression_ratio_sent"] = 1.0
            
        if stats["bytes_received"] > 0:
            stats["compression_ratio_received"] = stats["bytes_received"] / stats["compressed_bytes_received"] if stats["compressed_bytes_received"] > 0 else 1.0
        else:
            stats["compression_ratio_received"] = 1.0
        
        return stats
    
    def reset_transfer_stats(self):
        """Reinicia las estadísticas de transferencia."""
        self.transfer_stats = {
            "bytes_sent": 0,
            "bytes_received": 0,
            "compressed_bytes_sent": 0,
            "compressed_bytes_received": 0,
            "transfer_time": 0,
            "compression_time": 0,
            "decompression_time": 0
        }
    
    def check_block(self, block_id: str) -> Tuple[bool, Optional[int], Optional[str]]:
        """
        Verifica si un bloque existe en el DataNode.
        
        Args:
            block_id: Identificador único del bloque.
            
        Returns:
            Tuple[bool, Optional[int], Optional[str]]: 
                - Existe el bloque
                - Tamaño del bloque (si existe)
                - Checksum del bloque (si existe)
        """
        try:
            request = datanode_pb2.BlockRequest(block_id=block_id)
            response = self.stub.CheckBlock(request)
            
            return response.exists, response.size if response.exists else None, response.checksum if response.exists else None
        except grpc.RpcError as e:
            print(f"Error al verificar el bloque: {e}")
            return False, None, None
    
    def delete_block(self, block_id: str) -> bool:
        """
        Elimina un bloque del DataNode.
        
        Args:
            block_id: Identificador único del bloque.
            
        Returns:
            bool: True si el bloque se eliminó correctamente, False en caso contrario.
        """
        try:
            request = datanode_pb2.BlockRequest(block_id=block_id)
            response = self.stub.DeleteBlock(request)
            
            return response.status == datanode_pb2.BlockResponse.SUCCESS
        except grpc.RpcError as e:
            print(f"Error al eliminar el bloque: {e}")
            return False
