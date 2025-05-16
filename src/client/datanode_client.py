import grpc
from typing import List, Optional, Iterator, Tuple

# Importamos los módulos generados por gRPC
from src.common.proto import datanode_pb2, datanode_pb2_grpc

class DataNodeClient:
    def __init__(self, hostname: str, port: int):
        self.hostname = hostname
        self.port = port
        self.channel = None
        self.stub = None
    
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
        def block_data_iterator():
            chunk_size = 4096
            total_size = len(data)
            
            for i in range(0, total_size, chunk_size):
                chunk = data[i:i+chunk_size]
                yield datanode_pb2.BlockData(
                    block_id=block_id,
                    data=chunk,
                    offset=i,
                    total_size=total_size
                )
        
        try:
            response = self.stub.StoreBlock(block_data_iterator())
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
        try:
            request = datanode_pb2.BlockRequest(block_id=block_id)
            response_iterator = self.stub.RetrieveBlock(request)
            
            data_chunks = []
            for chunk in response_iterator:
                data_chunks.append(chunk.data)
            
            if not data_chunks:
                return None
            
            return b''.join(data_chunks)
        except grpc.RpcError as e:
            print(f"Error al recuperar el bloque: {e}")
            return None
    
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
