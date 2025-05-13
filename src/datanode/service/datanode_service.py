import grpc
from concurrent import futures
import os
import hashlib
from typing import Dict, Optional

# Estos imports se completarán cuando se genere el código gRPC
# from common.proto import datanode_pb2, datanode_pb2_grpc

class DataNodeServicer:
    def __init__(self, storage_dir: str, node_id: str, hostname: str, port: int):
        self.storage_dir = storage_dir
        self.node_id = node_id
        self.hostname = hostname
        self.port = port
        self.blocks: Dict[str, Dict] = {}
        
        # Crear directorio de almacenamiento si no existe
        os.makedirs(self.storage_dir, exist_ok=True)
    
    def get_block_path(self, block_id: str) -> str:
        return os.path.join(self.storage_dir, block_id)
    
    def block_exists(self, block_id: str) -> bool:
        return os.path.exists(self.get_block_path(block_id))
    
    def get_block_size(self, block_id: str) -> Optional[int]:
        if self.block_exists(block_id):
            return os.path.getsize(self.get_block_path(block_id))
        return None
    
    def calculate_checksum(self, block_id: str) -> Optional[str]:
        if not self.block_exists(block_id):
            return None
        
        sha256 = hashlib.sha256()
        with open(self.get_block_path(block_id), 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    # Implementación de los métodos gRPC
    # Estos métodos se implementarán cuando se genere el código gRPC
    
    # def StoreBlock(self, request_iterator, context):
    #     """Almacena un bloque de datos enviado por el cliente."""
    #     pass
    
    # def RetrieveBlock(self, request, context):
    #     """Recupera un bloque de datos y lo envía al cliente."""
    #     pass
    
    # def ReplicateBlock(self, request, context):
    #     """Replica un bloque a otro DataNode."""
    #     pass
    
    # def TransferBlock(self, request, context):
    #     """Transfiere un bloque entre DataNodes."""
    #     pass
    
    # def CheckBlock(self, request, context):
    #     """Verifica si un bloque existe en el DataNode."""
    #     pass
    
    # def DeleteBlock(self, request, context):
    #     """Elimina un bloque del DataNode."""
    #     pass


def serve(node_id: str, hostname: str, port: int, storage_dir: str):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Registrar el servicio
    # datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(
    #     DataNodeServicer(storage_dir, node_id, hostname, port), server
    # )
    
    server.add_insecure_port(f'{hostname}:{port}')
    server.start()
    print(f"DataNode {node_id} running at {hostname}:{port}")
    
    # Mantener el servidor en ejecución
    server.wait_for_termination()


if __name__ == "__main__":
    # Ejemplo de uso
    node_id = "datanode1"
    hostname = "localhost"
    port = 50051
    storage_dir = "./data/blocks"
    
    serve(node_id, hostname, port, storage_dir)
