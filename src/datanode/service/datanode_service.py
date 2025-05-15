import grpc
from concurrent import futures
import os
import shutil
import logging
from typing import Dict, Optional, Iterator

# Importamos los módulos necesarios
from src.datanode.storage.block_storage import BlockStorage
from src.datanode.registration import DataNodeRegistration

# Estos imports se usarán cuando se genere el código gRPC
# Para desarrollo, definimos clases mock para poder implementar la funcionalidad
class MockContext:
    def set_code(self, code):
        pass
    
    def set_details(self, details):
        pass

class MockBlockRequest:
    def __init__(self, block_id):
        self.block_id = block_id

class MockBlockData:
    def __init__(self, block_id, data, offset=0, total_size=0):
        self.block_id = block_id
        self.data = data
        self.offset = offset
        self.total_size = total_size

class MockBlockResponse:
    def __init__(self, status=0, message="", block_id=""):
        self.status = status
        self.message = message
        self.block_id = block_id

class MockBlockStatus:
    def __init__(self, exists=False, size=0, checksum=""):
        self.exists = exists
        self.size = size
        self.checksum = checksum

class MockReplicationRequest:
    def __init__(self, block_id, target_datanode_id, target_hostname, target_port):
        self.block_id = block_id
        self.target_datanode_id = target_datanode_id
        self.target_hostname = target_hostname
        self.target_port = target_port

class MockTransferRequest:
    def __init__(self, block_id, source_datanode_id, source_hostname, source_port,
                 target_datanode_id, target_hostname, target_port):
        self.block_id = block_id
        self.source_datanode_id = source_datanode_id
        self.source_hostname = source_hostname
        self.source_port = source_port
        self.target_datanode_id = target_datanode_id
        self.target_hostname = target_hostname
        self.target_port = target_port

class DataNodeServicer:
    def __init__(self, storage_dir: str, node_id: str, hostname: str, port: int, namenode_url: str = None):
        self.node_id = node_id
        self.hostname = hostname
        self.port = port
        self.storage = BlockStorage(storage_dir)
        self.logger = logging.getLogger(f"DataNode-{node_id}")
        
        # Configurar registro con NameNode si se proporciona la URL
        if namenode_url:
            storage_capacity = self._get_storage_capacity(storage_dir)
            self.registration = DataNodeRegistration(
                namenode_url=namenode_url,
                node_id=node_id,
                hostname=hostname,
                port=port,
                storage_capacity=storage_capacity
            )
            self.registration.start_heartbeat_thread(self._get_storage_stats)
    
    def _get_storage_capacity(self, storage_dir: str) -> int:
        try:
            stats = shutil.disk_usage(storage_dir)
            return stats.total
        except Exception as e:
            self.logger.error(f"Error getting storage capacity: {str(e)}")
            return 1000000000  # Default 1GB if can't determine
    
    def _get_storage_stats(self):
        stats = self.storage.get_storage_stats()
        stats["available_space"] = self.storage.get_available_space()
        return stats
    
    # Implementación de los métodos gRPC
    def StoreBlock(self, request_iterator, context):
        """Almacena un bloque de datos enviado por el cliente."""
        block_id = None
        block_data = bytearray()
        
        try:
            for chunk in request_iterator:
                if block_id is None:
                    block_id = chunk.block_id
                block_data.extend(chunk.data)
            
            if block_id is None:
                return MockBlockResponse(
                    status=1,  # ERROR
                    message="No block ID provided"
                )
            
            success = self.storage.store_block(block_id, block_data)
            
            if success:
                return MockBlockResponse(
                    status=0,  # SUCCESS
                    message="Block stored successfully",
                    block_id=block_id
                )
            else:
                return MockBlockResponse(
                    status=1,  # ERROR
                    message="Failed to store block",
                    block_id=block_id
                )
        except Exception as e:
            self.logger.error(f"Error storing block: {str(e)}")
            return MockBlockResponse(
                status=1,  # ERROR
                message=f"Error: {str(e)}",
                block_id=block_id if block_id else ""
            )
    
    def RetrieveBlock(self, request, context):
        """Recupera un bloque de datos y lo envía al cliente."""
        block_id = request.block_id
        
        if not self.storage.block_exists(block_id):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Block {block_id} not found")
            return
        
        try:
            chunks = self.storage.stream_block(block_id)
            if chunks is None:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"Error reading block {block_id}")
                return
            
            for chunk, offset, total_size in chunks:
                yield MockBlockData(
                    block_id=block_id,
                    data=chunk,
                    offset=offset,
                    total_size=total_size
                )
        except Exception as e:
            self.logger.error(f"Error retrieving block: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error: {str(e)}")
    
    def ReplicateBlock(self, request, context):
        """Replica un bloque a otro DataNode."""
        block_id = request.block_id
        target_datanode_id = request.target_datanode_id
        target_hostname = request.target_hostname
        target_port = request.target_port
        
        if not self.storage.block_exists(block_id):
            return MockBlockResponse(
                status=2,  # NOT_FOUND
                message=f"Block {block_id} not found",
                block_id=block_id
            )
        
        try:
            # En una implementación real, aquí estableceríamos una conexión gRPC
            # con el DataNode objetivo y enviaríamos el bloque
            # Para esta implementación básica, simulamos que la replicación fue exitosa
            
            self.logger.info(f"Replicating block {block_id} to {target_datanode_id} at {target_hostname}:{target_port}")
            
            return MockBlockResponse(
                status=0,  # SUCCESS
                message=f"Block {block_id} replicated to {target_datanode_id}",
                block_id=block_id
            )
        except Exception as e:
            self.logger.error(f"Error replicating block: {str(e)}")
            return MockBlockResponse(
                status=1,  # ERROR
                message=f"Error: {str(e)}",
                block_id=block_id
            )
    
    def TransferBlock(self, request, context):
        """Transfiere un bloque entre DataNodes."""
        block_id = request.block_id
        source_datanode_id = request.source_datanode_id
        target_datanode_id = request.target_datanode_id
        
        # En esta implementación básica, simulamos la transferencia
        self.logger.info(f"Transferring block {block_id} from {source_datanode_id} to {target_datanode_id}")
        
        return MockBlockResponse(
            status=0,  # SUCCESS
            message=f"Block {block_id} transferred from {source_datanode_id} to {target_datanode_id}",
            block_id=block_id
        )
    
    def CheckBlock(self, request, context):
        """Verifica si un bloque existe en el DataNode."""
        block_id = request.block_id
        exists = self.storage.block_exists(block_id)
        
        if exists:
            size = self.storage.get_block_size(block_id)
            checksum = self.storage.calculate_checksum(block_id)
            return MockBlockStatus(
                exists=True,
                size=size if size is not None else 0,
                checksum=checksum if checksum is not None else ""
            )
        else:
            return MockBlockStatus(
                exists=False,
                size=0,
                checksum=""
            )
    
    def DeleteBlock(self, request, context):
        """Elimina un bloque del DataNode."""
        block_id = request.block_id
        
        if not self.storage.block_exists(block_id):
            return MockBlockResponse(
                status=2,  # NOT_FOUND
                message=f"Block {block_id} not found",
                block_id=block_id
            )
        
        success = self.storage.delete_block(block_id)
        
        if success:
            return MockBlockResponse(
                status=0,  # SUCCESS
                message=f"Block {block_id} deleted successfully",
                block_id=block_id
            )
        else:
            return MockBlockResponse(
                status=1,  # ERROR
                message=f"Failed to delete block {block_id}",
                block_id=block_id
            )


def serve(node_id: str, hostname: str, port: int, storage_dir: str, namenode_url: str = None):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Crear el servicio
    servicer = DataNodeServicer(storage_dir, node_id, hostname, port, namenode_url)
    
    # Registrar el servicio
    # En una implementación real con gRPC generado:
    # datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(servicer, server)
    
    server.add_insecure_port(f'{hostname}:{port}')
    server.start()
    print(f"DataNode {node_id} running at {hostname}:{port}")
    
    # Mantener el servidor en ejecución
    server.wait_for_termination()


if __name__ == "__main__":
    # Ejemplo de uso
    import argparse
    
    parser = argparse.ArgumentParser(description='DataNode Service')
    parser.add_argument('--node-id', type=str, default="datanode1", help='Unique ID for this DataNode')
    parser.add_argument('--hostname', type=str, default="localhost", help='Hostname to bind')
    parser.add_argument('--port', type=int, default=50051, help='Port to bind')
    parser.add_argument('--storage-dir', type=str, default="./data/blocks", help='Directory to store blocks')
    parser.add_argument('--namenode-url', type=str, help='URL of the NameNode')
    
    args = parser.parse_args()
    
    serve(
        node_id=args.node_id,
        hostname=args.hostname,
        port=args.port,
        storage_dir=args.storage_dir,
        namenode_url=args.namenode_url
    )
