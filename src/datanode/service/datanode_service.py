import grpc
from concurrent import futures
import os
import shutil
import logging
from typing import Dict, Optional, Iterator

# Importamos los módulos necesarios
from src.datanode.storage.block_storage import BlockStorage
from src.datanode.registration import DataNodeRegistration

# Importamos los módulos generados por gRPC
from src.common.proto import datanode_pb2, datanode_pb2_grpc

class DataNodeServicer(datanode_pb2_grpc.DataNodeServiceServicer):
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
        # Obtener estadísticas completas del almacenamiento
        stats = self.storage.get_storage_stats()
        
        # Registrar información en el log
        self.logger.info(f"DataNode {self.node_id} storage stats: "
                        f"{stats['total_blocks']} blocks, "
                        f"{stats['total_size']} bytes used, "
                        f"{stats['available_space']} bytes available")
        
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
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.ERROR,
                    message="No block ID provided"
                )
            
            # Verificar si el bloque ya existe
            if self.storage.block_exists(block_id):
                self.logger.warning(f"Block {block_id} already exists, overwriting")
            
            # Almacenar el bloque
            success = self.storage.store_block(block_id, block_data)
            
            # Calcular el checksum para verificar la integridad
            checksum = None
            if success:
                checksum = self.storage.calculate_checksum(block_id)
                self.logger.info(f"Stored block {block_id} with checksum {checksum}")
            
            if success:
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.SUCCESS,
                    message=f"Block stored successfully with checksum {checksum}",
                    block_id=block_id
                )
            else:
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.ERROR,
                    message="Failed to store block",
                    block_id=block_id
                )
        except Exception as e:
            self.logger.error(f"Error storing block: {str(e)}")
            return datanode_pb2.BlockResponse(
                status=datanode_pb2.BlockResponse.ERROR,
                message=f"Error: {str(e)}",
                block_id=block_id if block_id else ""
            )
    
    def RetrieveBlock(self, request, context):
        """Recupera un bloque de datos y lo envía al cliente."""
        block_id = request.block_id
        
        # Verificar si el bloque existe
        if not self.storage.block_exists(block_id):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Block {block_id} not found")
            return
        
        try:
            # Verificar la integridad del bloque antes de enviarlo
            stored_checksum = self.storage.calculate_checksum(block_id)
            if not stored_checksum:
                context.set_code(grpc.StatusCode.DATA_LOSS)
                context.set_details(f"Block {block_id} appears to be corrupted")
                return
            
            # Obtener los chunks del bloque
            chunks = self.storage.stream_block(block_id)
            if chunks is None:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"Error reading block {block_id}")
                return
            
            # Enviar los chunks al cliente
            self.logger.info(f"Retrieving block {block_id} with checksum {stored_checksum}")
            for chunk, offset, total_size in chunks:
                yield datanode_pb2.BlockData(
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
        
        # Verificar si el bloque existe
        if not self.storage.block_exists(block_id):
            return datanode_pb2.BlockResponse(
                status=datanode_pb2.BlockResponse.NOT_FOUND,
                message=f"Block {block_id} not found",
                block_id=block_id
            )
        
        try:
            # Obtener los datos del bloque
            block_data = self.storage.retrieve_block(block_id)
            if not block_data:
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.ERROR,
                    message=f"Error reading block {block_id}",
                    block_id=block_id
                )
            
            # Establecer conexión con el DataNode objetivo
            try:
                channel = grpc.insecure_channel(f"{target_hostname}:{target_port}")
                stub = datanode_pb2_grpc.DataNodeServiceStub(channel)
                
                # Enviar el bloque al DataNode objetivo
                def block_data_iterator():
                    chunk_size = 4096
                    total_size = len(block_data)
                    
                    for i in range(0, total_size, chunk_size):
                        chunk = block_data[i:i+chunk_size]
                        yield datanode_pb2.BlockData(
                            block_id=block_id,
                            data=chunk,
                            offset=i,
                            total_size=total_size
                        )
                
                response = stub.StoreBlock(block_data_iterator())
                channel.close()
                
                if response.status == datanode_pb2.BlockResponse.SUCCESS:
                    self.logger.info(f"Block {block_id} replicated to {target_datanode_id} at {target_hostname}:{target_port}")
                    return datanode_pb2.BlockResponse(
                        status=datanode_pb2.BlockResponse.SUCCESS,
                        message=f"Block {block_id} replicated to {target_datanode_id}",
                        block_id=block_id
                    )
                else:
                    self.logger.error(f"Failed to replicate block {block_id}: {response.message}")
                    return datanode_pb2.BlockResponse(
                        status=datanode_pb2.BlockResponse.ERROR,
                        message=f"Failed to replicate block: {response.message}",
                        block_id=block_id
                    )
            except Exception as e:
                self.logger.error(f"Error connecting to target DataNode: {str(e)}")
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.ERROR,
                    message=f"Error connecting to target DataNode: {str(e)}",
                    block_id=block_id
                )
        except Exception as e:
            self.logger.error(f"Error replicating block: {str(e)}")
            return datanode_pb2.BlockResponse(
                status=datanode_pb2.BlockResponse.ERROR,
                message=f"Error: {str(e)}",
                block_id=block_id
            )
    
    def TransferBlock(self, request, context):
        """Transfiere un bloque entre DataNodes."""
        block_id = request.block_id
        source_datanode_id = request.source_datanode_id
        source_hostname = request.source_hostname
        source_port = request.source_port
        target_datanode_id = request.target_datanode_id
        target_hostname = request.target_hostname
        target_port = request.target_port
        
        try:
            # Establecer conexión con el DataNode origen
            source_channel = grpc.insecure_channel(f"{source_hostname}:{source_port}")
            source_stub = datanode_pb2_grpc.DataNodeServiceStub(source_channel)
            
            # Establecer conexión con el DataNode destino
            target_channel = grpc.insecure_channel(f"{target_hostname}:{target_port}")
            target_stub = datanode_pb2_grpc.DataNodeServiceStub(target_channel)
            
            # Solicitar el bloque al DataNode origen
            block_request = datanode_pb2.BlockRequest(block_id=block_id)
            block_data = bytearray()
            
            try:
                # Recuperar el bloque del DataNode origen
                for chunk in source_stub.RetrieveBlock(block_request):
                    block_data.extend(chunk.data)
                
                # Enviar el bloque al DataNode destino
                def block_data_iterator():
                    chunk_size = 4096
                    total_size = len(block_data)
                    
                    for i in range(0, total_size, chunk_size):
                        chunk = block_data[i:i+chunk_size]
                        yield datanode_pb2.BlockData(
                            block_id=block_id,
                            data=chunk,
                            offset=i,
                            total_size=total_size
                        )
                
                # Almacenar el bloque en el DataNode destino
                response = target_stub.StoreBlock(block_data_iterator())
                
                # Cerrar las conexiones
                source_channel.close()
                target_channel.close()
                
                if response.status == datanode_pb2.BlockResponse.SUCCESS:
                    self.logger.info(f"Block {block_id} transferred from {source_datanode_id} to {target_datanode_id}")
                    return datanode_pb2.BlockResponse(
                        status=datanode_pb2.BlockResponse.SUCCESS,
                        message=f"Block {block_id} transferred from {source_datanode_id} to {target_datanode_id}",
                        block_id=block_id
                    )
                else:
                    self.logger.error(f"Failed to transfer block {block_id}: {response.message}")
                    return datanode_pb2.BlockResponse(
                        status=datanode_pb2.BlockResponse.ERROR,
                        message=f"Failed to transfer block: {response.message}",
                        block_id=block_id
                    )
            except Exception as e:
                self.logger.error(f"Error during block transfer: {str(e)}")
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.ERROR,
                    message=f"Error during block transfer: {str(e)}",
                    block_id=block_id
                )
        except Exception as e:
            self.logger.error(f"Error setting up transfer: {str(e)}")
            return datanode_pb2.BlockResponse(
                status=datanode_pb2.BlockResponse.ERROR,
                message=f"Error setting up transfer: {str(e)}",
                block_id=block_id
            )
    
    def CheckBlock(self, request, context):
        """Verifica si un bloque existe en el DataNode."""
        block_id = request.block_id
        exists = self.storage.block_exists(block_id)
        
        if exists:
            size = self.storage.get_block_size(block_id)
            checksum = self.storage.calculate_checksum(block_id)
            self.logger.info(f"Block {block_id} exists with size {size} and checksum {checksum}")
            return datanode_pb2.BlockStatus(
                exists=True,
                size=size if size is not None else 0,
                checksum=checksum if checksum is not None else ""
            )
        else:
            self.logger.info(f"Block {block_id} does not exist")
            return datanode_pb2.BlockStatus(
                exists=False,
                size=0,
                checksum=""
            )
    
    def DeleteBlock(self, request, context):
        """Elimina un bloque del DataNode."""
        block_id = request.block_id
        
        if not self.storage.block_exists(block_id):
            self.logger.warning(f"Attempted to delete non-existent block {block_id}")
            return datanode_pb2.BlockResponse(
                status=datanode_pb2.BlockResponse.NOT_FOUND,
                message=f"Block {block_id} not found",
                block_id=block_id
            )
        
        success = self.storage.delete_block(block_id)
        
        if success:
            self.logger.info(f"Block {block_id} deleted successfully")
            return datanode_pb2.BlockResponse(
                status=datanode_pb2.BlockResponse.SUCCESS,
                message=f"Block {block_id} deleted successfully",
                block_id=block_id
            )
        else:
            self.logger.error(f"Failed to delete block {block_id}")
            return datanode_pb2.BlockResponse(
                status=datanode_pb2.BlockResponse.ERROR,
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
    
    # Registrar el servicio con gRPC
    datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(servicer, server)
    
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
