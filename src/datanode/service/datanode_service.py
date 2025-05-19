import grpc
from concurrent import futures
import os
import shutil
import logging
import time
from typing import Dict, Optional, Iterator, Any, Tuple

# Importamos los módulos necesarios
from src.datanode.storage.block_storage import BlockStorage
from src.datanode.registration import DataNodeRegistration
from src.common.compression import DataCompressor, COMPRESSION_BALANCED

# Importamos los módulos generados por gRPC
from src.common.proto import datanode_pb2, datanode_pb2_grpc

class DataNodeServicer(datanode_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, storage_dir: str, node_id: str, hostname: str, port: int, namenode_url: str = None):
        self.node_id = node_id
        self.hostname = hostname
        self.port = port
        self.storage = BlockStorage(storage_dir)
        self.logger = logging.getLogger(f"DataNode-{node_id}")
        
        # Inicializar el compresor de datos
        self.compressor = DataCompressor(auto_select=True)
        
        # Estadísticas de transferencia
        self.transfer_stats = {
            "bytes_sent": 0,
            "bytes_received": 0,
            "compressed_bytes_sent": 0,
            "compressed_bytes_received": 0,
            "compression_time": 0,
            "decompression_time": 0,
            "transfer_time": 0,
            "blocks_compressed": 0,
            "blocks_uncompressed": 0,
            "blocks_transferred": 0,
            "blocks_transfer_failed": 0
        }
        
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
        
        # Añadir estadísticas de transferencia y compresión
        stats.update({
            "transfer_stats": self.transfer_stats.copy()
        })
        
        # Calcular ratios de compresión
        if self.transfer_stats["bytes_sent"] > 0:
            stats["compression_ratio_sent"] = self.transfer_stats["bytes_sent"] / \
                                          self.transfer_stats["compressed_bytes_sent"] \
                if self.transfer_stats["compressed_bytes_sent"] > 0 else 1.0
        else:
            stats["compression_ratio_sent"] = 1.0
            
        if self.transfer_stats["bytes_received"] > 0:
            stats["compression_ratio_received"] = self.transfer_stats["bytes_received"] / \
                                              self.transfer_stats["compressed_bytes_received"] \
                if self.transfer_stats["compressed_bytes_received"] > 0 else 1.0
        else:
            stats["compression_ratio_received"] = 1.0
        
        # Registrar información en el log
        self.logger.info(f"DataNode {self.node_id} storage stats: "
                        f"{stats['total_blocks']} blocks, "
                        f"{stats['total_size']} bytes used, "
                        f"{stats['available_space']} bytes available, "
                        f"compression ratio: {stats.get('compression_ratio_sent', 1.0):.2f}x sent, "
                        f"{stats.get('compression_ratio_received', 1.0):.2f}x received")
        
        return stats
    
    # Implementación de los métodos gRPC
    def StoreBlock(self, request_iterator, context):
        """Almacena un bloque de datos enviado por el cliente."""
        block_id = None
        data = bytearray()
        total_size = 0
        original_size = 0
        is_compressed = False
        compression_metadata = {}
        start_time = time.time()
        
        try:
            # Recopilar todos los chunks del bloque
            first_chunk = True
            for chunk in request_iterator:
                if first_chunk:
                    block_id = chunk.block_id
                    total_size = chunk.total_size
                    
                    # Procesar metadatos de compresión si están presentes
                    if hasattr(chunk, 'compressed') and chunk.compressed:
                        is_compressed = chunk.compressed
                        original_size = chunk.original_size
                        if hasattr(chunk, 'compression_metadata') and chunk.compression_metadata:
                            try:
                                metadata_str = chunk.compression_metadata.decode('utf-8')
                                compression_metadata = eval(metadata_str)
                            except Exception as e:
                                self.logger.error(f"Error parsing compression metadata: {str(e)}")
                    
                    first_chunk = False
                
                data.extend(chunk.data)
            
            if not block_id:
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.ERROR,
                    message="No block ID provided",
                    block_id=""
                )
            
            # Actualizar estadísticas de transferencia
            compressed_size = len(data)
            self.transfer_stats["compressed_bytes_received"] += compressed_size
            
            # Descomprimir los datos si es necesario
            if is_compressed:
                decompression_start = time.time()
                try:
                    uncompressed_data = self.compressor.decompress(bytes(data), compression_metadata)
                    data = uncompressed_data
                    decompression_time = time.time() - decompression_start
                    self.transfer_stats["decompression_time"] += decompression_time
                    self.transfer_stats["bytes_received"] += original_size
                    self.transfer_stats["blocks_compressed"] += 1
                    
                    # Mostrar estadísticas de descompresión
                    ratio = len(data) / compressed_size if compressed_size > 0 else 1.0
                    algorithm = compression_metadata.get("algorithm", "unknown")
                    self.logger.info(f"Block {block_id} decompressed: {compressed_size/1024:.1f}KB → "
                                    f"{len(data)/1024:.1f}KB (ratio: {ratio:.1f}x, algorithm: {algorithm})")
                except Exception as e:
                    self.logger.error(f"Error decompressing data: {str(e)}")
                    # Continuar con los datos comprimidos si hay error
                    self.transfer_stats["bytes_received"] += compressed_size
                    self.transfer_stats["blocks_uncompressed"] += 1
            else:
                self.transfer_stats["bytes_received"] += compressed_size
                self.transfer_stats["blocks_uncompressed"] += 1
            
            # Actualizar tiempo de transferencia
            self.transfer_stats["transfer_time"] += time.time() - start_time
            
            # Almacenar el bloque y obtener el checksum
            success, checksum = self.storage.store_block(block_id, bytes(data))
            
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
        start_time = time.time()
        
        try:
            # Verificar si el bloque existe
            if not self.storage.block_exists(block_id):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Block {block_id} not found")
                return
            
            # Obtener los datos del bloque
            data = self.storage.retrieve_block(block_id)
            if not data:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"Failed to retrieve block {block_id}")
                return
            
            # Comprimir los datos si son lo suficientemente grandes
            original_size = len(data)
            compression_metadata = {}
            compressed_data = data
            is_compressed = False
            
            # Solo comprimir bloques de más de 512 bytes
            if original_size > 512:
                compression_start = time.time()
                compressed_data, compression_metadata = self.compressor.compress(data)
                compression_time = time.time() - compression_start
                
                # Usar datos comprimidos si la compresión fue efectiva
                if compression_metadata.get("compressed", False):
                    is_compressed = True
                    self.transfer_stats["compression_time"] += compression_time
                    self.transfer_stats["blocks_compressed"] += 1
            
            # Descomprimir si es necesario
            if is_compressed:
                start_decomp = time.time()
                try:
                    block_data = self.compressor.decompress(block_data, compression_metadata)
                    self.transfer_stats["decompression_time"] += time.time() - start_decomp
                    self.transfer_stats["compressed_bytes_received"] += len(block_data)
                except Exception as e:
                    self.logger.error(f"Error decompressing block {block_id}: {str(e)}")
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"Error decompressing block: {str(e)}")
                    return
            
            # Actualizar estadísticas
            self.transfer_stats["bytes_received"] += len(block_data)
            
            # Enviar el bloque en chunks
            chunk_size = 4 * 1024 * 1024  # 4MB
            total_size = len(block_data)
            
            for i in range(0, total_size, chunk_size):
                chunk = block_data[i:i+chunk_size]
                yield datanode_pb2.BlockData(
                    block_id=block_id,
                    data=chunk,
                    offset=i,
                    total_size=total_size,
                    original_size=original_size,
                    compressed=is_compressed,
                    compression_metadata=compression_metadata
                )
                
            self.logger.info(f"Block {block_id} retrieved successfully ({total_size} bytes)")
            
            # Actualizar tiempo de transferencia
            self.transfer_stats["transfer_time"] += time.time() - start_time
            
        except Exception as e:
            self.logger.error(f"Error retrieving block {block_id}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error: {str(e)}")
            return
    
    def ReplicateBlock(self, request, context):
        """Replica un bloque a otro DataNode siguiendo el protocolo Leader-Follower."""
        block_id = request.block_id
        target_datanode_id = request.target_datanode_id
        target_hostname = request.target_hostname
        target_port = request.target_port
        
        try:
            # Verificar si el bloque existe
            if not self.storage.block_exists(block_id):
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.NOT_FOUND,
                    message=f"Block {block_id} not found",
                    block_id=block_id
                )
            
            # Obtener los datos del bloque y su checksum
            block_data = self.storage.retrieve_block(block_id)
            if not block_data:
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.ERROR,
                    message=f"Error reading block {block_id}",
                    block_id=block_id
                )
            
            # Calcular checksum del bloque
            import hashlib
            checksum = hashlib.sha256(block_data).hexdigest()
            
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
                
                # Enviar el bloque al DataNode objetivo
                response = stub.StoreBlock(block_data_iterator())
                
                if response.status == datanode_pb2.BlockResponse.SUCCESS:
                    # Verificar la integridad del bloque replicado
                    verify_request = datanode_pb2.BlockRequest(block_id=block_id)
                    verify_response = stub.CheckBlock(verify_request)
                    
                    if verify_response.exists and verify_response.checksum == checksum:
                        self.logger.info(f"Block {block_id} replicated successfully to {target_datanode_id} with verified integrity")
                        return datanode_pb2.BlockResponse(
                            status=datanode_pb2.BlockResponse.SUCCESS,
                            message=f"Block {block_id} replicated to {target_datanode_id} with verified integrity",
                            block_id=block_id
                        )
                    else:
                        self.logger.error(f"Block integrity verification failed for {block_id} on {target_datanode_id}")
                        return datanode_pb2.BlockResponse(
                            status=datanode_pb2.BlockResponse.ERROR,
                            message=f"Block integrity verification failed",
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
            self.logger.error(f"Error replicating block {block_id}: {str(e)}")
            return datanode_pb2.BlockResponse(
                status=datanode_pb2.BlockResponse.ERROR,
                message=f"Error replicating block: {str(e)}",
                block_id=block_id
            )
    
    def TransferBlock(self, request, context):
        """Transfiere un bloque a otro DataNode para balanceo de carga."""
        block_id = request.block_id
        target_datanode_id = request.target_datanode_id
        target_hostname = request.target_hostname
        target_port = request.target_port
        start_time = time.time()
        
        self.logger.info(f"Iniciando transferencia del bloque {block_id} a {target_datanode_id} ({target_hostname}:{target_port})")
        
        try:
            # Verificar si el bloque existe
            if not self.storage.block_exists(block_id):
                self.logger.warning(f"Bloque {block_id} no encontrado para transferencia")
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.NOT_FOUND,
                    message=f"Block {block_id} not found",
                    block_id=block_id
                )
            
            # Leer el bloque
            block_data = self.storage.read_block(block_id)
            if not block_data:
                self.logger.error(f"Error leyendo bloque {block_id} para transferencia")
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.ERROR,
                    message=f"Error reading block {block_id}",
                    block_id=block_id
                )
            
            # Obtener información del bloque
            block_info = self.storage.get_block_info(block_id)
            block_size = block_info.get('size', len(block_data))
            
            try:
                # Establecer conexión con el DataNode destino
                self.logger.info(f"Conectando con DataNode destino {target_hostname}:{target_port}")
                target_channel = grpc.insecure_channel(f"{target_hostname}:{target_port}")
                target_stub = datanode_pb2_grpc.DataNodeServiceStub(target_channel)
                
                # Preparar datos para envío
                chunk_size = 4 * 1024 * 1024  # 4MB por chunk
                total_size = len(block_data)
                
                def block_data_iterator():
                    for i in range(0, total_size, chunk_size):
                        chunk = block_data[i:i+chunk_size]
                        yield datanode_pb2.BlockData(
                            block_id=block_id,
                            data=chunk,
                            offset=i,
                            total_size=total_size,
                            original_size=total_size,
                            compressed=False
                        )
                
                # Enviar el bloque al DataNode destino
                self.logger.info(f"Enviando bloque {block_id} ({total_size/1024/1024:.2f} MB) a {target_datanode_id}")
                response = target_stub.StoreBlock(block_data_iterator())
                target_channel.close()
                
                # Verificar respuesta
                if response.status == datanode_pb2.BlockResponse.SUCCESS:
                    # Actualizar estadísticas
                    self.transfer_stats["bytes_sent"] += total_size
                    self.transfer_stats["blocks_transferred"] += 1
                    
                    # Calcular tiempo y velocidad de transferencia
                    transfer_time = time.time() - start_time
                    transfer_rate = total_size / transfer_time if transfer_time > 0 else 0
                    
                    self.logger.info(f"Bloque {block_id} transferido exitosamente a {target_datanode_id} "
                                    f"({total_size/1024/1024:.2f} MB en {transfer_time:.2f}s, "
                                    f"velocidad: {transfer_rate/1024/1024:.2f} MB/s)")
                    
                    return datanode_pb2.BlockResponse(
                        status=datanode_pb2.BlockResponse.SUCCESS,
                        message=f"Block {block_id} transferred successfully to {target_datanode_id}",
                        block_id=block_id
                    )
                else:
                    self.transfer_stats["blocks_transfer_failed"] += 1
                    self.logger.error(f"Error al transferir bloque {block_id}: {response.message}")
                    return datanode_pb2.BlockResponse(
                        status=datanode_pb2.BlockResponse.ERROR,
                        message=f"Error transferring block: {response.message}",
                        block_id=block_id
                    )
            
            except Exception as e:
                self.transfer_stats["blocks_transfer_failed"] += 1
                self.logger.error(f"Error durante la transferencia del bloque {block_id}: {str(e)}")
                return datanode_pb2.BlockResponse(
                    status=datanode_pb2.BlockResponse.ERROR,
                    message=f"Error during transfer: {str(e)}",
                    block_id=block_id
                )
                
        except Exception as e:
            self.logger.error(f"Error preparando transferencia del bloque {block_id}: {str(e)}")
            return datanode_pb2.BlockResponse(
                status=datanode_pb2.BlockResponse.ERROR,
                message=f"Error preparing transfer: {str(e)}",
                block_id=block_id
            )
    
    def GetTransferStats(self, request, context):
        """Obtiene estadísticas de transferencia del DataNode."""
        try:
            # Copiar estadísticas actuales
            stats = self.transfer_stats.copy()
            
            # Calcular ratios de compresión
            if stats["bytes_sent"] > 0 and stats["compressed_bytes_sent"] > 0:
                stats["compression_ratio_sent"] = stats["bytes_sent"] / stats["compressed_bytes_sent"]
            else:
                stats["compression_ratio_sent"] = 1.0
                
            if stats["bytes_received"] > 0 and stats["compressed_bytes_received"] > 0:
                stats["compression_ratio_received"] = stats["bytes_received"] / stats["compressed_bytes_received"]
            else:
                stats["compression_ratio_received"] = 1.0
            
            # Convertir a mensaje protobuf
            return datanode_pb2.TransferStats(
                bytes_sent=stats["bytes_sent"],
                bytes_received=stats["bytes_received"],
                compressed_bytes_sent=stats["compressed_bytes_sent"],
                compressed_bytes_received=stats["compressed_bytes_received"],
                compression_ratio_sent=stats["compression_ratio_sent"],
                compression_ratio_received=stats["compression_ratio_received"],
                blocks_compressed=stats["blocks_compressed"],
                blocks_uncompressed=stats["blocks_uncompressed"],
                blocks_transferred=stats["blocks_transferred"],
                blocks_transfer_failed=stats["blocks_transfer_failed"]
            )
        except Exception as e:
            self.logger.error(f"Error obteniendo estadísticas de transferencia: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error getting transfer stats: {str(e)}")
            return datanode_pb2.TransferStats()
    
    def CheckBlock(self, request, context):
        """Verifica si un bloque existe y su integridad."""
        block_id = request.block_id
        
        try:
            # Obtener información del bloque
            block_info = self.storage.get_block_info(block_id)
            
            return datanode_pb2.BlockStatus(
                exists=block_info["exists"],
                size=block_info["size"],
                checksum=block_info["checksum"]
            )
        except Exception as e:
            self.logger.error(f"Error checking block {block_id}: {str(e)}")
            return datanode_pb2.BlockStatus(
                exists=False,
                size=0,
                checksum=""
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
        """Verifica si un bloque existe y su integridad."""
        block_id = request.block_id
        
        try:
            # Obtener información del bloque
            block_info = self.storage.get_block_info(block_id)
            
            return datanode_pb2.BlockStatus(
                exists=block_info["exists"],
                size=block_info["size"],
                checksum=block_info["checksum"]
            )
        except Exception as e:
            self.logger.error(f"Error checking block {block_id}: {str(e)}")
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
