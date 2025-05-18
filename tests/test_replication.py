import unittest
import os
import shutil
import time
import grpc
import threading
from concurrent import futures

from src.datanode.service.datanode_service import serve as serve_datanode
from src.common.proto import datanode_pb2, datanode_pb2_grpc

class TestDataNodeReplication(unittest.TestCase):
    def setUp(self):
        # Crear directorios temporales para los DataNodes
        self.base_dir = "test_data"
        self.datanode1_dir = os.path.join(self.base_dir, "datanode1")
        self.datanode2_dir = os.path.join(self.base_dir, "datanode2")
        
        os.makedirs(self.datanode1_dir, exist_ok=True)
        os.makedirs(self.datanode2_dir, exist_ok=True)
        
        # Configuración de los DataNodes
        self.datanode1_config = {
            "node_id": "datanode1",
            "hostname": "localhost",
            "port": 50051,
            "storage_dir": self.datanode1_dir
        }
        
        self.datanode2_config = {
            "node_id": "datanode2",
            "hostname": "localhost",
            "port": 50052,
            "storage_dir": self.datanode2_dir
        }
        
        # Iniciar los DataNodes en hilos separados
        self.datanode1_thread = threading.Thread(
            target=serve_datanode,
            kwargs=self.datanode1_config,
            daemon=True
        )
        
        self.datanode2_thread = threading.Thread(
            target=serve_datanode,
            kwargs=self.datanode2_config,
            daemon=True
        )
        
        self.datanode1_thread.start()
        self.datanode2_thread.start()
        
        # Esperar a que los servidores estén listos
        time.sleep(2)
        
        # Crear stubs para los DataNodes
        self.datanode1_channel = grpc.insecure_channel(f"{self.datanode1_config['hostname']}:{self.datanode1_config['port']}")
        self.datanode2_channel = grpc.insecure_channel(f"{self.datanode2_config['hostname']}:{self.datanode2_config['port']}")
        
        self.datanode1_stub = datanode_pb2_grpc.DataNodeServiceStub(self.datanode1_channel)
        self.datanode2_stub = datanode_pb2_grpc.DataNodeServiceStub(self.datanode2_channel)
    
    def tearDown(self):
        # Cerrar canales
        self.datanode1_channel.close()
        self.datanode2_channel.close()
        
        # Limpiar directorios de prueba
        shutil.rmtree(self.base_dir)
    
    def test_block_replication(self):
        """Test de replicación de bloques entre DataNodes"""
        # 1. Crear un bloque en el DataNode1
        block_id = "test_block_1"
        test_data = b"This is a test block for replication"
        
        def block_data_iterator():
            yield datanode_pb2.BlockData(
                block_id=block_id,
                data=test_data,
                offset=0,
                total_size=len(test_data)
            )
        
        # Almacenar el bloque en DataNode1
        store_response = self.datanode1_stub.StoreBlock(block_data_iterator())
        self.assertEqual(store_response.status, datanode_pb2.BlockResponse.SUCCESS)
        
        # 2. Replicar el bloque de DataNode1 a DataNode2
        replication_request = datanode_pb2.ReplicationRequest(
            block_id=block_id,
            target_datanode_id=self.datanode2_config["node_id"],
            target_hostname=self.datanode2_config["hostname"],
            target_port=self.datanode2_config["port"]
        )
        
        replication_response = self.datanode1_stub.ReplicateBlock(replication_request)
        self.assertEqual(replication_response.status, datanode_pb2.BlockResponse.SUCCESS)
        
        # 3. Verificar que el bloque existe en ambos DataNodes
        check_response1 = self.datanode1_stub.CheckBlock(datanode_pb2.BlockRequest(block_id=block_id))
        check_response2 = self.datanode2_stub.CheckBlock(datanode_pb2.BlockRequest(block_id=block_id))
        
        self.assertTrue(check_response1.exists)
        self.assertTrue(check_response2.exists)
        
        # 4. Verificar que los checksums coinciden
        self.assertEqual(check_response1.checksum, check_response2.checksum)
        
        # 5. Verificar que los datos son idénticos
        block_data1 = bytearray()
        block_data2 = bytearray()
        
        for chunk in self.datanode1_stub.RetrieveBlock(datanode_pb2.BlockRequest(block_id=block_id)):
            block_data1.extend(chunk.data)
        
        for chunk in self.datanode2_stub.RetrieveBlock(datanode_pb2.BlockRequest(block_id=block_id)):
            block_data2.extend(chunk.data)
        
        self.assertEqual(bytes(block_data1), test_data)
        self.assertEqual(bytes(block_data2), test_data)
        self.assertEqual(bytes(block_data1), bytes(block_data2))

if __name__ == '__main__':
    unittest.main() 