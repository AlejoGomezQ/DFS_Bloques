import unittest
import os
import shutil
import time
import grpc
import threading
from concurrent import futures

from src.datanode.service.datanode_service import serve as serve_datanode
from src.common.proto import datanode_pb2, datanode_pb2_grpc
from src.namenode.metadata.manager import MetadataManager
from src.namenode.replication.block_replicator import BlockReplicator
from src.namenode.api.models import DataNodeStatus

class TestBlockReplication(unittest.TestCase):
    def setUp(self):
        # Crear directorios temporales para los DataNodes
        self.base_dir = "test_data"
        self.datanode1_dir = os.path.join(self.base_dir, "datanode1")
        self.datanode2_dir = os.path.join(self.base_dir, "datanode2")
        self.datanode3_dir = os.path.join(self.base_dir, "datanode3")
        
        os.makedirs(self.datanode1_dir, exist_ok=True)
        os.makedirs(self.datanode2_dir, exist_ok=True)
        os.makedirs(self.datanode3_dir, exist_ok=True)
        
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
        
        self.datanode3_config = {
            "node_id": "datanode3",
            "hostname": "localhost",
            "port": 50053,
            "storage_dir": self.datanode3_dir
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
        
        self.datanode3_thread = threading.Thread(
            target=serve_datanode,
            kwargs=self.datanode3_config,
            daemon=True
        )
        
        self.datanode1_thread.start()
        self.datanode2_thread.start()
        self.datanode3_thread.start()
        
        # Esperar a que los servidores estén listos
        time.sleep(2)
        
        # Crear stubs para los DataNodes
        self.datanode1_channel = grpc.insecure_channel(f"{self.datanode1_config['hostname']}:{self.datanode1_config['port']}")
        self.datanode2_channel = grpc.insecure_channel(f"{self.datanode2_config['hostname']}:{self.datanode2_config['port']}")
        self.datanode3_channel = grpc.insecure_channel(f"{self.datanode3_config['hostname']}:{self.datanode3_config['port']}")
        
        self.datanode1_stub = datanode_pb2_grpc.DataNodeServiceStub(self.datanode1_channel)
        self.datanode2_stub = datanode_pb2_grpc.DataNodeServiceStub(self.datanode2_channel)
        self.datanode3_stub = datanode_pb2_grpc.DataNodeServiceStub(self.datanode3_channel)
        
        # Inicializar el gestor de metadatos y el replicador de bloques
        self.metadata_manager = MetadataManager(db_path=os.path.join(self.base_dir, "test.db"))
        self.block_replicator = BlockReplicator(self.metadata_manager)
        
        # Registrar los DataNodes
        self.datanode1 = self.metadata_manager.register_datanode(
            hostname=self.datanode1_config["hostname"],
            port=self.datanode1_config["port"],
            storage_capacity=1000000000,  # 1GB
            available_space=1000000000
        )
        
        self.datanode2 = self.metadata_manager.register_datanode(
            hostname=self.datanode2_config["hostname"],
            port=self.datanode2_config["port"],
            storage_capacity=1000000000,  # 1GB
            available_space=1000000000
        )
        
        self.datanode3 = self.metadata_manager.register_datanode(
            hostname=self.datanode3_config["hostname"],
            port=self.datanode3_config["port"],
            storage_capacity=1000000000,  # 1GB
            available_space=1000000000
        )
    
    def tearDown(self):
        # Cerrar canales
        self.datanode1_channel.close()
        self.datanode2_channel.close()
        self.datanode3_channel.close()
        
        # Limpiar directorios de prueba
        shutil.rmtree(self.base_dir)
    
    def test_block_re_replication(self):
        """Test de re-replicación de bloques después de un fallo"""
        # 1. Crear un bloque en el DataNode1
        block_id = "test_block_1"
        test_data = b"This is a test block for re-replication"
        
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
        
        # 2. Replicar el bloque a DataNode2
        replication_request = datanode_pb2.ReplicationRequest(
            block_id=block_id,
            target_datanode_id=self.datanode2_config["node_id"],
            target_hostname=self.datanode2_config["hostname"],
            target_port=self.datanode2_config["port"]
        )
        
        replication_response = self.datanode1_stub.ReplicateBlock(replication_request)
        self.assertEqual(replication_response.status, datanode_pb2.BlockResponse.SUCCESS)
        
        # 3. Registrar las ubicaciones del bloque en el metadata
        self.metadata_manager.add_block_location(block_id, self.datanode1.node_id, True)
        self.metadata_manager.add_block_location(block_id, self.datanode2.node_id, False)
        
        # 4. Simular fallo del DataNode2
        self.metadata_manager.update_datanode_status(self.datanode2.node_id, DataNodeStatus.INACTIVE)
        
        # 5. Intentar re-replicar el bloque
        success = self.block_replicator.handle_block_replication(block_id, self.datanode2.node_id)
        self.assertTrue(success)
        
        # 6. Verificar que el bloque existe en DataNode3
        check_response = self.datanode3_stub.CheckBlock(datanode_pb2.BlockRequest(block_id=block_id))
        self.assertTrue(check_response.exists)
        
        # 7. Verificar que los datos son idénticos
        block_data = bytearray()
        for chunk in self.datanode3_stub.RetrieveBlock(datanode_pb2.BlockRequest(block_id=block_id)):
            block_data.extend(chunk.data)
        
        self.assertEqual(bytes(block_data), test_data)
        
        # 8. Verificar que la ubicación del bloque se actualizó en el metadata
        block_info = self.metadata_manager.get_block_info(block_id)
        self.assertIsNotNone(block_info)
        self.assertEqual(len(block_info.locations), 2)  # DataNode1 y DataNode3
        self.assertTrue(any(loc.datanode_id == self.datanode3.node_id for loc in block_info.locations))

if __name__ == '__main__':
    unittest.main() 