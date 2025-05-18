import unittest
import os
import shutil
import time
import grpc
import threading
from concurrent import futures

from src.namenode.leader.leader_election import LeaderElection
from src.namenode.sync.metadata_sync import MetadataSync
from src.namenode.metadata.manager import MetadataManager
from src.common.proto import namenode_pb2, namenode_pb2_grpc

class TestNameNodeFollower(unittest.TestCase):
    def setUp(self):
        # Crear directorios temporales para los NameNodes
        self.base_dir = "test_data"
        self.namenode1_dir = os.path.join(self.base_dir, "namenode1")
        self.namenode2_dir = os.path.join(self.base_dir, "namenode2")
        
        os.makedirs(self.namenode1_dir, exist_ok=True)
        os.makedirs(self.namenode2_dir, exist_ok=True)
        
        # Configuración de los NameNodes
        self.namenode1_config = {
            "node_id": "namenode1",
            "hostname": "localhost",
            "port": 50061
        }
        
        self.namenode2_config = {
            "node_id": "namenode2",
            "hostname": "localhost",
            "port": 50062
        }
        
        # Inicializar gestores de metadatos
        self.metadata_manager1 = MetadataManager(db_path=os.path.join(self.namenode1_dir, "metadata.db"))
        self.metadata_manager2 = MetadataManager(db_path=os.path.join(self.namenode2_dir, "metadata.db"))
        
        # Inicializar sistemas de elección de líder
        self.leader_election1 = LeaderElection(
            node_id=self.namenode1_config["node_id"],
            hostname=self.namenode1_config["hostname"],
            port=self.namenode1_config["port"]
        )
        
        self.leader_election2 = LeaderElection(
            node_id=self.namenode2_config["node_id"],
            hostname=self.namenode2_config["hostname"],
            port=self.namenode2_config["port"]
        )
        
        # Inicializar servicios de sincronización
        self.metadata_sync1 = MetadataSync(self.metadata_manager1)
        self.metadata_sync2 = MetadataSync(self.metadata_manager2)
        
        # Configurar callbacks
        self.leader_election1.on_leader_elected = lambda: self.metadata_sync1.start()
        self.leader_election2.on_leader_elected = lambda: self.metadata_sync2.start()
        
        # Añadir nodos conocidos
        self.leader_election1.add_node(
            self.namenode2_config["node_id"],
            self.namenode2_config["hostname"],
            self.namenode2_config["port"]
        )
        
        self.leader_election2.add_node(
            self.namenode1_config["node_id"],
            self.namenode1_config["hostname"],
            self.namenode1_config["port"]
        )
    
    def tearDown(self):
        # Detener servicios
        self.leader_election1.stop()
        self.leader_election2.stop()
        self.metadata_sync1.stop()
        self.metadata_sync2.stop()
        
        # Limpiar directorios de prueba
        shutil.rmtree(self.base_dir)
    
    def test_leader_election(self):
        """Test de elección de líder entre NameNodes"""
        # 1. Iniciar sistemas de elección
        self.leader_election1.start()
        self.leader_election2.start()
        
        # Esperar a que se complete la elección
        time.sleep(10)
        
        # 2. Verificar que solo un nodo es líder
        self.assertTrue(
            self.leader_election1.is_leader != self.leader_election2.is_leader,
            "Solo un NameNode debe ser líder"
        )
        
        # 3. Verificar que el líder no líder conoce al líder
        if self.leader_election1.is_leader:
            self.assertEqual(self.leader_election2.current_leader, self.namenode1_config["node_id"])
        else:
            self.assertEqual(self.leader_election1.current_leader, self.namenode2_config["node_id"])
    
    def test_metadata_sync(self):
        """Test de sincronización de metadatos"""
        # 1. Iniciar sistemas
        self.leader_election1.start()
        self.leader_election2.start()
        
        # Esperar a que se complete la elección
        time.sleep(10)
        
        # 2. Crear algunos metadatos en el líder
        if self.leader_election1.is_leader:
            self.metadata_manager1.register_datanode("datanode1", "localhost", 50051, 1000000, 1000000)
            self.metadata_manager1.register_datanode("datanode2", "localhost", 50052, 1000000, 1000000)
        else:
            self.metadata_manager2.register_datanode("datanode1", "localhost", 50051, 1000000, 1000000)
            self.metadata_manager2.register_datanode("datanode2", "localhost", 50052, 1000000, 1000000)
        
        # 3. Esperar a que se sincronicen los metadatos
        time.sleep(10)
        
        # 4. Verificar que los metadatos están sincronizados
        datanodes1 = self.metadata_manager1.list_datanodes()
        datanodes2 = self.metadata_manager2.list_datanodes()
        
        self.assertEqual(len(datanodes1), len(datanodes2))
        self.assertEqual(
            {dn.node_id for dn in datanodes1},
            {dn.node_id for dn in datanodes2}
        )
    
    def test_failover(self):
        """Test de failover cuando el líder falla"""
        # 1. Iniciar sistemas
        self.leader_election1.start()
        self.leader_election2.start()
        
        # Esperar a que se complete la elección
        time.sleep(10)
        
        # 2. Identificar el líder actual
        if self.leader_election1.is_leader:
            leader = self.leader_election1
            follower = self.leader_election2
        else:
            leader = self.leader_election2
            follower = self.leader_election1
        
        # 3. Simular fallo del líder
        leader.stop()
        
        # 4. Esperar a que se complete la nueva elección
        time.sleep(10)
        
        # 5. Verificar que el follower se convirtió en líder
        self.assertTrue(follower.is_leader)
        self.assertEqual(follower.current_leader, follower.node_id)

if __name__ == '__main__':
    unittest.main() 