import unittest
import time
import os
import shutil
import threading
import requests
from datetime import datetime, timedelta

from src.namenode.metadata.manager import MetadataManager
from src.namenode.monitoring.datanode_monitor import DataNodeMonitor
from src.namenode.api.models import DataNodeStatus
from src.datanode.registration import DataNodeRegistration

class TestDataNodeFailure(unittest.TestCase):
    def setUp(self):
        # Crear directorio temporal para la base de datos
        self.test_dir = "test_data"
        os.makedirs(self.test_dir, exist_ok=True)
        
        # Inicializar el gestor de metadatos
        self.metadata_manager = MetadataManager(db_path=os.path.join(self.test_dir, "test.db"))
        
        # Inicializar el monitor con un timeout corto para pruebas
        self.monitor = DataNodeMonitor(self.metadata_manager, heartbeat_timeout=5)
        
        # Registrar un DataNode de prueba
        self.datanode = self.metadata_manager.register_datanode(
            hostname="localhost",
            port=50051,
            storage_capacity=1000000000,  # 1GB
            available_space=1000000000
        )
        
        # Iniciar el monitor
        self.monitor.start()
    
    def tearDown(self):
        # Detener el monitor
        self.monitor.stop()
        
        # Limpiar directorio de prueba
        shutil.rmtree(self.test_dir)
    
    def test_heartbeat_timeout(self):
        """Test de detección de timeout en heartbeat"""
        # Verificar que el DataNode está activo inicialmente
        datanode = self.metadata_manager.get_datanode(self.datanode.node_id)
        self.assertEqual(datanode.status, DataNodeStatus.ACTIVE)
        
        # Simular un heartbeat antiguo
        old_time = datetime.now() - timedelta(seconds=10)
        self.metadata_manager.update_datanode_heartbeat(self.datanode.node_id, 1000000000)
        
        # Esperar a que el monitor detecte el timeout
        time.sleep(6)
        
        # Verificar que el DataNode fue marcado como inactivo
        datanode = self.metadata_manager.get_datanode(self.datanode.node_id)
        self.assertEqual(datanode.status, DataNodeStatus.INACTIVE)
    
    def test_health_check(self):
        """Test de verificación de salud del DataNode"""
        # Verificar salud inicial
        health = self.monitor.get_datanode_health(self.datanode.node_id)
        self.assertTrue(health["is_healthy"])
        
        # Simular un heartbeat antiguo
        old_time = datetime.now() - timedelta(seconds=10)
        self.metadata_manager.update_datanode_heartbeat(self.datanode.node_id, 1000000000)
        
        # Verificar que el estado de salud cambió
        health = self.monitor.get_datanode_health(self.datanode.node_id)
        self.assertFalse(health["is_healthy"])
    
    def test_failure_handling(self):
        """Test de manejo de fallos de DataNode"""
        # Registrar algunos bloques en el DataNode
        block_id = "test_block_1"
        self.metadata_manager.add_block_location(block_id, self.datanode.node_id, True)
        
        # Simular un fallo
        self.monitor._handle_datanode_failure(self.datanode.node_id)
        
        # Verificar que el DataNode fue marcado como inactivo
        datanode = self.metadata_manager.get_datanode(self.datanode.node_id)
        self.assertEqual(datanode.status, DataNodeStatus.INACTIVE)
        
        # Verificar que los bloques fueron identificados para re-replicación
        blocks = self.metadata_manager.get_blocks_by_datanode(self.datanode.node_id)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].block_id, block_id)

if __name__ == '__main__':
    unittest.main() 