import os
import sys
import time
import threading
import unittest
import tempfile
import shutil
import subprocess
import requests
import json
import logging
from pathlib import Path

# Añadir el directorio src al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.datanode.storage.block_storage import BlockStorage
from src.datanode.registration import DataNodeRegistration

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("CommunicationTest")

class TestNameNodeDataNodeCommunication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Crear directorios temporales para los DataNodes
        cls.datanode1_dir = tempfile.mkdtemp(prefix="datanode1_")
        cls.datanode2_dir = tempfile.mkdtemp(prefix="datanode2_")
        
        # Configurar el NameNode (se asume que está en ejecución en localhost:8000)
        cls.namenode_url = "http://localhost:8000"
        
        # Crear algunos bloques de prueba en los DataNodes
        cls.datanode1_storage = BlockStorage(cls.datanode1_dir)
        cls.datanode2_storage = BlockStorage(cls.datanode2_dir)
        
        # Crear bloques de prueba
        cls.test_blocks = {
            "block1": b"Contenido del bloque 1",
            "block2": b"Contenido del bloque 2 que es un poco mas largo",
            "block3": b"Este es el contenido del bloque 3 con datos de prueba"
        }
        
        for block_id, data in cls.test_blocks.items():
            cls.datanode1_storage.store_block(block_id, data)
            # Solo almacenar algunos bloques en el segundo DataNode
            if block_id != "block3":
                cls.datanode2_storage.store_block(block_id, data)
    
    @classmethod
    def tearDownClass(cls):
        # Limpiar los directorios temporales
        shutil.rmtree(cls.datanode1_dir, ignore_errors=True)
        shutil.rmtree(cls.datanode2_dir, ignore_errors=True)
    
    def test_datanode_registration(self):
        """Probar el registro de un DataNode con el NameNode"""
        # Crear una instancia de DataNodeRegistration
        registration = DataNodeRegistration(
            namenode_url=self.namenode_url,
            node_id="test-datanode-1",
            hostname="localhost",
            port=50051,
            storage_capacity=1000000000,  # 1GB
            registration_interval=5  # 5 segundos
        )
        
        # Registrar el DataNode
        success = registration.register()
        self.assertTrue(success, "El registro del DataNode debería ser exitoso")
        
        # Verificar que el DataNode aparece en la lista de DataNodes
        response = requests.get(f"{self.namenode_url}/datanodes/")
        self.assertEqual(response.status_code, 200, "La solicitud para listar DataNodes debería ser exitosa")
        
        datanodes = response.json()
        self.assertTrue(any(dn["node_id"] == "test-datanode-1" for dn in datanodes),
                       "El DataNode registrado debería aparecer en la lista")
    
    def test_datanode_heartbeat(self):
        """Probar el envío de heartbeats desde un DataNode al NameNode"""
        # Crear una instancia de DataNodeRegistration
        registration = DataNodeRegistration(
            namenode_url=self.namenode_url,
            node_id="test-datanode-2",
            hostname="localhost",
            port=50052,
            storage_capacity=2000000000,  # 2GB
            registration_interval=5  # 5 segundos
        )
        
        # Registrar el DataNode
        success = registration.register()
        self.assertTrue(success, "El registro del DataNode debería ser exitoso")
        
        # Preparar información de bloques para el heartbeat
        blocks_info = {}
        for block_id in ["block1", "block2"]:
            size = self.datanode2_storage.get_block_size(block_id)
            checksum = self.datanode2_storage.calculate_checksum(block_id)
            
            blocks_info[block_id] = {
                "size": size,
                "checksum": checksum
            }
        
        # Enviar heartbeat
        available_space = self.datanode2_storage.get_available_space()
        success = registration.heartbeat(available_space, blocks_info)
        self.assertTrue(success, "El envío del heartbeat debería ser exitoso")
        
        # Verificar que el DataNode sigue activo
        response = requests.get(f"{self.namenode_url}/datanodes/test-datanode-2")
        self.assertEqual(response.status_code, 200, "La solicitud para obtener información del DataNode debería ser exitosa")
        
        datanode_info = response.json()
        self.assertEqual(datanode_info["status"], "active", "El estado del DataNode debería ser 'active'")
        
        # En una implementación completa, también verificaríamos que los bloques se han registrado correctamente
        # Pero esto requeriría endpoints adicionales en el NameNode

def run_tests():
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

if __name__ == "__main__":
    # Verificar que el NameNode esté en ejecución
    try:
        response = requests.get("http://localhost:8000/health")
        if response.status_code != 200:
            logger.error("El NameNode no está respondiendo correctamente. Asegúrate de que esté en ejecución.")
            sys.exit(1)
    except requests.exceptions.ConnectionError:
        logger.error("No se puede conectar al NameNode. Asegúrate de que esté en ejecución en localhost:8000.")
        sys.exit(1)
    
    # Ejecutar las pruebas
    run_tests()
