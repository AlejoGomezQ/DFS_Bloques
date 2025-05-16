import unittest
import os
import sys
import uuid
import time
import threading
import tempfile
import shutil
import requests
from pathlib import Path

# Agregar el directorio raíz del proyecto al path para poder importar los módulos
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.namenode.api.app import start_namenode
from src.datanode.service.datanode_service import serve as start_datanode
from src.client.namenode_client import NameNodeClient
from src.client.datanode_client import DataNodeClient


class TestDataNodeNameNodeIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Crear directorios temporales para las pruebas
        cls.namenode_dir = tempfile.mkdtemp()
        cls.datanode_dir = tempfile.mkdtemp()
        
        # Configurar los puertos
        cls.namenode_port = 8000
        cls.datanode_port = 50051
        
        # Configurar los clientes
        cls.namenode_client = NameNodeClient("http://localhost:8000")
        cls.datanode_client = DataNodeClient("localhost", 50051)
        
        # Iniciar el NameNode en un hilo separado
        cls.namenode_thread = threading.Thread(
            target=start_namenode,
            args=(cls.namenode_dir, cls.namenode_port),
            daemon=True
        )
        cls.namenode_thread.start()
        
        # Iniciar el DataNode en un hilo separado
        cls.datanode_id = str(uuid.uuid4())
        cls.datanode_thread = threading.Thread(
            target=start_datanode,
            args=(cls.datanode_id, "localhost", cls.datanode_port, cls.datanode_dir, f"http://localhost:{cls.namenode_port}"),
            daemon=True
        )
        cls.datanode_thread.start()
        
        # Esperar a que los servicios estén disponibles
        cls._wait_for_services()
        
        # Registrar el DataNode en el NameNode
        cls._register_datanode()
    
    @classmethod
    def tearDownClass(cls):
        # Eliminar los directorios temporales
        shutil.rmtree(cls.namenode_dir, ignore_errors=True)
        shutil.rmtree(cls.datanode_dir, ignore_errors=True)
    
    @classmethod
    def _wait_for_services(cls):
        """Espera a que los servicios estén disponibles"""
        # Esperar a que el NameNode esté disponible
        max_retries = 10
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(f"http://localhost:{cls.namenode_port}/health")
                if response.status_code == 200:
                    break
            except requests.exceptions.ConnectionError:
                pass
            
            retries += 1
            time.sleep(1)
        
        if retries == max_retries:
            raise Exception("No se pudo conectar al NameNode")
        
        # Esperar un poco más para que el DataNode esté disponible
        time.sleep(2)
    
    @classmethod
    def _register_datanode(cls):
        """Registra el DataNode en el NameNode"""
        response = requests.post(
            f"http://localhost:{cls.namenode_port}/datanodes/",
            json={
                "datanode_id": cls.datanode_id,
                "hostname": "localhost",
                "port": cls.datanode_port,
                "storage_capacity": 1024 * 1024 * 1024  # 1GB
            }
        )
        assert response.status_code == 201, f"Error al registrar el DataNode: {response.text}"
    
    def test_store_and_retrieve_block(self):
        """Prueba el almacenamiento y recuperación de un bloque a través del sistema completo"""
        # Crear un archivo en el NameNode
        file_path = "/test/integration_test.txt"
        file_response = self.namenode_client.create_file(
            name="integration_test.txt",
            path=file_path,
            file_type="text/plain",
            size=1024
        )
        
        self.assertIsNotNone(file_response)
        file_id = file_response.get("file_id")
        self.assertIsNotNone(file_id)
        
        # Crear un bloque para el archivo
        block_id = str(uuid.uuid4())
        block_data = b"Este es un bloque de prueba para la integracion entre DataNode y NameNode"
        block_size = len(block_data)
        
        # Registrar el bloque en el NameNode
        block_response = self.namenode_client.create_block(
            file_id=file_id,
            block_id=block_id,
            block_size=block_size,
            sequence_number=0
        )
        
        self.assertIsNotNone(block_response)
        self.assertEqual(block_response.get("block_id"), block_id)
        
        # Almacenar el bloque en el DataNode
        store_success = self.datanode_client.connect().store_block(block_id, block_data)
        self.assertTrue(store_success)
        
        # Registrar la ubicación del bloque en el NameNode
        location_response = self.namenode_client.add_block_location(
            block_id=block_id,
            datanode_id=self.datanode_id
        )
        
        self.assertIsNotNone(location_response)
        
        # Verificar que el bloque existe en el DataNode
        exists, size, checksum = self.datanode_client.check_block(block_id)
        self.assertTrue(exists)
        self.assertEqual(size, block_size)
        self.assertIsNotNone(checksum)
        
        # Recuperar el bloque del DataNode
        retrieved_data = self.datanode_client.retrieve_block(block_id)
        self.assertIsNotNone(retrieved_data)
        self.assertEqual(retrieved_data, block_data)
        
        # Obtener la información del bloque desde el NameNode
        blocks = self.namenode_client.get_blocks_by_file(file_id)
        self.assertIsNotNone(blocks)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].get("block_id"), block_id)
        
        # Eliminar el bloque
        delete_success = self.datanode_client.delete_block(block_id)
        self.assertTrue(delete_success)
        
        # Verificar que el bloque ya no existe en el DataNode
        exists, _, _ = self.datanode_client.check_block(block_id)
        self.assertFalse(exists)
        
        # Eliminar el archivo del NameNode
        delete_file_response = self.namenode_client.delete_file(file_id)
        self.assertTrue(delete_file_response)


if __name__ == "__main__":
    unittest.main()
