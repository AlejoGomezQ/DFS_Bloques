import os
import sys
import unittest
import tempfile
import shutil
import uuid
import json
import requests
from datetime import datetime

# Agregar el directorio raíz al path para poder importar los módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.namenode.api.models import FileType, BlockLocation
from src.client.namenode_client import NameNodeClient


class TestNameNodeOperations(unittest.TestCase):
    """Pruebas para las operaciones básicas del NameNode"""
    
    @classmethod
    def setUpClass(cls):
        """Configuración inicial para las pruebas"""
        # URL del NameNode (asumiendo que está en ejecución)
        cls.namenode_url = "http://localhost:8000"
        
        # Cliente para comunicarse con el NameNode
        cls.client = NameNodeClient(cls.namenode_url)
        
        # Verificar que el NameNode está en ejecución
        try:
            response = requests.get(f"{cls.namenode_url}/directories/")
            if response.status_code >= 400:
                raise Exception(f"Error al conectar con el NameNode: {response.status_code}")
        except Exception as e:
            print(f"ERROR: El NameNode no está en ejecución o no es accesible: {e}")
            print("Asegúrate de que el NameNode esté en ejecución antes de ejecutar estas pruebas.")
            sys.exit(1)
    
    def setUp(self):
        """Preparación antes de cada prueba"""
        # Asegurarnos de que el directorio raíz existe
        try:
            self.client._make_request('get', '/directories/')
        except Exception:
            # Si no existe, intentar crearlo
            try:
                self.client._make_request('post', '/directories/', {
                    'name': '',
                    'path': '',
                    'type': 'directory'
                })
            except Exception as e:
                print(f"No se pudo crear el directorio raíz: {e}")
        
        # Crear un directorio temporal para las pruebas
        self.test_dir = f"test_{uuid.uuid4()}"
        
        # Crear el directorio en el NameNode
        self.client._make_request('post', '/directories/', {
            'name': self.test_dir,
            'path': self.test_dir,
            'type': 'directory'
        })
    
    def tearDown(self):
        """Limpieza después de cada prueba"""
        # Eliminar el directorio de prueba
        try:
            self.client._make_request('delete', f'/directories/{self.test_dir}')
        except Exception:
            pass
    
    def test_file_operations(self):
        """Prueba las operaciones básicas de archivos"""
        # Crear un archivo
        file_path = f"{self.test_dir}/test_file.txt"
        file_data = {
            'name': 'test_file.txt',
            'path': file_path,
            'type': 'file',
            'size': 1024,
            'blocks': []
        }
        
        created_file = self.client._make_request('post', '/files/', file_data)
        file_id = created_file['file_id']
        
        # Verificar que el archivo se creó correctamente
        self.assertIsNotNone(file_id)
        self.assertEqual(created_file['name'], 'test_file.txt')
        self.assertEqual(created_file['path'], file_path)
        
        # Obtener el archivo por ID
        retrieved_file = self.client._make_request('get', f'/files/{file_id}')
        self.assertEqual(retrieved_file['file_id'], file_id)
        
        # Obtener el archivo por ruta
        retrieved_file_by_path = self.client._make_request('get', f'/files/path/{file_path}')
        self.assertEqual(retrieved_file_by_path['file_id'], file_id)
        
        # Eliminar el archivo
        self.client._make_request('delete', f'/files/{file_id}')
        
        # Verificar que el archivo ya no existe
        try:
            self.client._make_request('get', f'/files/{file_id}')
            self.fail("Se esperaba una excepción al intentar obtener un archivo eliminado")
        except Exception:
            pass
    
    def test_block_operations(self):
        """Prueba las operaciones básicas de bloques"""
        # Crear un archivo
        file_path = f"{self.test_dir}/test_file_with_blocks.txt"
        file_data = {
            'name': 'test_file_with_blocks.txt',
            'path': file_path,
            'type': 'file',
            'size': 1024,
            'blocks': []
        }
        
        created_file = self.client._make_request('post', '/files/', file_data)
        file_id = created_file['file_id']
        
        # Registrar un DataNode para las pruebas
        datanode_data = {
            'hostname': 'localhost',
            'port': 50051,
            'storage_capacity': 1024 * 1024 * 1024,  # 1GB
            'available_space': 1024 * 1024 * 1024    # 1GB
        }
        
        datanode = self.client._make_request('post', '/datanodes/register', datanode_data)
        datanode_id = datanode['node_id']
        
        # Crear un bloque
        block_data = {
            'block_id': str(uuid.uuid4()),
            'file_id': file_id,
            'size': 512,
            'checksum': 'abc123',
            'locations': [
                {
                    'block_id': str(uuid.uuid4()),
                    'datanode_id': datanode_id,
                    'is_leader': True
                }
            ]
        }
        
        created_block = self.client._make_request('post', '/blocks/', block_data)
        block_id = created_block['block_id']
        
        # Verificar que el bloque se creó correctamente
        self.assertIsNotNone(block_id)
        self.assertEqual(created_block['file_id'], file_id)
        self.assertEqual(created_block['size'], 512)
        
        # Obtener información del bloque
        retrieved_block = self.client._make_request('get', f'/blocks/{block_id}')
        self.assertEqual(retrieved_block['block_id'], block_id)
        
        # Actualizar información del bloque
        updated_block_data = {
            'block_id': block_id,
            'file_id': file_id,
            'size': 1024,
            'checksum': 'updated_checksum',
            'locations': retrieved_block['locations']
        }
        
        updated_block = self.client._make_request('put', f'/blocks/{block_id}', updated_block_data)
        self.assertEqual(updated_block['size'], 1024)
        self.assertEqual(updated_block['checksum'], 'updated_checksum')
        
        # Obtener bloques del archivo
        file_blocks = self.client._make_request('get', f'/blocks/file/{file_id}')
        self.assertEqual(len(file_blocks), 1)
        self.assertEqual(file_blocks[0]['block_id'], block_id)
        
        # Añadir una nueva ubicación para el bloque
        new_datanode_data = {
            'hostname': 'localhost',
            'port': 50052,
            'storage_capacity': 1024 * 1024 * 1024,  # 1GB
            'available_space': 1024 * 1024 * 1024    # 1GB
        }
        
        new_datanode = self.client._make_request('post', '/datanodes/register', new_datanode_data)
        new_datanode_id = new_datanode['node_id']
        
        location_data = {
            'block_id': block_id,
            'datanode_id': new_datanode_id,
            'is_leader': False
        }
        
        self.client._make_request('post', f'/blocks/{block_id}/locations', location_data)
        
        # Verificar que se añadió la nueva ubicación
        updated_block = self.client._make_request('get', f'/blocks/{block_id}')
        self.assertEqual(len(updated_block['locations']), 2)
        
        # Eliminar una ubicación
        self.client._make_request('delete', f'/blocks/{block_id}/locations/{new_datanode_id}')
        
        # Verificar que se eliminó la ubicación
        updated_block = self.client._make_request('get', f'/blocks/{block_id}')
        self.assertEqual(len(updated_block['locations']), 1)
        
        # Limpiar
        self.client._make_request('delete', f'/files/{file_id}')


if __name__ == '__main__':
    unittest.main()
