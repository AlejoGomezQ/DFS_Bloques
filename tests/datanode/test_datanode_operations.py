import unittest
import os
import shutil
import uuid
import tempfile
import sys
from pathlib import Path

# Agregar el directorio raiz del proyecto al path para poder importar los modulos
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.datanode.storage.block_storage import BlockStorage


class TestDataNodeOperations(unittest.TestCase):
    def setUp(self):
        # Crear un directorio temporal para las pruebas
        self.test_dir = tempfile.mkdtemp()
        self.storage = BlockStorage(self.test_dir)
        
    def tearDown(self):
        # Eliminar el directorio temporal después de las pruebas
        shutil.rmtree(self.test_dir)
    
    def test_store_block(self):
        """Prueba el almacenamiento de un bloque"""
        block_id = str(uuid.uuid4())
        test_data = b"Este es un bloque de prueba"
        
        # Almacenar el bloque
        result = self.storage.store_block(block_id, test_data)
        
        # Verificar que se almacenó correctamente
        self.assertTrue(result)
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, block_id)))
        
        # Verificar el contenido del bloque
        with open(os.path.join(self.test_dir, block_id), 'rb') as f:
            stored_data = f.read()
        
        self.assertEqual(stored_data, test_data)
    
    def test_retrieve_block(self):
        """Prueba la recuperación de un bloque"""
        block_id = str(uuid.uuid4())
        test_data = b"Este es un bloque para recuperar"
        
        # Almacenar el bloque primero
        self.storage.store_block(block_id, test_data)
        
        # Recuperar el bloque
        retrieved_data = self.storage.retrieve_block(block_id)
        
        # Verificar que los datos recuperados son correctos
        self.assertEqual(retrieved_data, test_data)
        
        # Intentar recuperar un bloque que no existe
        non_existent_block = str(uuid.uuid4())
        self.assertIsNone(self.storage.retrieve_block(non_existent_block))
    
    def test_delete_block(self):
        """Prueba la eliminación de un bloque"""
        block_id = str(uuid.uuid4())
        test_data = b"Este es un bloque para eliminar"
        
        # Almacenar el bloque primero
        self.storage.store_block(block_id, test_data)
        
        # Verificar que el bloque existe
        self.assertTrue(self.storage.block_exists(block_id))
        
        # Eliminar el bloque
        result = self.storage.delete_block(block_id)
        
        # Verificar que se eliminó correctamente
        self.assertTrue(result)
        self.assertFalse(self.storage.block_exists(block_id))
        
        # Intentar eliminar un bloque que no existe
        non_existent_block = str(uuid.uuid4())
        self.assertFalse(self.storage.delete_block(non_existent_block))
    
    def test_block_exists(self):
        """Prueba la verificación de existencia de un bloque"""
        block_id = str(uuid.uuid4())
        test_data = b"Este es un bloque para verificar"
        
        # Inicialmente el bloque no debe existir
        self.assertFalse(self.storage.block_exists(block_id))
        
        # Almacenar el bloque
        self.storage.store_block(block_id, test_data)
        
        # Ahora el bloque debe existir
        self.assertTrue(self.storage.block_exists(block_id))
    
    def test_get_block_size(self):
        """Prueba la obtención del tamaño de un bloque"""
        block_id = str(uuid.uuid4())
        test_data = b"Este es un bloque para medir su tamano"
        
        # Almacenar el bloque
        self.storage.store_block(block_id, test_data)
        
        # Verificar el tamaño del bloque
        size = self.storage.get_block_size(block_id)
        self.assertEqual(size, len(test_data))
        
        # Intentar obtener el tamaño de un bloque que no existe
        non_existent_block = str(uuid.uuid4())
        self.assertIsNone(self.storage.get_block_size(non_existent_block))
    
    def test_calculate_checksum(self):
        """Prueba el cálculo del checksum de un bloque"""
        block_id = str(uuid.uuid4())
        test_data = b"Este es un bloque para calcular su checksum"
        
        # Almacenar el bloque
        self.storage.store_block(block_id, test_data)
        
        # Calcular el checksum manualmente
        import hashlib
        expected_checksum = hashlib.sha256(test_data).hexdigest()
        
        # Verificar el checksum calculado por el sistema
        checksum = self.storage.calculate_checksum(block_id)
        self.assertEqual(checksum, expected_checksum)
        
        # Intentar calcular el checksum de un bloque que no existe
        non_existent_block = str(uuid.uuid4())
        self.assertIsNone(self.storage.calculate_checksum(non_existent_block))
    
    def test_get_all_blocks(self):
        """Prueba la obtención de todos los bloques almacenados"""
        # Inicialmente no debe haber bloques
        self.assertEqual(len(self.storage.get_all_blocks()), 0)
        
        # Almacenar varios bloques
        block_ids = [str(uuid.uuid4()) for _ in range(5)]
        for block_id in block_ids:
            self.storage.store_block(block_id, f"Bloque {block_id}".encode())
        
        # Verificar que se obtienen todos los bloques
        stored_blocks = self.storage.get_all_blocks()
        self.assertEqual(len(stored_blocks), 5)
        
        # Verificar que todos los bloques almacenados están en la lista
        for block_id in block_ids:
            self.assertIn(block_id, stored_blocks)
    
    def test_stream_block(self):
        """Prueba la transmisión de un bloque en chunks"""
        block_id = str(uuid.uuid4())
        # Crear un bloque grande para probar la transmisión en chunks
        test_data = b"x" * 10000  # 10KB de datos
        
        # Almacenar el bloque
        self.storage.store_block(block_id, test_data)
        
        # Transmitir el bloque en chunks de 1KB
        chunks = self.storage.stream_block(block_id, chunk_size=1000)
        
        # Verificar que se obtienen los chunks correctos
        self.assertIsNotNone(chunks)
        
        # Reconstruir los datos a partir de los chunks
        reconstructed_data = b""
        total_size = None
        for chunk, offset, size in chunks:
            reconstructed_data += chunk
            if total_size is None:
                total_size = size
        
        # Verificar que los datos reconstruidos son correctos
        self.assertEqual(len(reconstructed_data), len(test_data))
        self.assertEqual(reconstructed_data, test_data)
        self.assertEqual(total_size, len(test_data))
        
        # Intentar transmitir un bloque que no existe
        non_existent_block = str(uuid.uuid4())
        self.assertIsNone(self.storage.stream_block(non_existent_block))


if __name__ == "__main__":
    unittest.main()
