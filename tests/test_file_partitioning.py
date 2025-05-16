import os
import sys
import tempfile
import unittest
import shutil
import random
import string

# Agregar el directorio raíz al path para poder importar los módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.client.file_splitter import FileSplitter


class TestFileSplitter(unittest.TestCase):
    """Pruebas para el módulo de particionamiento de archivos"""
    
    def setUp(self):
        """Configuración inicial para las pruebas"""
        # Crear un directorio temporal para las pruebas
        self.test_dir = tempfile.mkdtemp()
        
        # Crear un archivo de prueba con contenido aleatorio
        self.test_file_path = os.path.join(self.test_dir, "test_file.txt")
        self._create_test_file(self.test_file_path, 10 * 1024 * 1024)  # 10MB
        
        # Inicializar el particionador con un tamaño de bloque pequeño para las pruebas
        self.block_size = 1 * 1024 * 1024  # 1MB
        self.file_splitter = FileSplitter(self.block_size)
    
    def tearDown(self):
        """Limpieza después de las pruebas"""
        # Eliminar el directorio temporal
        shutil.rmtree(self.test_dir)
    
    def _create_test_file(self, file_path, size):
        """Crea un archivo de prueba con contenido aleatorio"""
        with open(file_path, 'wb') as f:
            # Generar contenido aleatorio en bloques de 1MB para no consumir mucha memoria
            chunk_size = 1024 * 1024  # 1MB
            for _ in range(0, size, chunk_size):
                # Ajustar el tamaño del último bloque si es necesario
                current_chunk_size = min(chunk_size, size - _ if _ < size else 0)
                f.write(os.urandom(current_chunk_size))
    
    def test_split_file(self):
        """Prueba la división de un archivo en bloques"""
        # Dividir el archivo en bloques
        blocks = self.file_splitter.split_file(self.test_file_path)
        
        # Verificar que se crearon los bloques correctamente
        file_size = os.path.getsize(self.test_file_path)
        expected_blocks = (file_size + self.block_size - 1) // self.block_size
        
        self.assertEqual(len(blocks), expected_blocks, 
                         f"Se esperaban {expected_blocks} bloques, pero se obtuvieron {len(blocks)}")
        
        # Verificar que todos los bloques tienen un ID único
        block_ids = [block['block_id'] for block in blocks]
        self.assertEqual(len(block_ids), len(set(block_ids)), 
                         "Los IDs de los bloques no son únicos")
        
        # Verificar que el tamaño total de los bloques es igual al tamaño del archivo
        total_size = sum(block['size'] for block in blocks)
        self.assertEqual(total_size, file_size, 
                         f"El tamaño total de los bloques ({total_size}) no coincide con el tamaño del archivo ({file_size})")
        
        # Verificar que todos los bloques excepto posiblemente el último tienen el tamaño correcto
        for i, block in enumerate(blocks[:-1]):
            self.assertEqual(block['size'], self.block_size, 
                             f"El bloque {i} tiene un tamaño incorrecto: {block['size']} (esperado: {self.block_size})")
        
        # Verificar que los índices son correctos
        for i, block in enumerate(blocks):
            self.assertEqual(block['index'], i, 
                             f"El índice del bloque {i} es incorrecto: {block['index']}")
    
    def test_join_blocks(self):
        """Prueba la unión de bloques para reconstruir el archivo original"""
        # Dividir el archivo en bloques
        blocks = self.file_splitter.split_file(self.test_file_path)
        
        # Reconstruir el archivo
        output_path = os.path.join(self.test_dir, "reconstructed_file.txt")
        result = self.file_splitter.join_blocks(blocks, output_path)
        
        # Verificar que la operación fue exitosa
        self.assertTrue(result, "La unión de bloques falló")
        
        # Verificar que el archivo reconstruido existe
        self.assertTrue(os.path.exists(output_path), 
                        f"El archivo reconstruido no existe: {output_path}")
        
        # Verificar que el tamaño del archivo reconstruido es igual al original
        original_size = os.path.getsize(self.test_file_path)
        reconstructed_size = os.path.getsize(output_path)
        self.assertEqual(reconstructed_size, original_size, 
                         f"El tamaño del archivo reconstruido ({reconstructed_size}) no coincide con el original ({original_size})")
        
        # Verificar que el contenido del archivo reconstruido es igual al original
        with open(self.test_file_path, 'rb') as f1, open(output_path, 'rb') as f2:
            # Comparar por bloques para no cargar todo el archivo en memoria
            while True:
                chunk1 = f1.read(8192)
                chunk2 = f2.read(8192)
                
                if not chunk1 and not chunk2:
                    break
                
                self.assertEqual(chunk1, chunk2, 
                                 "El contenido del archivo reconstruido no coincide con el original")
    
    def test_small_file(self):
        """Prueba con un archivo más pequeño que el tamaño de bloque"""
        # Crear un archivo pequeño
        small_file_path = os.path.join(self.test_dir, "small_file.txt")
        self._create_test_file(small_file_path, 100 * 1024)  # 100KB
        
        # Dividir el archivo en bloques
        blocks = self.file_splitter.split_file(small_file_path)
        
        # Verificar que solo se creó un bloque
        self.assertEqual(len(blocks), 1, 
                         f"Se esperaba 1 bloque, pero se obtuvieron {len(blocks)}")
        
        # Verificar que el tamaño del bloque es igual al tamaño del archivo
        file_size = os.path.getsize(small_file_path)
        self.assertEqual(blocks[0]['size'], file_size, 
                         f"El tamaño del bloque ({blocks[0]['size']}) no coincide con el tamaño del archivo ({file_size})")


if __name__ == '__main__':
    unittest.main()
