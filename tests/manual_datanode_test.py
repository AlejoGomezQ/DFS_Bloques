import os
import sys
import uuid
import shutil

# Añadir el directorio src al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.datanode.storage.block_storage import BlockStorage

# Crear un directorio temporal para las pruebas
test_dir = os.path.join(os.path.dirname(__file__), 'test_blocks')
os.makedirs(test_dir, exist_ok=True)

try:
    # Inicializar el almacenamiento de bloques
    storage = BlockStorage(test_dir)
    
    # 1. Almacenar un bloque
    block_id = str(uuid.uuid4())
    test_data = b"Este es un bloque de prueba con datos aleatorios " * 10
    print(f"Almacenando bloque con ID: {block_id}")
    result = storage.store_block(block_id, test_data)
    print(f"Resultado del almacenamiento: {'Éxito' if result else 'Fallo'}")
    
    # 2. Verificar que el bloque existe
    exists = storage.block_exists(block_id)
    print(f"¿El bloque existe? {exists}")
    
    # 3. Obtener el tamaño del bloque
    size = storage.get_block_size(block_id)
    print(f"Tamaño del bloque: {size} bytes")
    
    # 4. Calcular el checksum del bloque
    checksum = storage.calculate_checksum(block_id)
    print(f"Checksum del bloque: {checksum}")
    
    # 5. Recuperar el bloque
    retrieved_data = storage.retrieve_block(block_id)
    data_match = retrieved_data == test_data
    print(f"Datos recuperados correctamente: {data_match}")
    
    # 6. Obtener estadísticas de almacenamiento
    stats = storage.get_storage_stats()
    print(f"Estadísticas de almacenamiento:")
    print(f"  - Total de bloques: {stats['total_blocks']}")
    print(f"  - Tamaño total: {stats['total_size']} bytes")
    print(f"  - Bloques: {stats['blocks']}")
    
    # 7. Espacio disponible
    available = storage.get_available_space()
    print(f"Espacio disponible: {available / (1024*1024):.2f} MB")
    
    # 8. Eliminar el bloque
    delete_result = storage.delete_block(block_id)
    print(f"Bloque eliminado: {'Sí' if delete_result else 'No'}")
    exists_after_delete = storage.block_exists(block_id)
    print(f"¿El bloque sigue existiendo? {exists_after_delete}")
    
finally:
    # Limpiar el directorio de prueba
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
        print(f"Directorio de prueba eliminado: {test_dir}")
