import os
import sys
import uuid

# Añadir el directorio src al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.datanode.service.datanode_service import (
    MockBlockRequest, 
    MockBlockData, 
    MockContext,
    DataNodeServicer
)

def simulate_client_operations():
    # Crear un directorio temporal para las pruebas
    test_dir = "./data/test-client-blocks"
    os.makedirs(test_dir, exist_ok=True)
    
    # Crear una instancia del DataNodeServicer
    servicer = DataNodeServicer(
        storage_dir=test_dir,
        node_id="test-client-datanode",
        hostname="localhost",
        port=50052
    )
    
    # Generar un ID de bloque para las pruebas
    block_id = str(uuid.uuid4())
    test_data = b"Este es un bloque de prueba para el cliente simulado " * 5
    
    print(f"\n=== Simulación de operaciones de cliente con DataNode ===")
    print(f"Usando directorio de almacenamiento: {test_dir}")
    print(f"ID de bloque para pruebas: {block_id}")
    
    # 1. Almacenar un bloque
    print("\n1. Almacenando un bloque...")
    # Crear un iterador de chunks para simular streaming
    chunk_size = 50
    chunks = []
    for i in range(0, len(test_data), chunk_size):
        chunk = test_data[i:i+chunk_size]
        chunks.append(MockBlockData(
            block_id=block_id,
            data=chunk,
            offset=i,
            total_size=len(test_data)
        ))
    
    # Llamar al método StoreBlock
    response = servicer.StoreBlock(chunks, MockContext())
    print(f"Respuesta: status={response.status}, mensaje='{response.message}'")
    
    # 2. Verificar si el bloque existe
    print("\n2. Verificando si el bloque existe...")
    status_response = servicer.CheckBlock(MockBlockRequest(block_id), MockContext())
    print(f"Existe: {status_response.exists}")
    print(f"Tamaño: {status_response.size} bytes")
    print(f"Checksum: {status_response.checksum}")
    
    # 3. Recuperar el bloque
    print("\n3. Recuperando el bloque...")
    retrieved_chunks = list(servicer.RetrieveBlock(MockBlockRequest(block_id), MockContext()))
    retrieved_data = b""
    for chunk in retrieved_chunks:
        retrieved_data += chunk.data
    
    data_match = retrieved_data == test_data
    print(f"Datos recuperados correctamente: {data_match}")
    print(f"Tamaño de datos recuperados: {len(retrieved_data)} bytes")
    
    # 4. Eliminar el bloque
    print("\n4. Eliminando el bloque...")
    delete_response = servicer.DeleteBlock(MockBlockRequest(block_id), MockContext())
    print(f"Respuesta: status={delete_response.status}, mensaje='{delete_response.message}'")
    
    # Verificar que el bloque ya no existe
    status_after_delete = servicer.CheckBlock(MockBlockRequest(block_id), MockContext())
    print(f"¿El bloque sigue existiendo? {status_after_delete.exists}")
    
    print("\n=== Prueba completada ===")

if __name__ == "__main__":
    simulate_client_operations()
