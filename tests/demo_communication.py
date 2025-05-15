import os
import sys
import time
import logging
import tempfile
import shutil
import requests
import json
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
logger = logging.getLogger("DemoNameNodeDataNode")

def create_test_blocks(storage_dir):
    """Crear algunos bloques de prueba en el directorio de almacenamiento"""
    storage = BlockStorage(storage_dir)
    
    # Crear bloques de prueba
    test_blocks = {
        "test-block-1": b"Contenido del bloque de prueba 1",
        "test-block-2": b"Contenido del bloque de prueba 2 con mas datos",
        "test-block-3": b"Este es el contenido del bloque de prueba 3 con aun mas datos para probar"
    }
    
    for block_id, data in test_blocks.items():
        logger.info(f"Creando bloque de prueba: {block_id}")
        storage.store_block(block_id, data)
    
    # Mostrar información de los bloques
    stats = storage.get_storage_stats()
    logger.info(f"Bloques creados: {len(stats['blocks'])}")
    logger.info(f"Tamaño total: {stats['total_size']} bytes")
    
    return stats

def simulate_datanode_heartbeat(namenode_url, node_id, hostname, port, storage_stats):
    """Simular el registro y heartbeat de un DataNode"""
    # Crear una instancia de DataNodeRegistration
    registration = DataNodeRegistration(
        namenode_url=namenode_url,
        node_id=node_id,
        hostname=hostname,
        port=port,
        storage_capacity=1000000000,  # 1GB
        registration_interval=10  # 10 segundos
    )
    
    # Registrar el DataNode
    logger.info(f"Registrando DataNode {node_id} con el NameNode...")
    success = registration.register()
    
    if success:
        logger.info(f"DataNode {node_id} registrado exitosamente")
        
        # Preparar información de bloques para el heartbeat
        blocks_info = {}
        for block_id in storage_stats["blocks"]:
            block_size = storage_stats.get("block_sizes", {}).get(block_id, 0)
            block_checksum = storage_stats.get("block_checksums", {}).get(block_id, "")
            
            blocks_info[block_id] = {
                "size": block_size,
                "checksum": block_checksum
            }
        
        # Enviar heartbeat
        logger.info(f"Enviando heartbeat con información de {len(blocks_info)} bloques...")
        heartbeat_success = registration.heartbeat(storage_stats["available_space"], blocks_info)
        
        if heartbeat_success:
            logger.info("Heartbeat enviado exitosamente")
            
            # Verificar que el DataNode sigue activo
            try:
                response = requests.get(f"{namenode_url}/datanodes/{node_id}")
                if response.status_code == 200:
                    datanode_info = response.json()
                    logger.info(f"Información del DataNode después del heartbeat:")
                    logger.info(f"  - ID: {datanode_info['node_id']}")
                    logger.info(f"  - Estado: {datanode_info['status']}")
                    logger.info(f"  - Bloques almacenados: {datanode_info['blocks_stored']}")
                    logger.info(f"  - Espacio disponible: {datanode_info['available_space']} bytes")
                    logger.info(f"  - Último heartbeat: {datanode_info['last_heartbeat']}")
                else:
                    logger.error(f"Error al obtener información del DataNode: {response.status_code}")
            except requests.exceptions.ConnectionError:
                logger.error("No se puede conectar al NameNode para verificar el estado del DataNode")
        else:
            logger.error("Error al enviar heartbeat")
    else:
        logger.error("Error al registrar el DataNode")

def main():
    # Verificar que el NameNode esté en ejecución
    namenode_url = "http://localhost:8001"
    try:
        response = requests.get(f"{namenode_url}/health")
        if response.status_code != 200:
            logger.error("El NameNode no está respondiendo correctamente. Asegúrate de que esté en ejecución.")
            logger.info("Asegúrate de que el NameNode esté en ejecución con el comando:")
            logger.info("python -m src.namenode.api.main")
            return
    except requests.exceptions.ConnectionError:
        logger.error("No se puede conectar al NameNode. Asegúrate de que esté en ejecución en localhost:8000.")
        logger.info("Asegúrate de que el NameNode esté en ejecución con el comando:")
        logger.info("python -m src.namenode.api.main")
        return
    
    logger.info("NameNode está en ejecución y respondiendo correctamente.")
    
    # Crear directorio temporal para pruebas
    datanode_dir = os.path.join(os.path.dirname(__file__), "demo_datanode_blocks")
    os.makedirs(datanode_dir, exist_ok=True)
    
    try:
        # Crear bloques de prueba
        storage_stats = create_test_blocks(datanode_dir)
        
        # Simular registro y heartbeat de un DataNode
        simulate_datanode_heartbeat(
            namenode_url=namenode_url,
            node_id="demo-datanode",
            hostname="localhost",
            port=50051,
            storage_stats=storage_stats
        )
        
        logger.info("Demostración de comunicación completada.")
        
    finally:
        # Eliminar directorio de prueba
        logger.info(f"Eliminando directorio de prueba: {datanode_dir}")
        shutil.rmtree(datanode_dir, ignore_errors=True)

if __name__ == "__main__":
    main()
