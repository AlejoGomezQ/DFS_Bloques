import os
import sys
import time
import subprocess
import signal
import logging
import tempfile
import shutil
import requests
from pathlib import Path

# Añadir el directorio src al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.datanode.storage.block_storage import BlockStorage

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("IntegrationTest")

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
    
    return test_blocks

def main():
    # Crear directorios temporales para pruebas
    datanode_dir = os.path.join(os.path.dirname(__file__), "test_datanode_blocks")
    os.makedirs(datanode_dir, exist_ok=True)
    
    try:
        # Crear bloques de prueba
        test_blocks = create_test_blocks(datanode_dir)
        
        # Verificar que el NameNode esté en ejecución
        namenode_url = "http://localhost:8000"
        try:
            response = requests.get(f"{namenode_url}/health")
            if response.status_code != 200:
                logger.error("El NameNode no está respondiendo correctamente. Asegúrate de que esté en ejecución.")
                return
        except requests.exceptions.ConnectionError:
            logger.error("No se puede conectar al NameNode. Asegúrate de que esté en ejecución en localhost:8000.")
            return
        
        logger.info("NameNode está en ejecución y respondiendo correctamente.")
        
        # Iniciar el DataNode en un proceso separado
        datanode_cmd = [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "..", "src", "datanode", "main.py"),
            "--node-id", "test-datanode",
            "--hostname", "localhost",
            "--port", "50051",
            "--storage-dir", datanode_dir,
            "--namenode-url", namenode_url
        ]
        
        logger.info(f"Iniciando DataNode: {' '.join(datanode_cmd)}")
        datanode_process = subprocess.Popen(
            datanode_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Esperar un momento para que el DataNode se inicie y se registre
        logger.info("Esperando a que el DataNode se inicie y se registre...")
        time.sleep(5)
        
        # Verificar que el DataNode se haya registrado correctamente
        try:
            response = requests.get(f"{namenode_url}/datanodes/")
            if response.status_code == 200:
                datanodes = response.json()
                if any(dn["node_id"] == "test-datanode" for dn in datanodes):
                    logger.info("DataNode registrado correctamente con el NameNode.")
                else:
                    logger.warning("DataNode no encontrado en la lista de DataNodes registrados.")
            else:
                logger.error(f"Error al obtener la lista de DataNodes: {response.status_code}")
        except requests.exceptions.ConnectionError:
            logger.error("No se puede conectar al NameNode para verificar el registro del DataNode.")
        
        # Esperar un poco más para que se envíe al menos un heartbeat
        logger.info("Esperando a que se envíe un heartbeat...")
        time.sleep(10)
        
        # Verificar el estado del DataNode después del heartbeat
        try:
            response = requests.get(f"{namenode_url}/datanodes/test-datanode")
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
            logger.error("No se puede conectar al NameNode para verificar el estado del DataNode.")
        
        logger.info("Prueba de comunicación completada.")
        
    finally:
        # Limpiar
        if 'datanode_process' in locals():
            logger.info("Deteniendo el proceso del DataNode...")
            datanode_process.terminate()
            try:
                datanode_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                datanode_process.kill()
        
        # Eliminar directorio de prueba
        logger.info(f"Eliminando directorio de prueba: {datanode_dir}")
        shutil.rmtree(datanode_dir, ignore_errors=True)

if __name__ == "__main__":
    main()
