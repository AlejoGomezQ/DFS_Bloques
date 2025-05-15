import os
import argparse
import logging
from service.datanode_service import serve

def main():
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='DataNode Service')
    parser.add_argument('--node-id', type=str, default="datanode1", help='Unique ID for this DataNode')
    parser.add_argument('--hostname', type=str, default="localhost", help='Hostname to bind')
    parser.add_argument('--port', type=int, default=50051, help='Port to bind')
    parser.add_argument('--storage-dir', type=str, default="./data/blocks", help='Directory to store blocks')
    parser.add_argument('--namenode-url', type=str, default="http://localhost:8000", help='URL of the NameNode')
    
    args = parser.parse_args()
    
    # Asegurar que el directorio de almacenamiento existe
    os.makedirs(args.storage_dir, exist_ok=True)
    
    # Iniciar el servicio
    serve(
        node_id=args.node_id,
        hostname=args.hostname,
        port=args.port,
        storage_dir=args.storage_dir,
        namenode_url=args.namenode_url
    )

if __name__ == "__main__":
    main()
