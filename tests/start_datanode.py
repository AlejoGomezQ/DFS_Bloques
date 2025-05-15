import os
import sys
import argparse

# Añadir el directorio src al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.datanode.service.datanode_service import serve

def main():
    parser = argparse.ArgumentParser(description='Iniciar un DataNode para pruebas')
    parser.add_argument('--node-id', type=str, default='test-datanode-1', help='ID del DataNode')
    parser.add_argument('--hostname', type=str, default='localhost', help='Hostname para el servicio')
    parser.add_argument('--port', type=int, default=50051, help='Puerto para el servicio')
    parser.add_argument('--storage-dir', type=str, default='./data/test-blocks', help='Directorio para almacenar bloques')
    
    args = parser.parse_args()
    
    # Crear directorio de almacenamiento si no existe
    os.makedirs(args.storage_dir, exist_ok=True)
    
    print(f"Iniciando DataNode {args.node_id} en {args.hostname}:{args.port}")
    print(f"Almacenamiento en: {args.storage_dir}")
    
    # Iniciar el servicio
    serve(
        node_id=args.node_id,
        hostname=args.hostname,
        port=args.port,
        storage_dir=args.storage_dir
    )

if __name__ == "__main__":
    main()
