import os
import sys
import argparse

# Añadir el directorio src al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.namenode.api.main import app
import uvicorn

def main():
    parser = argparse.ArgumentParser(description='Iniciar el NameNode para pruebas')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host para el servidor')
    parser.add_argument('--port', type=int, default=8000, help='Puerto para el servidor')
    
    args = parser.parse_args()
    
    print(f"Iniciando NameNode en {args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()
