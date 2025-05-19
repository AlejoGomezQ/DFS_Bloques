#!/usr/bin/env python3
"""
Script para iniciar todos los componentes del sistema DFS_Bloques.
"""
import os
import subprocess
import time
import signal
import sys
import argparse
from pathlib import Path

# Directorio base del proyecto
BASE_DIR = Path(__file__).parent.absolute()

# Procesos en ejecución
processes = []

def signal_handler(sig, frame):
    """Manejador de señales para detener todos los procesos al salir."""
    print("\nDeteniendo todos los procesos...")
    for p in processes:
        if p.poll() is None:  # Si el proceso sigue en ejecución
            p.terminate()
    sys.exit(0)

def create_directories():
    """Crea los directorios necesarios para los DataNodes."""
    for i in range(1, 4):
        os.makedirs(os.path.join(BASE_DIR, f"data/datanode{i}"), exist_ok=True)

def start_namenode():
    """Inicia el NameNode."""
    print("Iniciando NameNode...")
    cmd = [
        sys.executable, "-m", "src.namenode.api.main",
        "--id", "namenode1",
        "--host", "localhost",
        "--rest-port", "8000",
        "--grpc-port", "50051"
    ]
    process = subprocess.Popen(
        cmd,
        cwd=BASE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    processes.append(process)
    # Esperar a que el NameNode esté listo
    time.sleep(2)
    
    # Verificar si el proceso ha terminado (error)
    if process.poll() is not None:
        print("Error al iniciar el NameNode:")
        stderr = process.stderr.read()
        print(stderr)
        return None
    
    # Verificar si el NameNode está respondiendo
    try:
        import requests
        response = requests.get("http://localhost:8000/health", timeout=3)
        if response.status_code == 200:
            print("NameNode iniciado correctamente.")
        else:
            print(f"NameNode respondió con código {response.status_code}.")
    except Exception as e:
        print(f"No se pudo conectar al NameNode: {e}")
    
    return process

def start_datanodes():
    """Inicia los DataNodes."""
    for i in range(1, 4):
        print(f"Iniciando DataNode {i}...")
        cmd = [
            sys.executable, "-m", "src.datanode.main",
            "--node-id", f"datanode{i}",
            "--hostname", "localhost",
            "--port", f"{5000 + i}",
            "--storage-dir", f"./data/datanode{i}",
            "--namenode-url", "http://localhost:8000"
        ]
        process = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        processes.append(process)
        
        # Esperar un poco y verificar si el proceso ha terminado (error)
        time.sleep(1)
        if process.poll() is not None:
            print(f"Error al iniciar el DataNode {i}:")
            stderr = process.stderr.read()
            print(stderr)
        else:
            print(f"DataNode {i} iniciado.")

def start_cli():
    """Inicia el cliente CLI."""
    print("Iniciando cliente CLI...")
    cmd = [
        sys.executable, "-m", "src.client.cli",
        "--namenode", "http://localhost:8000"
    ]
    process = subprocess.Popen(
        cmd,
        cwd=BASE_DIR,
        stdin=sys.stdin,
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    processes.append(process)
    return process

def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description="Iniciar el sistema DFS_Bloques")
    parser.add_argument("--skip-namenode", action="store_true", help="No iniciar el NameNode")
    parser.add_argument("--skip-datanodes", action="store_true", help="No iniciar los DataNodes")
    parser.add_argument("--skip-cli", action="store_true", help="No iniciar el CLI")
    
    args = parser.parse_args()
    
    # Registrar el manejador de señales
    signal.signal(signal.SIGINT, signal_handler)
    
    # Crear directorios necesarios
    create_directories()
    
    # Iniciar componentes según los argumentos
    if not args.skip_namenode:
        namenode_process = start_namenode()
    
    if not args.skip_datanodes:
        start_datanodes()
    
    if not args.skip_cli:
        cli_process = start_cli()
        cli_process.wait()
    else:
        # Si no se inicia el CLI, mantener el script en ejecución
        print("Todos los componentes iniciados. Presiona Ctrl+C para detener.")
        while True:
            time.sleep(1)

if __name__ == "__main__":
    main()
