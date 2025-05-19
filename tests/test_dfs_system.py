#!/usr/bin/env python3
"""
Script para probar todos los componentes del sistema DFS_Bloques.
Este script inicia todos los componentes necesarios y ejecuta pruebas para verificar
que todos los puntos del PRD marcados como completados funcionen correctamente.
"""
import os
import sys
import time
import signal
import subprocess
import tempfile
import random
import string
import shutil
from pathlib import Path

# Directorio base del proyecto
BASE_DIR = Path(__file__).parent.absolute()

# Procesos en ejecución
processes = []

def signal_handler(sig, frame):
    """Manejador de señales para detener todos los procesos al salir."""
    print("\nDeteniendo todos los procesos...")
    for p in processes:
        if p and p.poll() is None:  # Si el proceso sigue en ejecución
            p.terminate()
    sys.exit(0)

def create_directories():
    """Crea los directorios necesarios para los DataNodes."""
    for i in range(1, 4):
        os.makedirs(os.path.join(BASE_DIR, f"data/datanode{i}"), exist_ok=True)

def start_namenode():
    """Inicia el NameNode."""
    print("\n=== Iniciando NameNode ===")
    cmd = [
        sys.executable, "-m", "src.namenode.api.main",
        "--id", "namenode1",
        "--host", "localhost",
        "--rest-port", "8000",
        "--grpc-port", "50051"
    ]
    
    # Crear un archivo para capturar la salida
    log_file = open(os.path.join(BASE_DIR, "namenode.log"), "w")
    
    process = subprocess.Popen(
        cmd,
        cwd=BASE_DIR,
        stdout=log_file,
        stderr=log_file,
        text=True,
        bufsize=1
    )
    processes.append(process)
    
    # Esperar a que el NameNode esté listo
    print("Esperando a que el NameNode esté listo...")
    for _ in range(10):
        if process.poll() is not None:
            print("Error: El NameNode se detuvo inesperadamente.")
            log_file.close()
            with open(os.path.join(BASE_DIR, "namenode.log"), "r") as f:
                print("\nContenido del log del NameNode:")
                print(f.read())
            return None
        
        try:
            import requests
            response = requests.get("http://localhost:8000/health", timeout=1)
            if response.status_code == 200:
                print("NameNode iniciado correctamente.")
                return process
        except Exception:
            time.sleep(1)
    
    print("Error: No se pudo conectar al NameNode después de varios intentos.")
    log_file.close()
    with open(os.path.join(BASE_DIR, "namenode.log"), "r") as f:
        print("\nContenido del log del NameNode:")
        print(f.read())
    return None

def start_datanodes():
    """Inicia los DataNodes."""
    datanode_processes = []
    
    for i in range(1, 4):
        print(f"\n=== Iniciando DataNode {i} ===")
        cmd = [
            sys.executable, "-m", "src.datanode.main",
            "--node-id", f"datanode{i}",
            "--hostname", "localhost",
            "--port", f"{5000 + i}",
            "--storage-dir", f"./data/datanode{i}",
            "--namenode-url", "http://localhost:8000"
        ]
        
        # Crear un archivo para capturar la salida
        log_file = open(os.path.join(BASE_DIR, f"datanode{i}.log"), "w")
        
        process = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            stdout=log_file,
            stderr=log_file,
            text=True,
            bufsize=1
        )
        processes.append(process)
        datanode_processes.append(process)
        
        # Esperar un poco para que el DataNode se registre
        print(f"Esperando a que el DataNode {i} se registre...")
        time.sleep(2)
        
        # Verificar si el proceso ha terminado (error)
        if process.poll() is not None:
            print(f"Error: El DataNode {i} se detuvo inesperadamente.")
            log_file.close()
            with open(os.path.join(BASE_DIR, f"datanode{i}.log"), "r") as f:
                print(f"\nContenido del log del DataNode {i}:")
                print(f.read())
        else:
            print(f"DataNode {i} iniciado.")
    
    return datanode_processes

def run_cli_tests():
    """Ejecuta pruebas en el CLI para verificar todas las funcionalidades."""
    print("\n=== Ejecutando pruebas en el CLI ===")
    
    # Crear archivos de prueba
    test_dir = os.path.join(BASE_DIR, "test_files")
    os.makedirs(test_dir, exist_ok=True)
    
    small_file = os.path.join(test_dir, "small_file.txt")
    with open(small_file, "w") as f:
        f.write("Este es un archivo pequeño de prueba para el DFS.")
    
    medium_file = os.path.join(test_dir, "medium_file.txt")
    with open(medium_file, "w") as f:
        f.write("A" * 1024 * 10)  # ~10KB
    
    # Comandos para probar todas las funcionalidades
    test_commands = [
        # Prueba de operaciones básicas de directorio (Punto 3.4)
        "mkdir /test",
        "mkdir /test/documents",
        "mkdir /test/images",
        "mkdir -p /test/nested/folders/example",
        "ls -l /",
        "ls -l /test",
        
        # Prueba de operaciones PUT y GET (Puntos 3.2 y 3.3)
        f"put {small_file} /test/documents/small_file.txt",
        f"put {medium_file} /test/images/medium_file.txt --workers=3",
        "ls -l /test/documents",
        "ls -l /test/images",
        "get /test/documents/small_file.txt test_files/downloaded_small.txt",
        "get /test/images/medium_file.txt test_files/downloaded_medium.txt --workers=3",
        
        # Prueba de navegación entre directorios (CD y PWD)
        "pwd",
        "cd /test",
        "pwd",
        "cd documents",
        "pwd",
        "ls",
        "cd ..",
        "pwd",
        "cd /",
        "pwd",
        
        # Prueba de los comandos RM y RMDIR (Punto 5.1)
        "rm -f /test/documents/small_file.txt",
        "ls -l /test/documents",
        "mkdir /test/temp",
        f"put {small_file} /test/temp/file.txt",
        "rmdir /test/temp",  # Debería fallar (directorio no vacío)
        "rmdir -r /test/temp",  # Debería funcionar (eliminar recursivamente)
        "ls -l /test",
        "mkdir /test/temp2",
        f"put {small_file} /test/temp2/file.txt",
        "rmdir -r -f /test/temp2",  # Forzar eliminación sin confirmación
        "ls -l /test",
        
        # Limpieza final
        "rmdir -r -f /test",
        "ls -l /",
        "exit"
    ]
    
    # Escribir los comandos a un archivo
    commands_file = os.path.join(test_dir, "cli_commands.txt")
    with open(commands_file, "w") as f:
        for cmd in test_commands:
            f.write(cmd + "\n")
    
    # Ejecutar el CLI con los comandos de prueba
    print("Ejecutando comandos de prueba en el CLI...")
    cmd = [
        sys.executable, "-m", "src.client.cli",
        "--namenode", "http://localhost:8000"
    ]
    
    # Usar el archivo de comandos como entrada
    with open(commands_file, "r") as input_file:
        process = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            stdin=input_file,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        stdout, stderr = process.communicate()
        
        # Guardar la salida en un archivo
        with open(os.path.join(BASE_DIR, "cli_output.log"), "w") as f:
            f.write(stdout)
            if stderr:
                f.write("\n\nERRORES:\n")
                f.write(stderr)
        
        print("\nPruebas del CLI completadas.")
        print(f"Salida guardada en {os.path.join(BASE_DIR, 'cli_output.log')}")
        
        # Mostrar un resumen de las pruebas
        print("\n=== Resumen de Pruebas ===")
        
        # Verificar si se crearon los archivos descargados
        downloaded_small = os.path.join(test_dir, "downloaded_small.txt")
        downloaded_medium = os.path.join(test_dir, "downloaded_medium.txt")
        
        if os.path.exists(downloaded_small):
            with open(small_file, "r") as f1, open(downloaded_small, "r") as f2:
                if f1.read() == f2.read():
                    print("✓ Prueba GET (archivo pequeño): Éxito")
                else:
                    print("✗ Prueba GET (archivo pequeño): Fallo - El contenido no coincide")
        else:
            print("✗ Prueba GET (archivo pequeño): Fallo - No se descargó el archivo")
        
        if os.path.exists(downloaded_medium):
            with open(medium_file, "r") as f1, open(downloaded_medium, "r") as f2:
                if f1.read() == f2.read():
                    print("✓ Prueba GET (archivo mediano): Éxito")
                else:
                    print("✗ Prueba GET (archivo mediano): Fallo - El contenido no coincide")
        else:
            print("✗ Prueba GET (archivo mediano): Fallo - No se descargó el archivo")
        
        # Analizar la salida para verificar otras pruebas
        if "Directorio /test creado exitosamente" in stdout:
            print("✓ Prueba MKDIR: Éxito")
        else:
            print("✗ Prueba MKDIR: Fallo")
        
        if "Directorio /test/nested/folders/example creado exitosamente" in stdout:
            print("✓ Prueba MKDIR -p (recursivo): Éxito")
        else:
            print("✗ Prueba MKDIR -p (recursivo): Fallo")
        
        if "Archivo test_files/small_file.txt subido exitosamente" in stdout:
            print("✓ Prueba PUT: Éxito")
        else:
            print("✗ Prueba PUT: Fallo")
        
        if "Archivo /test/documents/small_file.txt eliminado exitosamente" in stdout:
            print("✓ Prueba RM: Éxito")
        else:
            print("✗ Prueba RM: Fallo")
        
        if "Directorio /test/temp y todo su contenido eliminados exitosamente" in stdout:
            print("✓ Prueba RMDIR -r: Éxito")
        else:
            print("✗ Prueba RMDIR -r: Fallo")
        
        if "Directorio actual: /test/documents" in stdout:
            print("✓ Prueba CD: Éxito")
        else:
            print("✗ Prueba CD: Fallo")

def main():
    """Función principal."""
    # Registrar el manejador de señales
    signal.signal(signal.SIGINT, signal_handler)
    
    # Crear directorios necesarios
    create_directories()
    
    # Iniciar el NameNode
    namenode_process = start_namenode()
    if not namenode_process:
        print("Error: No se pudo iniciar el NameNode. Abortando pruebas.")
        return
    
    # Iniciar los DataNodes
    datanode_processes = start_datanodes()
    if not datanode_processes:
        print("Error: No se pudieron iniciar los DataNodes. Abortando pruebas.")
        return
    
    # Esperar un poco para que los DataNodes se registren
    print("\nEsperando a que los DataNodes se registren con el NameNode...")
    time.sleep(5)
    
    # Ejecutar pruebas en el CLI
    run_cli_tests()
    
    # Detener todos los procesos
    print("\nDeteniendo todos los procesos...")
    for p in processes:
        if p and p.poll() is None:
            p.terminate()
    
    print("\nPruebas completadas. Revisa los archivos de log para más detalles.")

if __name__ == "__main__":
    main()
