#!/usr/bin/env python3
"""
Script para probar la optimización de transferencia de datos mediante compresión.
Este script genera archivos de prueba, los sube al DFS y mide el rendimiento
con y sin compresión para demostrar la mejora.
"""
import os
import time
import random
import string
import tempfile
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

from src.client.dfs_client import DFSClient
from src.client.datanode_client import DataNodeClient
from src.common.compression import DataCompressor, COMPRESSION_NONE, COMPRESSION_FAST, COMPRESSION_BALANCED, COMPRESSION_MAX

# Directorio base del proyecto
BASE_DIR = Path(__file__).parent.absolute()

def generate_test_file(file_path, size_mb, data_type="random"):
    """
    Genera un archivo de prueba con el tamaño y tipo de datos especificados.
    
    Args:
        file_path: Ruta donde se guardará el archivo
        size_mb: Tamaño del archivo en MB
        data_type: Tipo de datos ("random", "text", "binary", "repeating")
    """
    size_bytes = size_mb * 1024 * 1024
    
    with open(file_path, 'wb') as f:
        if data_type == "random":
            # Datos aleatorios (difíciles de comprimir)
            f.write(os.urandom(size_bytes))
            
        elif data_type == "text":
            # Texto aleatorio (compresible)
            chars = string.ascii_letters + string.digits + string.punctuation + ' ' * 10
            text = ''.join(random.choice(chars) for _ in range(size_bytes))
            f.write(text.encode('utf-8'))
            
        elif data_type == "binary":
            # Datos binarios con patrones (muy compresibles)
            pattern = bytes([random.randint(0, 255) for _ in range(1024)])
            repeats = size_bytes // len(pattern) + 1
            f.write(pattern * repeats)
            
        elif data_type == "repeating":
            # Datos altamente repetitivos (extremadamente compresibles)
            pattern = b'ABCDEFGHIJKLMNOPQRSTUVWXYZ' * 40
            repeats = size_bytes // len(pattern) + 1
            f.write(pattern * repeats)
    
    print(f"Archivo generado: {file_path} ({size_mb} MB, tipo: {data_type})")

def test_compression_performance():
    """Prueba el rendimiento de diferentes algoritmos y niveles de compresión."""
    print("\n=== Prueba de Rendimiento de Compresión ===")
    
    # Crear un directorio temporal para los archivos de prueba
    temp_dir = tempfile.mkdtemp()
    
    # Generar archivos de prueba de diferentes tipos
    test_files = []
    for data_type in ["random", "text", "binary", "repeating"]:
        file_path = os.path.join(temp_dir, f"test_{data_type}_5mb.dat")
        generate_test_file(file_path, 5, data_type)
        test_files.append((file_path, data_type))
    
    # Inicializar el compresor
    compressor = DataCompressor()
    
    # Probar cada archivo con diferentes niveles de compresión
    results = []
    
    for file_path, data_type in test_files:
        print(f"\nProbando compresión para {os.path.basename(file_path)} ({data_type}):")
        
        # Leer el archivo
        with open(file_path, 'rb') as f:
            data = f.read()
        
        original_size = len(data)
        
        # Probar diferentes niveles de compresión
        for level in [COMPRESSION_NONE, COMPRESSION_FAST, COMPRESSION_BALANCED, COMPRESSION_MAX]:
            level_name = {
                COMPRESSION_NONE: "None",
                COMPRESSION_FAST: "Fast",
                COMPRESSION_BALANCED: "Balanced",
                COMPRESSION_MAX: "Maximum"
            }.get(level, str(level))
            
            # Medir tiempo de compresión
            start_time = time.time()
            compressed_data, metadata = compressor.compress(data, level=level)
            compression_time = time.time() - start_time
            
            # Medir tiempo de descompresión
            start_time = time.time()
            decompressed_data = compressor.decompress(compressed_data, metadata)
            decompression_time = time.time() - start_time
            
            # Verificar integridad
            is_valid = data == decompressed_data
            
            # Calcular ratio de compresión
            compressed_size = len(compressed_data)
            ratio = original_size / compressed_size if compressed_size > 0 else 1.0
            
            # Guardar resultados
            results.append({
                "data_type": data_type,
                "level": level_name,
                "original_size": original_size,
                "compressed_size": compressed_size,
                "ratio": ratio,
                "compression_time": compression_time,
                "decompression_time": decompression_time,
                "is_valid": is_valid
            })
            
            print(f"  Nivel {level_name}: {original_size/1024/1024:.2f}MB → {compressed_size/1024/1024:.2f}MB "
                  f"(ratio: {ratio:.2f}x, tiempo: {compression_time:.4f}s comp, {decompression_time:.4f}s decomp)")
    
    # Visualizar resultados
    plot_compression_results(results)
    
    return results

def test_dfs_transfer_performance(namenode_url="http://localhost:8000"):
    """Prueba el rendimiento de transferencia en el DFS con y sin compresión."""
    print("\n=== Prueba de Rendimiento de Transferencia en DFS ===")
    
    # Crear un directorio temporal para los archivos de prueba
    temp_dir = tempfile.mkdtemp()
    
    # Generar archivos de prueba de diferentes tamaños y tipos
    test_files = []
    
    # Archivos de texto (compresibles)
    for size_mb in [1, 5, 10]:
        file_path = os.path.join(temp_dir, f"text_{size_mb}mb.txt")
        generate_test_file(file_path, size_mb, "text")
        test_files.append((file_path, f"text_{size_mb}mb"))
    
    # Archivos binarios (menos compresibles)
    for size_mb in [1, 5, 10]:
        file_path = os.path.join(temp_dir, f"binary_{size_mb}mb.dat")
        generate_test_file(file_path, size_mb, "random")
        test_files.append((file_path, f"binary_{size_mb}mb"))
    
    # Crear clientes DFS con y sin compresión
    client_compressed = DFSClient(namenode_url)
    client_compressed.datanode_client_class = lambda hostname, port: DataNodeClient(
        hostname, port, use_compression=True, compression_level=COMPRESSION_BALANCED
    )
    
    client_uncompressed = DFSClient(namenode_url)
    client_uncompressed.datanode_client_class = lambda hostname, port: DataNodeClient(
        hostname, port, use_compression=False
    )
    
    # Resultados
    results = []
    
    # Probar cada archivo
    for file_path, file_type in test_files:
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        
        print(f"\nProbando transferencia para {os.path.basename(file_path)} ({file_size:.2f} MB):")
        
        # Probar con compresión
        print("  Con compresión:")
        dfs_path_compressed = f"/test/compressed/{os.path.basename(file_path)}"
        
        # Subir archivo con compresión
        start_time = time.time()
        upload_success = client_compressed.put_file(file_path, dfs_path_compressed)
        upload_time_compressed = time.time() - start_time
        
        # Obtener estadísticas de transferencia
        compressed_stats = {}
        for hostname, port in client_compressed.datanode_clients:
            client = client_compressed.datanode_client_class(hostname, port)
            stats = client.get_transfer_stats()
            compressed_stats = stats  # Usar las estadísticas del último cliente
        
        # Descargar archivo con compresión
        download_path_compressed = os.path.join(temp_dir, f"download_compressed_{os.path.basename(file_path)}")
        start_time = time.time()
        download_success = client_compressed.get_file(dfs_path_compressed, download_path_compressed)
        download_time_compressed = time.time() - start_time
        
        # Verificar integridad
        with open(file_path, 'rb') as f1, open(download_path_compressed, 'rb') as f2:
            is_valid_compressed = f1.read() == f2.read()
        
        # Probar sin compresión
        print("  Sin compresión:")
        dfs_path_uncompressed = f"/test/uncompressed/{os.path.basename(file_path)}"
        
        # Subir archivo sin compresión
        start_time = time.time()
        upload_success = client_uncompressed.put_file(file_path, dfs_path_uncompressed)
        upload_time_uncompressed = time.time() - start_time
        
        # Obtener estadísticas de transferencia
        uncompressed_stats = {}
        for hostname, port in client_uncompressed.datanode_clients:
            client = client_uncompressed.datanode_client_class(hostname, port)
            stats = client.get_transfer_stats()
            uncompressed_stats = stats  # Usar las estadísticas del último cliente
        
        # Descargar archivo sin compresión
        download_path_uncompressed = os.path.join(temp_dir, f"download_uncompressed_{os.path.basename(file_path)}")
        start_time = time.time()
        download_success = client_uncompressed.get_file(dfs_path_uncompressed, download_path_uncompressed)
        download_time_uncompressed = time.time() - start_time
        
        # Verificar integridad
        with open(file_path, 'rb') as f1, open(download_path_uncompressed, 'rb') as f2:
            is_valid_uncompressed = f1.read() == f2.read()
        
        # Calcular mejora de rendimiento
        upload_speedup = upload_time_uncompressed / upload_time_compressed if upload_time_compressed > 0 else 1.0
        download_speedup = download_time_uncompressed / download_time_compressed if download_time_compressed > 0 else 1.0
        
        # Guardar resultados
        result = {
            "file_type": file_type,
            "file_size_mb": file_size,
            "upload_time_compressed": upload_time_compressed,
            "upload_time_uncompressed": upload_time_uncompressed,
            "download_time_compressed": download_time_compressed,
            "download_time_uncompressed": download_time_uncompressed,
            "upload_speedup": upload_speedup,
            "download_speedup": download_speedup,
            "is_valid_compressed": is_valid_compressed,
            "is_valid_uncompressed": is_valid_uncompressed,
            "compressed_stats": compressed_stats,
            "uncompressed_stats": uncompressed_stats
        }
        
        results.append(result)
        
        print(f"  Subida: {upload_time_uncompressed:.2f}s → {upload_time_compressed:.2f}s (speedup: {upload_speedup:.2f}x)")
        print(f"  Descarga: {download_time_uncompressed:.2f}s → {download_time_compressed:.2f}s (speedup: {download_speedup:.2f}x)")
        
        if "compression_ratio_sent" in compressed_stats:
            print(f"  Ratio de compresión: {compressed_stats.get('compression_ratio_sent', 1.0):.2f}x")
    
    # Visualizar resultados
    plot_transfer_results(results)
    
    return results

def plot_compression_results(results):
    """Visualiza los resultados de las pruebas de compresión."""
    try:
        # Preparar datos para gráficos
        data_types = sorted(set(r["data_type"] for r in results))
        levels = ["None", "Fast", "Balanced", "Maximum"]
        
        # Gráfico de ratio de compresión
        plt.figure(figsize=(12, 6))
        
        x = np.arange(len(data_types))
        width = 0.2
        
        for i, level in enumerate(levels):
            ratios = [next((r["ratio"] for r in results if r["data_type"] == dt and r["level"] == level), 1.0) 
                     for dt in data_types]
            plt.bar(x + i*width - width*1.5, ratios, width, label=f'Level: {level}')
        
        plt.xlabel('Data Type')
        plt.ylabel('Compression Ratio (higher is better)')
        plt.title('Compression Ratio by Data Type and Compression Level')
        plt.xticks(x, data_types)
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Guardar gráfico
        plt.savefig(os.path.join(BASE_DIR, 'compression_ratio.png'))
        plt.close()
        
        # Gráfico de tiempo de compresión
        plt.figure(figsize=(12, 6))
        
        for i, level in enumerate(levels[1:]):  # Excluir "None"
            times = [next((r["compression_time"] for r in results if r["data_type"] == dt and r["level"] == level), 0) 
                    for dt in data_types]
            plt.bar(x + (i-1)*width, times, width, label=f'Level: {level}')
        
        plt.xlabel('Data Type')
        plt.ylabel('Compression Time (seconds)')
        plt.title('Compression Time by Data Type and Compression Level')
        plt.xticks(x, data_types)
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Guardar gráfico
        plt.savefig(os.path.join(BASE_DIR, 'compression_time.png'))
        plt.close()
        
        print("\nGráficos de compresión guardados en el directorio del proyecto.")
    except Exception as e:
        print(f"Error al generar gráficos de compresión: {e}")

def plot_transfer_results(results):
    """Visualiza los resultados de las pruebas de transferencia."""
    try:
        # Preparar datos para gráficos
        file_types = sorted(set(r["file_type"] for r in results))
        
        # Gráfico de tiempo de subida
        plt.figure(figsize=(12, 6))
        
        x = np.arange(len(file_types))
        width = 0.35
        
        upload_times_compressed = [next((r["upload_time_compressed"] for r in results if r["file_type"] == ft), 0) 
                                 for ft in file_types]
        upload_times_uncompressed = [next((r["upload_time_uncompressed"] for r in results if r["file_type"] == ft), 0) 
                                   for ft in file_types]
        
        plt.bar(x - width/2, upload_times_uncompressed, width, label='Sin compresión')
        plt.bar(x + width/2, upload_times_compressed, width, label='Con compresión')
        
        plt.xlabel('Tipo de Archivo')
        plt.ylabel('Tiempo de Subida (segundos)')
        plt.title('Tiempo de Subida por Tipo de Archivo')
        plt.xticks(x, file_types)
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Guardar gráfico
        plt.savefig(os.path.join(BASE_DIR, 'upload_time.png'))
        plt.close()
        
        # Gráfico de tiempo de descarga
        plt.figure(figsize=(12, 6))
        
        download_times_compressed = [next((r["download_time_compressed"] for r in results if r["file_type"] == ft), 0) 
                                   for ft in file_types]
        download_times_uncompressed = [next((r["download_time_uncompressed"] for r in results if r["file_type"] == ft), 0) 
                                     for ft in file_types]
        
        plt.bar(x - width/2, download_times_uncompressed, width, label='Sin compresión')
        plt.bar(x + width/2, download_times_compressed, width, label='Con compresión')
        
        plt.xlabel('Tipo de Archivo')
        plt.ylabel('Tiempo de Descarga (segundos)')
        plt.title('Tiempo de Descarga por Tipo de Archivo')
        plt.xticks(x, file_types)
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Guardar gráfico
        plt.savefig(os.path.join(BASE_DIR, 'download_time.png'))
        plt.close()
        
        # Gráfico de speedup
        plt.figure(figsize=(12, 6))
        
        upload_speedup = [next((r["upload_speedup"] for r in results if r["file_type"] == ft), 1.0) 
                        for ft in file_types]
        download_speedup = [next((r["download_speedup"] for r in results if r["file_type"] == ft), 1.0) 
                          for ft in file_types]
        
        plt.bar(x - width/2, upload_speedup, width, label='Subida')
        plt.bar(x + width/2, download_speedup, width, label='Descarga')
        
        plt.xlabel('Tipo de Archivo')
        plt.ylabel('Speedup (veces más rápido)')
        plt.title('Mejora de Rendimiento con Compresión')
        plt.xticks(x, file_types)
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Guardar gráfico
        plt.savefig(os.path.join(BASE_DIR, 'transfer_speedup.png'))
        plt.close()
        
        print("\nGráficos de transferencia guardados en el directorio del proyecto.")
    except Exception as e:
        print(f"Error al generar gráficos de transferencia: {e}")

def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description='Prueba de optimización de transferencia de datos')
    parser.add_argument('--namenode', type=str, default='http://localhost:8000',
                        help='URL del NameNode (default: http://localhost:8000)')
    parser.add_argument('--compression-only', action='store_true',
                        help='Ejecutar solo pruebas de compresión (sin DFS)')
    parser.add_argument('--transfer-only', action='store_true',
                        help='Ejecutar solo pruebas de transferencia (sin pruebas de compresión)')
    
    args = parser.parse_args()
    
    print("=== Pruebas de Optimización de Transferencia de Datos ===")
    
    try:
        # Ejecutar pruebas según los argumentos
        if not args.transfer_only:
            compression_results = test_compression_performance()
        
        if not args.compression_only:
            transfer_results = test_dfs_transfer_performance(args.namenode)
        
        print("\n=== Pruebas completadas ===")
        print("Los resultados se han guardado como gráficos en el directorio del proyecto.")
    except Exception as e:
        print(f"\nError durante las pruebas: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    main()
