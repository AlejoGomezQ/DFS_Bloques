import os
import sys
import tempfile
import random
import time

# Agregar el directorio raíz al path para poder importar los módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.client.file_splitter import FileSplitter


def create_test_file(file_path, size_mb):
    """Crea un archivo de prueba con contenido aleatorio"""
    size_bytes = size_mb * 1024 * 1024
    
    print(f"Creando archivo de prueba de {size_mb}MB en {file_path}...")
    
    with open(file_path, 'wb') as f:
        # Generar contenido aleatorio en bloques de 1MB para no consumir mucha memoria
        chunk_size = 1024 * 1024  # 1MB
        for _ in range(0, size_bytes, chunk_size):
            # Ajustar el tamaño del último bloque si es necesario
            current_chunk_size = min(chunk_size, size_bytes - _ if _ < size_bytes else 0)
            f.write(os.urandom(current_chunk_size))
    
    print(f"Archivo creado: {file_path} ({os.path.getsize(file_path)} bytes)")


def demo_file_partitioning():
    """Demostración del particionamiento de archivos en bloques"""
    # Crear un directorio temporal para la demostración
    temp_dir = tempfile.mkdtemp()
    print(f"Directorio temporal creado: {temp_dir}")
    
    try:
        # Crear un archivo de prueba
        test_file_path = os.path.join(temp_dir, "original_file.dat")
        file_size_mb = 10  # 10MB
        create_test_file(test_file_path, file_size_mb)
        
        # Configurar diferentes tamaños de bloque para demostración
        block_sizes = [
            1 * 1024 * 1024,    # 1MB
            2 * 1024 * 1024,    # 2MB
            4 * 1024 * 1024     # 4MB
        ]
        
        for block_size in block_sizes:
            print("\n" + "="*80)
            print(f"Probando con tamaño de bloque: {block_size/1024/1024:.1f}MB")
            print("="*80)
            
            # Inicializar el particionador con el tamaño de bloque actual
            file_splitter = FileSplitter(block_size)
            
            # Medir el tiempo de particionamiento
            start_time = time.time()
            blocks = file_splitter.split_file(test_file_path)
            split_time = time.time() - start_time
            
            # Mostrar información sobre los bloques
            print(f"Archivo dividido en {len(blocks)} bloques en {split_time:.3f} segundos")
            print(f"Tamaño promedio de bloque: {sum(b['size'] for b in blocks)/len(blocks)/1024:.1f}KB")
            
            # Mostrar detalles de cada bloque
            print("\nDetalles de los bloques:")
            print("-" * 80)
            print(f"{'Índice':<10} {'ID':<36} {'Tamaño (bytes)':<15} {'Checksum (primeros 8 caracteres)'}")
            print("-" * 80)
            
            for block in blocks:
                print(f"{block['index']:<10} {block['block_id']:<36} {block['size']:<15} {block['checksum'][:8]}...")
            
            # Reconstruir el archivo
            output_path = os.path.join(temp_dir, f"reconstructed_{int(block_size/1024/1024)}MB.dat")
            
            start_time = time.time()
            result = file_splitter.join_blocks(blocks, output_path)
            join_time = time.time() - start_time
            
            if result:
                print(f"\nArchivo reconstruido exitosamente en {join_time:.3f} segundos: {output_path}")
                
                # Verificar que el tamaño del archivo reconstruido es igual al original
                original_size = os.path.getsize(test_file_path)
                reconstructed_size = os.path.getsize(output_path)
                
                if original_size == reconstructed_size:
                    print(f"✓ Tamaño verificado: {original_size} bytes")
                else:
                    print(f"✗ Error de tamaño: Original {original_size} bytes, Reconstruido {reconstructed_size} bytes")
                
                # Verificar el contenido (comparando los primeros bytes para no consumir mucha memoria)
                with open(test_file_path, 'rb') as f1, open(output_path, 'rb') as f2:
                    sample_size = min(1024 * 1024, original_size)  # Comparar hasta 1MB
                    data1 = f1.read(sample_size)
                    data2 = f2.read(sample_size)
                    
                    if data1 == data2:
                        print(f"✓ Contenido verificado (muestra de {sample_size/1024:.1f}KB)")
                    else:
                        print("✗ Error: El contenido del archivo reconstruido no coincide con el original")
            else:
                print("✗ Error: No se pudo reconstruir el archivo")
    
    finally:
        # Preguntar si se desea conservar los archivos temporales
        keep_files = input("\n¿Desea conservar los archivos temporales? (s/n): ").lower() == 's'
        
        if not keep_files:
            import shutil
            print(f"Eliminando directorio temporal: {temp_dir}")
            shutil.rmtree(temp_dir)
        else:
            print(f"Los archivos temporales se han conservado en: {temp_dir}")


if __name__ == '__main__':
    demo_file_partitioning()
