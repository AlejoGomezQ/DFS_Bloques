import os
import uuid
import time
import sys
from typing import List, Dict, Optional, BinaryIO, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.client.namenode_client import NameNodeClient
from src.client.datanode_client import DataNodeClient
from src.client.file_splitter import FileSplitter
from src.client.block_distributor import BlockDistributor


class DFSClient:
    """
    Cliente principal para interactuar con el sistema de archivos distribuido.
    """
    def __init__(self, namenode_url: str, block_size: int = None):
        """
        Inicializa el cliente DFS.
        
        Args:
            namenode_url: URL del NameNode (ej: 'http://localhost:8000')
            block_size: Tamaño de bloque personalizado (opcional)
        """
        self.namenode_client = NameNodeClient(namenode_url)
        self.block_distributor = BlockDistributor(self.namenode_client)
        
        # Si no se especifica un tamaño de bloque, obtener el óptimo
        if block_size is None:
            block_size = self.block_distributor.get_optimal_block_size()
        
        self.file_splitter = FileSplitter(block_size)
    
    def put_file(self, local_path: str, dfs_path: str, max_workers: int = 4) -> bool:
        """
        Sube un archivo al sistema de archivos distribuido.
        
        Args:
            local_path: Ruta local del archivo a subir
            dfs_path: Ruta destino en el DFS
            max_workers: Número máximo de hilos para subir bloques en paralelo
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            # Verificar que el archivo local existe
            if not os.path.exists(local_path) or not os.path.isfile(local_path):
                print(f"Error: El archivo local {local_path} no existe o no es un archivo")
                return False
            
            # Obtener el nombre del archivo
            file_name = os.path.basename(dfs_path)
            
            # Verificar que el directorio padre existe
            parent_dir = os.path.dirname(dfs_path)
            if parent_dir and not self._ensure_directory_exists(parent_dir):
                print(f"Error: No se pudo crear el directorio padre {parent_dir}")
                return False
            
            # Obtener el tamaño del archivo para la barra de progreso
            file_size = os.path.getsize(local_path)
            print(f"Archivo: {file_name} ({self._format_size(file_size)})")
            
            # Dividir el archivo en bloques
            print(f"Dividiendo el archivo en bloques...")
            start_time = time.time()
            blocks = self.file_splitter.split_file(local_path)
            print(f"Archivo dividido en {len(blocks)} bloques en {time.time() - start_time:.2f} segundos")
            
            # Distribuir los bloques entre los DataNodes disponibles
            print(f"Seleccionando DataNodes para los bloques...")
            block_distribution = self.block_distributor.distribute_blocks(blocks)
            
            # Verificar que todos los bloques tienen DataNodes asignados
            blocks_without_nodes = [b['block_id'] for b in blocks if b['block_id'] not in block_distribution]
            if blocks_without_nodes:
                print(f"Error: {len(blocks_without_nodes)} bloques no tienen DataNodes asignados")
                return False
            
            # Crear el archivo en el NameNode
            file_metadata = {
                'name': file_name,
                'path': dfs_path,
                'type': 'file',
                'size': file_size
            }
            
            print(f"Registrando archivo en el NameNode...")
            file_info = self.namenode_client.create_file(file_metadata)
            file_id = file_info.get('file_id')
            
            if not file_id:
                print("Error: No se pudo crear el archivo en el NameNode")
                return False
            
            # Registrar todos los bloques en el NameNode
            print(f"Registrando {len(blocks)} bloques en el NameNode...")
            for block in blocks:
                self.namenode_client._make_request(
                    'post', 
                    '/blocks/', 
                    {
                        'block_id': block['block_id'],
                        'file_id': file_id,
                        'size': block['size'],
                        'checksum': block['checksum'],
                        'locations': []  # Lista vacía inicialmente, se llenará al subir el bloque
                    }
                )
            
            # Subir bloques en paralelo con barra de progreso
            print(f"Subiendo bloques a los DataNodes...")
            
            # Inicializar variables para la barra de progreso
            uploaded_blocks = 0
            total_blocks = len(blocks)
            progress_bar_width = 50
            
            # Función para subir un bloque a un DataNode
            def upload_block_to_datanode(block, node_info):
                try:
                    with DataNodeClient(node_info['hostname'], node_info['port']) as datanode:
                        success = datanode.store_block(block['block_id'], block['data'])
                        
                        if success:
                            # Registrar la ubicación del bloque en el NameNode
                            self.namenode_client._make_request(
                                'post',
                                f'/blocks/{block["block_id"]}/locations',
                                {
                                    'datanode_id': node_info['node_id'],
                                    'is_leader': node_info.get('is_leader', False)
                                }
                            )
                            return True, block['block_id'], node_info['node_id']
                        else:
                            return False, block['block_id'], node_info['node_id']
                except Exception as e:
                    return False, block['block_id'], node_info['node_id']
            
            # Lista de tareas para subir bloques
            upload_tasks = []
            for block in blocks:
                block_id = block['block_id']
                for node_info in block_distribution[block_id]:
                    upload_tasks.append((block, node_info))
            
            # Subir bloques en paralelo
            successful_uploads = 0
            failed_uploads = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(upload_block_to_datanode, block, node_info) 
                          for block, node_info in upload_tasks]
                
                for future in as_completed(futures):
                    success, block_id, node_id = future.result()
                    
                    if success:
                        successful_uploads += 1
                    else:
                        failed_uploads += 1
                        print(f"\nError al subir el bloque {block_id} al DataNode {node_id}")
                    
                    # Actualizar la barra de progreso
                    uploaded_blocks += 1
                    progress = uploaded_blocks / len(upload_tasks)
                    filled_length = int(progress_bar_width * progress)
                    bar = '█' * filled_length + '-' * (progress_bar_width - filled_length)
                    percentage = progress * 100
                    
                    sys.stdout.write(f"\r[{bar}] {percentage:.1f}% ({uploaded_blocks}/{len(upload_tasks)})")
                    sys.stdout.flush()
            
            print("\n")
            
            # Verificar que al menos una copia de cada bloque se subió correctamente
            blocks_info = self.namenode_client.get_file_blocks(file_id)
            missing_blocks = []
            
            for block in blocks:
                block_id = block['block_id']
                block_info = next((b for b in blocks_info if b.get('block_id') == block_id), None)
                
                if not block_info or not block_info.get('locations'):
                    missing_blocks.append(block_id)
            
            if missing_blocks:
                print(f"Advertencia: {len(missing_blocks)} bloques no se pudieron subir correctamente")
                print("El archivo podría no estar completo en el DFS")
                return False
            
            # Calcular estadísticas
            end_time = time.time()
            total_time = end_time - start_time
            speed = file_size / total_time if total_time > 0 else 0
            
            print(f"Archivo {dfs_path} subido exitosamente")
            print(f"Tiempo total: {total_time:.2f} segundos")
            print(f"Velocidad: {self._format_size(speed)}/s")
            print(f"Bloques: {len(blocks)} ({successful_uploads} subidas exitosas, {failed_uploads} fallidas)")
            
            return True
            
        except Exception as e:
            print(f"Error al subir el archivo: {e}")
            return False
            
    def _format_size(self, size_bytes: int) -> str:
        """
        
        # Descargar bloques en paralelo con barra de progreso
        print(f"Descargando bloques...")
        
        # Inicializar variables para la barra de progreso
        downloaded_blocks = 0
        total_blocks = len(blocks_info)
        progress_bar_width = 50
        Args:
            dfs_path: Ruta del archivo en el DFS
            local_path: Ruta local donde se guardará el archivo
            max_workers: Número máximo de hilos para descargar bloques en paralelo
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            start_time = time.time()
            
            # Obtener la información del archivo
            print(f"Obteniendo información del archivo {dfs_path}...")
            file_info = self.namenode_client.get_file_by_path(dfs_path)
            
            if not file_info:
                print(f"Error: El archivo {dfs_path} no existe en el DFS")
                return False
            
            file_id = file_info.get('file_id')
            file_size = file_info.get('size', 0)
            file_name = file_info.get('name', os.path.basename(dfs_path))
            
            print(f"Archivo: {file_name} ({self._format_size(file_size)})")
            
            # Obtener la información de los bloques
            print(f"Obteniendo información de bloques...")
            blocks_info = self.namenode_client.get_file_blocks(file_id)
            
            if not blocks_info:
                print(f"Error: No se encontraron bloques para el archivo {dfs_path}")
                return False
            
            print(f"El archivo está dividido en {len(blocks_info)} bloques")
            
            # Crear un directorio temporal para almacenar los bloques
            temp_dir = os.path.join(os.path.dirname(local_path), f".tmp_{uuid.uuid4()}")
            os.makedirs(temp_dir, exist_ok=True)
            
            # Función para descargar un bloque
            def download_block(block_info):
                block_id = block_info.get('block_id')
                block_locations = block_info.get('locations', [])
                block_size = block_info.get('size', 0)
                block_checksum = block_info.get('checksum')
                block_index = blocks_info.index(block_info)
                
                if not block_locations:
                    return False, block_id, None, block_index, "No hay ubicaciones disponibles"
                
                # Intentar descargar el bloque de cualquier DataNode disponible
                for location in block_locations:
                    try:
                        datanode_info = self.namenode_client.get_datanode(location.get('datanode_id'))
                        
                        if not datanode_info:
                            continue
                        
                        # Conectar con el DataNode
                        with DataNodeClient(datanode_info['hostname'], datanode_info['port']) as datanode:
                            # Descargar el bloque
                            block_data = datanode.retrieve_block(block_id)
                            
                            if block_data:
                                # Verificar el checksum si está disponible
                                if block_checksum:
                                    calculated_checksum = self.file_splitter._calculate_checksum(block_data)
                                    if calculated_checksum != block_checksum:
                                        continue  # Checksum no coincide, probar otro DataNode
                                
                                # Guardar el bloque temporalmente
                                block_path = os.path.join(temp_dir, block_id)
                                with open(block_path, 'wb') as f:
                                    f.write(block_data)
                                
                                return True, block_id, block_data, block_index, None
                    except Exception as e:
                        continue  # Intentar con el siguiente DataNode
                
                return False, block_id, None, block_index, "No se pudo descargar de ningún DataNode"
            
            # Descargar bloques en paralelo con barra de progreso
            print(f"Descargando bloques...")
            
            # Inicializar variables para la barra de progreso
            downloaded_blocks = 0
            total_blocks = len(blocks_info)
            progress_bar_width = 50
            
            # Lista para almacenar los bloques descargados
            blocks = [None] * total_blocks
            
            # Descargar bloques en paralelo
            successful_downloads = 0
            failed_downloads = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(download_block, block_info) for block_info in blocks_info]
                
                for future in as_completed(futures):
                    success, block_id, block_data, block_index, error_msg = future.result()
                    
                    if success:
                        successful_downloads += 1
                        blocks[block_index] = {
                            'block_id': block_id,
                            'data': block_data,
                            'index': block_index,
                            'size': len(block_data) if block_data else 0
                        }
                    else:
                        failed_downloads += 1
                        print(f"\nError al descargar el bloque {block_id}: {error_msg}")
                    
                    # Actualizar la barra de progreso
                    downloaded_blocks += 1
                    progress = downloaded_blocks / total_blocks
                    filled_length = int(progress_bar_width * progress)
                    bar = '█' * filled_length + '-' * (progress_bar_width - filled_length)
                    percentage = progress * 100
                    
                    sys.stdout.write(f"\r[{bar}] {percentage:.1f}% ({downloaded_blocks}/{total_blocks})")
                    sys.stdout.flush()
            
            print("\n")
            
            # Eliminar los elementos None de la lista (bloques que no se pudieron descargar)
            blocks = [b for b in blocks if b is not None]
            
            # Verificar que se descargaron todos los bloques
            if len(blocks) != total_blocks:
                print(f"Advertencia: Solo se pudieron descargar {len(blocks)} de {total_blocks} bloques")
                print("El archivo podría estar incompleto o corrupto")
                if len(blocks) == 0:
                    print("No se pudo descargar ningún bloque, abortando operación")
                    return False
            
            # Crear el directorio destino si no existe
            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
            
            # Escribir el archivo
            print(f"Reconstruyendo archivo...")
            with open(local_path, 'wb') as f:
                for block in blocks:
                    f.write(block['data'])
            
            # Limpiar los archivos temporales
            for block in blocks:
                block_path = os.path.join(temp_dir, block['block_id'])
                if os.path.exists(block_path):
                    os.remove(block_path)
            
            os.rmdir(temp_dir)
            
            # Calcular estadísticas
            end_time = time.time()
            total_time = end_time - start_time
            downloaded_size = os.path.getsize(local_path)
            speed = downloaded_size / total_time if total_time > 0 else 0
            
            print(f"Archivo descargado exitosamente a {local_path}")
            print(f"Tiempo total: {total_time:.2f} segundos")
            print(f"Velocidad: {self._format_size(speed)}/s")
            print(f"Bloques: {len(blocks)}/{total_blocks} ({successful_downloads} descargas exitosas, {failed_downloads} fallidas)")
            
            return True
            
        except Exception as e:
            print(f"Error al descargar el archivo: {e}")
            return False
    
    def _ensure_directory_exists(self, directory_path: str) -> bool:
        """
        Asegura que un directorio existe en el DFS, creándolo si es necesario.
        
        Args:
            directory_path: Ruta del directorio
            
        Returns:
            True si el directorio existe o se creó correctamente, False en caso contrario
        """
        try:
            # Verificar si el directorio ya existe
            dir_info = self.namenode_client.list_directory(directory_path)
            if dir_info:
                return True
            
            # Si no existe, crear el directorio y sus padres recursivamente
            parts = directory_path.strip('/').split('/')
            current_path = ""
            
            for part in parts:
                if not part:
                    continue
                
                current_path = f"{current_path}/{part}" if current_path else part
                
                try:
                    # Verificar si este nivel del directorio ya existe
                    dir_info = self.namenode_client.list_directory(current_path)
                    if not dir_info:
                        # Crear el directorio
                        self.namenode_client.create_directory({
                            'name': part,
                            'path': current_path,
                            'type': 'directory'
                        })
                except Exception:
                    # Crear el directorio
                    self.namenode_client.create_directory({
                        'name': part,
                        'path': current_path,
                        'type': 'directory'
                    })
            
            return True
        except Exception as e:
            print(f"Error al crear el directorio {directory_path}: {e}")
            return False
