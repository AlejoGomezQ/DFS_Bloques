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
            
            # Subir bloques en paralelo con barra de progreso
            print(f"Subiendo bloques a los DataNodes...")
            
            # Inicializar variables para la barra de progreso
            uploaded_blocks = 0
            total_blocks = len(blocks)
            progress_bar_width = 50
            
            # Función para subir un bloque a un DataNode con reintentos
            def upload_block_to_datanode(block, node_info, max_retries=2):
                failed_nodes = []
                for attempt in range(max_retries):
                    try:
                        with DataNodeClient(node_info['hostname'], node_info['port']) as datanode:
                            success = datanode.store_block(block['block_id'], block['data'])
                            
                            if success:
                                # Registrar el bloque y su ubicación en el NameNode
                                block_info = {
                                    'block_id': block['block_id'],
                                    'file_id': file_id,
                                    'size': block['size'],
                                    'checksum': block['checksum'],
                                    'locations': [{
                                        'block_id': block['block_id'],
                                        'datanode_id': node_info['node_id'],
                                        'is_leader': node_info.get('is_leader', False)
                                    }]
                                }
                                
                                # Registrar el bloque en el NameNode
                                try:
                                    self.namenode_client._make_request('post', '/blocks/', block_info)
                                    return True, block['block_id'], node_info['node_id']
                                except Exception as e:
                                    print(f"\nError al registrar bloque en NameNode: {str(e)}")
                                    return False, block['block_id'], node_info['node_id']
                            
                            failed_nodes.append(node_info['node_id'])
                            
                            # Si falló y hay más intentos, intentar con un DataNode alternativo
                            if attempt < max_retries - 1:
                                alternative_nodes = self.block_distributor.get_alternative_datanodes(
                                    block['size'], 
                                    failed_nodes
                                )
                                if alternative_nodes:
                                    node_info = alternative_nodes[0]
                                    continue
                    except Exception as e:
                        print(f"\nError al subir bloque {block['block_id']}: {str(e)}")
                        failed_nodes.append(node_info['node_id'])
                        if attempt < max_retries - 1:
                            alternative_nodes = self.block_distributor.get_alternative_datanodes(
                                block['size'], 
                                failed_nodes
                            )
                            if alternative_nodes:
                                node_info = alternative_nodes[0]
                                continue
                
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
            
            if failed_uploads > 0:
                print(f"Advertencia: {failed_uploads} bloques no se pudieron subir correctamente")
                print("El archivo podría no estar completo en el DFS")
                return False
            
            return True
            
        except Exception as e:
            print(f"Error al subir el archivo: {str(e)}")
            return False
            
    def _format_size(self, size_bytes: int) -> str:
        """
        Formatea un tamaño de bytes a una cadena legible.
        
        Args:
            size_bytes: Tamaño en bytes
            
        Returns:
            Tamaño formateado como una cadena legible
        """
        if size_bytes < 1024:
            return f"{size_bytes} bytes"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    
    def get_file(self, dfs_path: str, local_path: str, max_workers: int = 4) -> bool:
        """
        Descarga un archivo del sistema de archivos distribuido.
        
        Args:
            dfs_path: Ruta del archivo en el DFS
            local_path: Ruta local donde se guardará el archivo
            max_workers: Número máximo de hilos para descargar bloques en paralelo
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            # Obtener información del archivo
            file_info = self.namenode_client.get_file_info(dfs_path)
            if not file_info:
                print(f"Error: El archivo {dfs_path} no existe en el DFS")
                return False
            
            # Verificar que es un archivo y no un directorio
            if file_info.get('type') != 'file':
                print(f"Error: {dfs_path} no es un archivo")
                return False
            
            # Obtener los bloques del archivo
            blocks = file_info.get('blocks', [])
            
            if not blocks:
                print("Error: No se encontraron bloques para el archivo")
                return False
            
            print(f"Archivo encontrado: {file_info.get('name')} ({len(blocks)} bloques)")
            
            # Crear el directorio local si no existe
            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
            
            # Descargar bloques en paralelo
            print(f"Descargando {len(blocks)} bloques...")
            downloaded_blocks = 0
            progress_bar_width = 50
            
            def download_block(block_info):
                block_id = block_info.get('block_id')
                locations = block_info.get('locations', [])
                
                if not locations:
                    print(f"Error: No hay ubicaciones disponibles para el bloque {block_id}")
                    return False, block_id, None
                
                # Intentar descargar de cada ubicación hasta que una funcione
                errors = []
                for location in locations:
                    try:
                        # Ya tenemos la información del DataNode en la ubicación
                        hostname = location.get('hostname')
                        port = location.get('port')
                        
                        if not hostname or not port:
                            errors.append(f"Información incompleta del DataNode")
                            continue
                            
                        print(f"Intentando descargar bloque {block_id} desde {hostname}:{port}")
                        with DataNodeClient(hostname, port) as datanode:
                            block_data = datanode.retrieve_block(block_id)
                            if block_data:
                                print(f"Bloque {block_id} descargado exitosamente ({len(block_data)} bytes)")
                                return True, block_id, block_data
                            else:
                                errors.append(f"El DataNode devolvió datos vacíos")
                    except Exception as e:
                        errors.append(f"Error con DataNode {location.get('datanode_id')} - {str(e)}")
                        continue
                
                print(f"No se pudo descargar el bloque {block_id}. Errores: {'; '.join(errors)}")
                return False, block_id, None
            
            # Descargar bloques en paralelo
            block_data_map = {}
            failed_blocks = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(download_block, block) for block in blocks]
                
                for future in as_completed(futures):
                    success, block_id, data = future.result()
                    
                    if success and data:
                        block_data_map[block_id] = data
                    else:
                        failed_blocks.append(block_id)
                    
                    # Actualizar la barra de progreso
                    downloaded_blocks += 1
                    progress = downloaded_blocks / len(blocks)
                    filled_length = int(progress_bar_width * progress)
                    bar = '█' * filled_length + '-' * (progress_bar_width - filled_length)
                    percentage = progress * 100
                    
                    sys.stdout.write(f"\r[{bar}] {percentage:.1f}% ({downloaded_blocks}/{len(blocks)})")
                    sys.stdout.flush()
            
            print("\n")
            
            if failed_blocks:
                print(f"Error: No se pudieron descargar {len(failed_blocks)} bloques")
                print("Bloques fallidos:", failed_blocks)
                return False
            
            # Escribir los bloques en orden al archivo local
            print("Escribiendo bloques al archivo local...")
            with open(local_path, 'wb') as f:
                for block in blocks:
                    block_id = block.get('block_id')
                    if block_id in block_data_map:
                        f.write(block_data_map[block_id])
            
            print(f"Archivo descargado exitosamente en {local_path}")
            return True
            
        except Exception as e:
            print(f"Error al descargar el archivo: {str(e)}")
            return False
    
    def delete_directory_recursive(self, dfs_path: str) -> bool:
        """
        Elimina un directorio y todo su contenido recursivamente.
        
        Args:
            dfs_path: Ruta del directorio en el DFS a eliminar
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            # Verificar que el directorio existe
            dir_info = self.namenode_client.list_directory(dfs_path)
            if not dir_info:
                print(f"Error: El directorio {dfs_path} no existe")
                return False
            
            # Obtener el contenido del directorio
            contents = dir_info.get('contents', [])
            
            # Eliminar recursivamente todos los archivos y subdirectorios
            for item in contents:
                item_path = f"{dfs_path}/{item['name']}" if dfs_path.endswith('/') else f"{dfs_path}/{item['name']}"
                if item['type'] == 'directory':
                    # Eliminar subdirectorio recursivamente
                    if not self.delete_directory_recursive(item_path):
                        return False
                else:
                    # Eliminar archivo
                    if not self.delete_file(item_path, silent=True):
                        return False
            
            # Finalmente, eliminar el directorio vacío
            try:
                self.namenode_client.delete_directory(dfs_path)
                print(f"Directorio {dfs_path} eliminado exitosamente")
                return True
            except Exception as e:
                print(f"Error al eliminar el directorio {dfs_path}: {e}")
                return False
            
        except Exception as e:
            print(f"Error al eliminar el directorio recursivamente: {e}")
            return False
    
    def delete_file(self, dfs_path: str, silent: bool = False) -> bool:
        """
        Elimina un archivo del sistema de archivos distribuido.
        
        Args:
            dfs_path: Ruta del archivo en el DFS a eliminar
            silent: Si es True, no muestra mensajes de progreso
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            # Verificar que el archivo existe
            file_info = self.namenode_client.get_file_by_path(dfs_path)
            if not file_info:
                if not silent:
                    print(f"Error: El archivo {dfs_path} no existe")
                return False
            
            # Verificar que es un archivo y no un directorio
            if file_info.get('type') == 'directory':
                if not silent:
                    print(f"Error: {dfs_path} es un directorio, use rmdir para eliminarlo")
                return False
            
            # Obtener el ID del archivo
            file_id = file_info.get('file_id')
            
            # Eliminar el archivo del NameNode
            if not silent:
                print(f"Eliminando archivo {dfs_path}...")
            self.namenode_client.delete_file(file_id)
            
            if not silent:
                print(f"Archivo {dfs_path} eliminado exitosamente")
            return True
            
        except Exception as e:
            print(f"Error al eliminar el archivo: {e}")
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
