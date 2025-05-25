import os
import uuid
import time
import sys
import hashlib
import requests
from typing import List, Dict, Optional, BinaryIO, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from src.client.namenode_client import NameNodeClient
from src.client.datanode_client import DataNodeClient
from src.client.file_splitter import FileSplitter
from src.client.block_distributor import BlockDistributor


class DFSClient:
    """
    Cliente principal para interactuar con el sistema de archivos distribuido.
    """
    def __init__(self, namenode_url: str, block_size: Optional[int] = None):
        """
        Inicializa el cliente DFS.
        
        Args:
            namenode_url: URL del NameNode (ej: 'http://localhost:8000')
            block_size: Tamaño de bloque personalizado (opcional)
        """
        self.namenode_client = NameNodeClient(namenode_url)
        self.block_distributor = BlockDistributor(self.namenode_client)
        self.logger = logging.getLogger("DFSClient")
        
        # Si no se especifica un tamaño de bloque, usar un valor más pequeño para pruebas (4KB)
        # o intentar obtener el óptimo del sistema
        if block_size is None:
            try:
                self.block_size = self.block_distributor.get_optimal_block_size()
            except:
                # Si falla al obtener el tamaño óptimo, usar un valor predeterminado más pequeño para pruebas
                self.block_size = 4 * 1024  # 4KB para pruebas (normalmente sería 64KB o más)
        else:
            self.block_size = block_size
        
        self.file_splitter = FileSplitter(self.block_size)
    
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
            blocks = self._split_file_into_blocks(local_path)
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
            def upload_block_to_datanode(block, node_info, is_leader=False, max_retries=2):
                failed_nodes = []
                for attempt in range(max_retries):
                    try:
                        with DataNodeClient(node_info['hostname'], node_info['port']) as datanode:
                            # Asegurarse de que el bloque tenga el file_id
                            block['file_id'] = file_id
                            block['checksum'] = ''  # Se calculará en _upload_block
                            
                            success = self._upload_block(local_path, block, [node_info], is_leader)
                            
                            if success:
                                return True, block['block_id'], node_info['node_id']
                            
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
                # Para cada bloque, marcar el primer DataNode como líder
                for i, node_info in enumerate(block_distribution[block_id]):
                    is_leader = (i == 0)  # El primer DataNode es el líder
                    upload_tasks.append((block, node_info, is_leader))
            
            # Subir bloques en paralelo
            successful_uploads = 0
            failed_uploads = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(upload_block_to_datanode, block, node_info, is_leader) 
                          for block, node_info, is_leader in upload_tasks]
                
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
    
    def get_file(self, dfs_path: str, local_path: str, max_workers: int = 4, max_retries: int = 3) -> bool:
        """
        Descarga un archivo del sistema de archivos distribuido.
        
        Args:
            dfs_path: Ruta del archivo en el DFS
            local_path: Ruta local donde se guardará el archivo
            max_workers: Número máximo de hilos para descargar bloques en paralelo
            max_retries: Número máximo de reintentos para bloques fallidos
            
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
            
            # Descargar bloques en paralelo
            block_data_map = {}
            failed_blocks = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self.download_block, block) for block in blocks]
                
                for future in as_completed(futures):
                    success, block_id, data = future.result()
                    
                    if success and data:
                        block_data_map[block_id] = data
                    else:
                        failed_blocks.append((block_id, next((block for block in blocks if block.get('block_id') == block_id), None)))
                    
                    # Actualizar la barra de progreso
                    downloaded_blocks += 1
                    progress = downloaded_blocks / len(blocks)
                    filled_length = int(progress_bar_width * progress)
                    bar = '█' * filled_length + '-' * (progress_bar_width - filled_length)
                    percentage = progress * 100
                    
                    sys.stdout.write(f"\r[{bar}] {percentage:.1f}% ({downloaded_blocks}/{len(blocks)})")
                    sys.stdout.flush()
            
            print("\n")
            
            # Reintentar bloques fallidos
            if failed_blocks and max_retries > 0:
                print(f"Reintentando descargar {len(failed_blocks)} bloques fallidos...")
                for retry in range(max_retries):
                    still_failed = []
                    
                    for block_id, block in failed_blocks:
                        # Actualizar información del bloque para obtener ubicaciones actualizadas
                        try:
                            updated_block = self.namenode_client.get_block_info(block_id)
                            if updated_block and 'locations' in updated_block and updated_block['locations']:
                                # Usar la información actualizada del bloque
                                success, _, data = self.download_block(updated_block)
                                if success and data:
                                    block_data_map[block_id] = data
                                    print(f"Bloque {block_id} descargado exitosamente en el reintento {retry+1}")
                                    continue
                            
                            still_failed.append((block_id, block))
                        except Exception as e:
                            print(f"Error al reintentar bloque {block_id}: {e}")
                            still_failed.append((block_id, block))
                    
                    # Actualizar la lista de bloques fallidos
                    failed_blocks = still_failed
                    
                    if not failed_blocks:
                        break
                    
                    if retry < max_retries - 1:
                        print(f"Reintentando {len(failed_blocks)} bloques aún fallidos (intento {retry+2}/{max_retries})...")
                        time.sleep(1)  # Pequeña pausa antes del siguiente reintento
            
            # Verificar si todavía hay bloques fallidos
            if failed_blocks:
                failed_block_ids = [block_id for block_id, _ in failed_blocks if block_id]
                print(f"Error: No se pudieron descargar {len(failed_blocks)} bloques")
                print("Bloques fallidos:", failed_block_ids)
                
                # Verificar si tenemos suficientes bloques para continuar
                if len(block_data_map) / len(blocks) >= 0.9:  # Si tenemos al menos 90% de los bloques
                    print("Advertencia: Algunos bloques no pudieron ser recuperados, pero intentaremos reconstruir el archivo con los bloques disponibles.")
                else:
                    return False
            
            # Escribir los bloques en orden al archivo local
            print("Escribiendo bloques al archivo local...")
            with open(local_path, 'wb') as f:
                for block in blocks:
                    block_id = block.get('block_id')
                    if block_id in block_data_map:
                        f.write(block_data_map[block_id])
                    else:
                        print(f"Advertencia: Bloque {block_id} no disponible, se reemplazará con datos vacíos")
                        # Reemplazar con datos vacíos del tamaño aproximado del bloque
                        if 'size' in block:
                            f.write(b'\0' * block.get('size', 0))
            
            print(f"Archivo descargado exitosamente en {local_path}")
            if failed_blocks:
                print("Advertencia: El archivo puede estar incompleto o corrupto debido a bloques faltantes.")
                return False
            return True
            
        except Exception as e:
            print(f"Error al descargar el archivo: {e}")
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

    def _split_file_into_blocks(self, file_path: str) -> List[Dict]:
        """
        Divide un archivo en bloques.
        
        Args:
            file_path: Ruta al archivo a dividir
            
        Returns:
            Lista de diccionarios con información de los bloques
        """
        blocks = []
        file_size = os.path.getsize(file_path)
        
        # Si el archivo es más pequeño que el tamaño de bloque, crear un solo bloque
        if file_size <= self.block_size:
            blocks.append({
                'offset': 0,
                'size': file_size,
                'block_id': str(uuid.uuid4())
            })
            return blocks
        
        # Dividir el archivo en bloques
        offset = 0
        while offset < file_size:
            remaining = file_size - offset
            block_size = min(remaining, self.block_size)
            
            blocks.append({
                'offset': offset,
                'size': block_size,
                'block_id': str(uuid.uuid4())
            })
            
            offset += block_size
        
        return blocks

    def _upload_block(self, file_path: str, block: Dict, datanodes: List[Dict], is_leader: bool = False) -> bool:
        """
        Sube un bloque a los DataNodes usando gRPC.
        
        Args:
            file_path: Ruta al archivo
            block: Información del bloque
            datanodes: Lista de DataNodes donde subir el bloque
            is_leader: Si es True, el DataNode es el líder
            
        Returns:
            True si la subida fue exitosa
        """
        try:
            # Leer el contenido del bloque
            with open(file_path, 'rb') as f:
                f.seek(block['offset'])
                data = f.read(block['size'])
            
            # Calcular checksum
            checksum = hashlib.md5(data).hexdigest()
            
            # Primero, crear el bloque en el NameNode
            try:
                # Preparar la información del bloque para el NameNode
                block_info = {
                    'block_id': block['block_id'],
                    'file_id': block.get('file_id', ''),  # Asegurarse de que file_id esté disponible
                    'size': block['size'],
                    'checksum': checksum,
                    'locations': []
                }
                
                # Registrar el bloque en el NameNode
                self.namenode_client._make_request('post', '/blocks/', block_info)
            except Exception as e:
                self.logger.error(f"Error registrando bloque en NameNode: {e}")
                # Continuamos con el intento de subida, ya que el error podría ser que el bloque ya existe
            
            # Subir el bloque a cada DataNode usando gRPC
            for datanode in datanodes:
                try:
                    with DataNodeClient(datanode['hostname'], datanode['port']) as datanode_client:
                        success = datanode_client.store_block(block['block_id'], data)
                        
                        if not success:
                            self.logger.error(f"Error uploading block to DataNode {datanode['node_id']}")
                            return False
                        
                        # Registrar la ubicación del bloque en el NameNode
                        self.namenode_client.add_block_location(
                            block['block_id'],
                            datanode['node_id'],
                            is_leader=is_leader
                        )
                except Exception as e:
                    self.logger.error(f"Error uploading block to DataNode {datanode['node_id']}: {e}")
                    return False
            
            return True
        except Exception as e:
            self.logger.error(f"Error uploading block: {e}")
            return False

    def download_block(self, block_info):
        block_id = block_info.get('block_id')
        locations = block_info.get('locations', [])
        
        if not locations:
            print(f"Error: No hay ubicaciones disponibles para el bloque {block_id}")
            # Intentar obtener información actualizada del bloque directamente
            try:
                updated_block_info = self.namenode_client.get_block_info(block_id)
                if updated_block_info and updated_block_info.get('locations'):
                    locations = updated_block_info.get('locations')
                    print(f"Recuperadas {len(locations)} ubicaciones para el bloque {block_id}")
                else:
                    print(f"No se pudieron recuperar ubicaciones para el bloque {block_id}")
                    return False, block_id, None
            except Exception as e:
                print(f"Error al actualizar información del bloque {block_id}: {e}")
                return False, block_id, None
        
        # Intentar descargar de cada ubicación hasta que una funcione
        errors = []
        for location in locations:
            try:
                # Obtener información completa del DataNode
                datanode_id = location.get('datanode_id')
                hostname = location.get('hostname')
                port = location.get('port')
                
                if not datanode_id:
                    errors.append("DataNode ID no disponible en la ubicación del bloque")
                    continue
                
                if not hostname or not port:
                    # Si no tenemos la información completa, intentar obtenerla del NameNode
                    try:
                        datanode_info = self.namenode_client.get_datanode(datanode_id)
                        if not datanode_info:
                            errors.append(f"No se pudo obtener información del DataNode {datanode_id}")
                            continue
                        
                        hostname = datanode_info.get('hostname')
                        port = datanode_info.get('port')
                        
                        if not hostname or not port:
                            errors.append(f"Información incompleta del DataNode {datanode_id}")
                            continue
                    except Exception as e:
                        errors.append(f"Error al obtener información del DataNode {datanode_id}: {e}")
                        continue
                
                # Intentar descargar el bloque
                print(f"Intentando descargar el bloque {block_id} desde {hostname}:{port}")
                with DataNodeClient(hostname, port) as datanode:
                    block_data = datanode.retrieve_block(block_id)
                    if block_data:
                        print(f"Bloque {block_id} descargado correctamente ({len(block_data)} bytes)")
                        return True, block_id, block_data
                    else:
                        errors.append(f"El DataNode {datanode_id} ({hostname}:{port}) devolvió datos vacíos")
            except Exception as e:
                errors.append(f"Error con DataNode {datanode_id} ({hostname}:{port}): {str(e)}")
                continue
        
        print(f"No se pudo descargar el bloque {block_id}. Errores: {'; '.join(errors)}")
        return False, block_id, None
