import os
import uuid
from typing import List, Dict, Optional, BinaryIO, Tuple
import time

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
    
    def put_file(self, local_path: str, dfs_path: str) -> bool:
        """
        Sube un archivo al sistema de archivos distribuido.
        
        Args:
            local_path: Ruta local del archivo a subir
            dfs_path: Ruta destino en el DFS
            
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
            
            # Dividir el archivo en bloques
            print(f"Dividiendo el archivo {local_path} en bloques...")
            blocks = self.file_splitter.split_file(local_path)
            
            # Distribuir los bloques entre los DataNodes disponibles
            print(f"Seleccionando DataNodes para {len(blocks)} bloques...")
            block_distribution = self.block_distributor.distribute_blocks(blocks)
            
            # Crear el archivo en el NameNode
            file_size = os.path.getsize(local_path)
            file_metadata = {
                'name': file_name,
                'path': dfs_path,
                'type': 'file',
                'size': file_size
            }
            
            print(f"Registrando archivo {dfs_path} en el NameNode...")
            file_info = self.namenode_client.create_file(file_metadata)
            file_id = file_info.get('file_id')
            
            if not file_id:
                print("Error: No se pudo crear el archivo en el NameNode")
                return False
            
            # Subir cada bloque a los DataNodes seleccionados
            print(f"Subiendo bloques a los DataNodes...")
            for block in blocks:
                block_id = block['block_id']
                
                if block_id not in block_distribution:
                    print(f"Error: No hay DataNodes disponibles para el bloque {block_id}")
                    continue
                
                # Registrar el bloque en el NameNode
                self.namenode_client._make_request(
                    'post', 
                    '/blocks/', 
                    {
                        'block_id': block_id,
                        'file_id': file_id,
                        'size': block['size'],
                        'checksum': block['checksum']
                    }
                )
                
                # Subir el bloque a cada DataNode seleccionado
                for node_info in block_distribution[block_id]:
                    try:
                        # Conectar con el DataNode
                        with DataNodeClient(node_info['hostname'], node_info['port']) as datanode:
                            # Subir el bloque
                            success = datanode.store_block(block_id, block['data'])
                            
                            if success:
                                # Registrar la ubicación del bloque en el NameNode
                                self.namenode_client._make_request(
                                    'post',
                                    f'/blocks/{block_id}/locations',
                                    {
                                        'datanode_id': node_info['node_id'],
                                        'is_leader': node_info.get('is_leader', False)
                                    }
                                )
                            else:
                                print(f"Error al subir el bloque {block_id} al DataNode {node_info['node_id']}")
                    except Exception as e:
                        print(f"Error al comunicarse con el DataNode {node_info['node_id']}: {e}")
            
            print(f"Archivo {dfs_path} subido exitosamente")
            return True
            
        except Exception as e:
            print(f"Error al subir el archivo: {e}")
            return False
    
    def get_file(self, dfs_path: str, local_path: str) -> bool:
        """
        Descarga un archivo del sistema de archivos distribuido.
        
        Args:
            dfs_path: Ruta del archivo en el DFS
            local_path: Ruta local donde se guardará el archivo
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
        """
        try:
            # Obtener la información del archivo
            file_info = self.namenode_client.get_file_by_path(dfs_path)
            
            if not file_info:
                print(f"Error: El archivo {dfs_path} no existe en el DFS")
                return False
            
            file_id = file_info.get('file_id')
            
            # Obtener la información de los bloques
            blocks_info = self.namenode_client.get_file_blocks(file_id)
            
            if not blocks_info:
                print(f"Error: No se encontraron bloques para el archivo {dfs_path}")
                return False
            
            # Crear un directorio temporal para almacenar los bloques
            temp_dir = os.path.join(os.path.dirname(local_path), f".tmp_{uuid.uuid4()}")
            os.makedirs(temp_dir, exist_ok=True)
            
            # Descargar cada bloque
            blocks = []
            
            for block_info in blocks_info:
                block_id = block_info.get('block_id')
                block_locations = block_info.get('locations', [])
                
                if not block_locations:
                    print(f"Error: No hay ubicaciones disponibles para el bloque {block_id}")
                    continue
                
                # Intentar descargar el bloque de cualquier DataNode disponible
                block_data = None
                
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
                                break
                    except Exception as e:
                        print(f"Error al descargar el bloque {block_id} del DataNode {location.get('datanode_id')}: {e}")
                
                if not block_data:
                    print(f"Error: No se pudo descargar el bloque {block_id} de ningún DataNode")
                    continue
                
                # Guardar el bloque temporalmente
                block_path = os.path.join(temp_dir, block_id)
                with open(block_path, 'wb') as f:
                    f.write(block_data)
                
                # Añadir la información del bloque para reconstruir el archivo
                blocks.append({
                    'block_id': block_id,
                    'data': block_data,
                    'index': blocks_info.index(block_info),
                    'size': block_info.get('size', 0),
                    'checksum': block_info.get('checksum')
                })
            
            # Reconstruir el archivo
            if not blocks:
                print(f"Error: No se pudieron descargar los bloques del archivo {dfs_path}")
                return False
            
            # Ordenar los bloques por índice
            blocks.sort(key=lambda x: x['index'])
            
            # Crear el directorio destino si no existe
            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
            
            # Escribir el archivo
            with open(local_path, 'wb') as f:
                for block in blocks:
                    f.write(block['data'])
            
            # Limpiar los archivos temporales
            for block in blocks:
                block_path = os.path.join(temp_dir, block['block_id'])
                if os.path.exists(block_path):
                    os.remove(block_path)
            
            os.rmdir(temp_dir)
            
            print(f"Archivo {dfs_path} descargado exitosamente a {local_path}")
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
