import requests
from typing import Dict, List, Optional, Any, Union
import json

class NameNodeClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        url = f"{self.base_url}{endpoint}"
        
        if method.lower() == 'get':
            response = requests.get(url)
        elif method.lower() == 'post':
            response = requests.post(url, json=data)
        elif method.lower() == 'put':
            response = requests.put(url, json=data)
        elif method.lower() == 'delete':
            response = requests.delete(url)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        if response.status_code >= 400:
            error_message = f"Error {response.status_code}: {response.text}"
            raise Exception(error_message)
        
        if response.status_code == 204:  # No content
            return {}
        
        return response.json()
    
    # Operaciones de archivos
    def create_file(self, file_metadata: Dict) -> Dict:
        return self._make_request('post', '/files/', data=file_metadata)
    
    def get_file(self, file_id: str) -> Dict:
        return self._make_request('get', f'/files/{file_id}')
    
    def get_file_by_path(self, path: str) -> Dict:
        return self._make_request('get', f'/files/path/{path}')
    
    def delete_file(self, file_id: str) -> None:
        self._make_request('delete', f'/files/{file_id}')
    
    # Operaciones de bloques
    def get_block_info(self, block_id: str) -> Dict:
        """
        Obtiene información detallada de un bloque, incluyendo sus ubicaciones.
        
        Args:
            block_id: ID del bloque
            
        Returns:
            Diccionario con información detallada del bloque o un diccionario vacío si hay error
        """
        try:
            response = self._make_request('get', f'/blocks/{block_id}')
            
            # Asegurarse de que la respuesta contiene información de ubicaciones completa
            if 'locations' in response:
                # Verificar que cada ubicación tiene información completa del DataNode
                for i, location in enumerate(response['locations']):
                    if not all(k in location for k in ['hostname', 'port']):
                        # Si falta información, intentar obtener el DataNode completo
                        try:
                            datanode_id = location.get('datanode_id')
                            if datanode_id:
                                datanode_info = self.get_datanode(datanode_id)
                                if datanode_info:
                                    # Actualizar información en la ubicación
                                    response['locations'][i].update({
                                        'hostname': datanode_info.get('hostname'),
                                        'port': datanode_info.get('port'),
                                        'status': datanode_info.get('status')
                                    })
                        except Exception:
                            # Si no se puede obtener la información completa, continuar con la siguiente ubicación
                            continue
            
            return response
        except Exception as e:
            print(f"Error al obtener información del bloque {block_id}: {e}")
            return {}
    
    def add_block_location(self, block_id: str, datanode_id: str, is_leader: bool = False) -> Dict:
        """
        Registra la ubicación de un bloque en un DataNode específico.
        
        Args:
            block_id: ID del bloque
            datanode_id: ID del DataNode donde se almacena el bloque
            is_leader: Indica si este DataNode es el líder para este bloque
            
        Returns:
            Información del bloque actualizada
        """
        location = {
            'block_id': block_id,
            'datanode_id': datanode_id,
            'is_leader': is_leader
        }
        try:
            return self._make_request('post', f'/blocks/{block_id}/locations', data=location)
        except Exception as e:
            # Si el error es 404 (bloque no encontrado), intentamos crear el bloque primero
            if "404" in str(e) and "Block not found" in str(e):
                print(f"El bloque {block_id} no existe en el NameNode. Intente registrar el bloque primero.")
            raise e
    
    def get_file_blocks(self, path: str) -> List[Dict]:
        """
        Obtiene información de los bloques de un archivo.
        
        Args:
            path: Ruta del archivo en el DFS
            
        Returns:
            Lista de diccionarios con información de los bloques
        """
        try:
            # Primero obtener la información del archivo para verificar que existe
            file_info = self.get_file_info(path)
            if not file_info:
                return []
            
            # Obtener los bloques y sus ubicaciones
            response = self._make_request('get', f'/files/blocks/{path}')
            if isinstance(response, dict) and 'blocks' in response:
                blocks = response['blocks']
            elif isinstance(response, list):
                blocks = response
            else:
                blocks = []
            
            # Para cada bloque, obtener información detallada
            detailed_blocks = []
            for block in blocks:
                block_id = block.get('block_id')
                if block_id:
                    try:
                        block_info = self.get_block_info(block_id)
                        if block_info:
                            detailed_blocks.append(block_info)
                    except Exception as e:
                        print(f"Error al obtener información del bloque {block_id}: {e}")
                        continue
            
            return detailed_blocks
        except Exception as e:
            print(f"Error al obtener información de los bloques: {e}")
            return []
    
    def report_block_status(self, block_reports: List[Dict]) -> None:
        self._make_request('post', '/blocks/report', data=block_reports)
    
    # Operaciones de DataNodes
    def register_datanode(self, registration: Dict) -> Dict:
        return self._make_request('post', '/datanodes/register', data=registration)
    
    def list_datanodes(self, status: Optional[str] = None) -> List[Dict]:
        """
        Obtiene la lista de DataNodes y su información.
        
        Args:
            status: Estado de los DataNodes a filtrar (opcional)
            
        Returns:
            Lista de diccionarios con información de los DataNodes
        """
        try:
            endpoint = '/datanodes'
            if status:
                endpoint += f'?status={status}'
            response = self._make_request('get', endpoint)
            return response if isinstance(response, list) else []
        except Exception as e:
            print(f"Error al obtener lista de DataNodes: {e}")
            return []
    
    def get_datanode(self, node_id: str) -> Dict:
        return self._make_request('get', f'/datanodes/{node_id}')
    
    def datanode_heartbeat(self, node_id: str, available_space: int) -> None:
        self._make_request('post', f'/datanodes/{node_id}/heartbeat', data={'available_space': available_space})
    
    # Operaciones de directorios
    def create_directory(self, directory: Dict) -> Dict:
        return self._make_request('post', '/directories/', data=directory)
    
    def list_directory(self, path: str) -> Dict:
        return self._make_request('get', f'/directories/{path}')
    
    def delete_directory(self, path: str, recursive: bool = False) -> None:
        endpoint = f'/directories/{path}'
        if recursive:
            endpoint += '?recursive=true'
        self._make_request('delete', endpoint)

    def get_file_info(self, path: str) -> Optional[Dict]:
        """
        Obtiene información detallada de un archivo.
        
        Args:
            path: Ruta del archivo en el DFS
            
        Returns:
            Diccionario con la información del archivo o None si no existe
        """
        try:
            # Obtener información detallada, incluyendo ubicaciones de bloques en DataNodes activos
            response = self._make_request('get', f'/files/info/{path}')
            if isinstance(response, dict):
                # Verificar si hay bloques sin ubicaciones disponibles
                if 'blocks' in response:
                    for block in response['blocks']:
                        if 'locations' in block and len(block['locations']) == 0:
                            print(f"Advertencia: El bloque {block['block_id']} no tiene ubicaciones disponibles")
                return response
            return None
        except Exception as e:
            print(f"Error al obtener información del archivo: {e}")
            return None

    def get_system_stats(self) -> Dict:
        """
        Obtiene estadísticas generales del sistema.
        
        Returns:
            Diccionario con estadísticas del sistema
        """
        try:
            response = self._make_request('get', '/system/stats')
            return response if response else {
                'namenode_active': False,
                'total_files': 0,
                'total_blocks': 0,
                'replication_factor': 2
            }
        except Exception as e:
            print(f"Error al obtener estadísticas del sistema: {e}")
            return {
                'namenode_active': False,
                'total_files': 0,
                'total_blocks': 0,
                'replication_factor': 2
            }
