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
        return self._make_request('get', f'/blocks/{block_id}')
    
    def get_file_blocks(self, file_id: str) -> List[Dict]:
        return self._make_request('get', f'/blocks/file/{file_id}')
    
    def report_block_status(self, block_reports: List[Dict]) -> None:
        self._make_request('post', '/blocks/report', data=block_reports)
    
    # Operaciones de DataNodes
    def register_datanode(self, registration: Dict) -> Dict:
        return self._make_request('post', '/datanodes/register', data=registration)
    
    def list_datanodes(self, status: Optional[str] = None) -> List[Dict]:
        endpoint = '/datanodes/'
        if status:
            endpoint += f'?status={status}'
        return self._make_request('get', endpoint)
    
    def get_datanode(self, node_id: str) -> Dict:
        return self._make_request('get', f'/datanodes/{node_id}')
    
    def datanode_heartbeat(self, node_id: str, available_space: int) -> None:
        self._make_request('post', f'/datanodes/{node_id}/heartbeat', data={'available_space': available_space})
    
    # Operaciones de directorios
    def create_directory(self, directory: Dict) -> Dict:
        return self._make_request('post', '/directories/', data=directory)
    
    def list_directory(self, path: str) -> Dict:
        return self._make_request('get', f'/directories/{path}')
    
    def delete_directory(self, path: str) -> None:
        self._make_request('delete', f'/directories/{path}')
