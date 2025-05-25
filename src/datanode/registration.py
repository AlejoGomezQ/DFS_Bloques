import requests
import time
import logging
import json
from typing import Dict, Optional, List, Callable

class DataNodeRegistration:
    def __init__(self, 
                 namenode_url: str, 
                 node_id: str, 
                 hostname: str, 
                 port: int,
                 storage_capacity: int,
                 registration_interval: int = 10):
        self.namenode_url = namenode_url
        self.node_id = node_id
        self.hostname = hostname
        self.port = port
        self.storage_capacity = storage_capacity
        self.registration_interval = registration_interval
        self.is_registered = False
        self.logger = logging.getLogger("DataNodeRegistration")
    
    def register(self) -> bool:
        try:
            registration_data = {
                "node_id": self.node_id,
                "hostname": self.hostname,
                "port": self.port,
                "storage_capacity": self.storage_capacity,
                "available_space": self.storage_capacity
            }
            
            response = requests.post(
                f"{self.namenode_url}/datanodes/register",
                json=registration_data
            )
            
            # El NameNode devuelve un código 201 (Created) cuando se registra un nuevo DataNode
            # y devuelve un objeto DataNodeInfo con los detalles del DataNode registrado
            if response.status_code == 201:
                datanode_info = response.json()
                # Actualizar el node_id si el NameNode asignó uno nuevo
                if "node_id" in datanode_info:
                    self.node_id = datanode_info["node_id"]
                
                self.is_registered = True
                self.logger.info(f"DataNode {self.node_id} registered successfully with NameNode")
                return True
            else:
                self.logger.error(f"Failed to register DataNode: Status {response.status_code} - {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Error registering DataNode: {str(e)}")
            return False
    
    def heartbeat(self, available_space: int, blocks: Dict) -> bool:
        try:
            # Preparar los datos del heartbeat con el formato esperado por el NameNode
            blocks_info = {}
            for block_id, block_data in blocks.items():
                blocks_info[block_id] = {
                    "block_id": block_id,
                    "size": block_data.get("size", 0),
                    "checksum": block_data.get("checksum", "")
                }
            
            heartbeat_data = {
                "node_id": self.node_id,
                "available_space": available_space,
                "blocks": blocks_info
            }
            
            # Log para depuración
            self.logger.debug(f"Enviando heartbeat al NameNode para {self.node_id} con {len(blocks_info)} bloques")
            
            # Enviar heartbeat al NameNode
            response = requests.post(
                f"{self.namenode_url}/datanodes/{self.node_id}/heartbeat",
                json=heartbeat_data
            )
            
            # El NameNode devuelve 204 (No Content) cuando el heartbeat se procesa correctamente
            if response.status_code == 204:  # No Content
                self.logger.debug(f"Heartbeat enviado exitosamente al NameNode")
                return True
            else:
                self.logger.error(f"Error al enviar heartbeat: Código {response.status_code} - {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Error al enviar heartbeat: {str(e)}")
            return False
    
    def start_heartbeat_thread(self, get_storage_stats_func: Callable) -> None:
        import threading
        
        def heartbeat_loop():
            while True:
                try:
                    if not self.is_registered:
                        self.logger.info(f"Attempting to register DataNode {self.node_id} with NameNode")
                        self.register()
                    
                    if self.is_registered:
                        # Obtener estadísticas de almacenamiento y bloques
                        stats = get_storage_stats_func()
                        
                        # Preparar información de bloques para el heartbeat
                        blocks_info = {}
                        for block_id in stats["blocks"]:
                            block_size = stats.get("block_sizes", {}).get(block_id, 0)
                            block_checksum = stats.get("block_checksums", {}).get(block_id, "")
                            
                            blocks_info[block_id] = {
                                "size": block_size,
                                "checksum": block_checksum
                            }
                        
                        # Enviar heartbeat
                        self.logger.info(f"Sending heartbeat for DataNode {self.node_id} with {len(blocks_info)} blocks")
                        success = self.heartbeat(stats["available_space"], blocks_info)
                        
                        if success:
                            self.logger.info(f"Heartbeat sent successfully")
                        else:
                            self.logger.warning(f"Failed to send heartbeat")
                    
                    time.sleep(self.registration_interval)
                except Exception as e:
                    self.logger.error(f"Error in heartbeat loop: {str(e)}")
                    time.sleep(self.registration_interval)
        
        self.logger.info(f"Starting heartbeat thread for DataNode {self.node_id}")
        heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        return heartbeat_thread

    def send_heartbeat(self):
        """
        Envía un heartbeat al NameNode con información actualizada del DataNode.
        """
        try:
            # Obtener estadísticas de almacenamiento
            storage_stats = self.storage_manager.get_storage_stats()
            
            # Preparar información de bloques
            blocks_info = {}
            for block_id, block_data in self.storage_manager.blocks.items():
                blocks_info[block_id] = {
                    'size': block_data['size'],
                    'checksum': block_data.get('checksum')
                }
            
            # Enviar heartbeat
            response = self.namenode_client.send_heartbeat(
                self.node_id,
                storage_stats['available_space'],
                blocks_info
            )
            
            self.logger.info("Heartbeat sent successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error sending heartbeat: {e}")
            return False
