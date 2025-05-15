import requests
import time
import logging
from typing import Dict, Optional

class DataNodeRegistration:
    def __init__(self, 
                 namenode_url: str, 
                 node_id: str, 
                 hostname: str, 
                 port: int,
                 storage_capacity: int,
                 registration_interval: int = 60):
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
            
            if response.status_code == 200:
                self.is_registered = True
                self.logger.info(f"DataNode {self.node_id} registered successfully with NameNode")
                return True
            else:
                self.logger.error(f"Failed to register DataNode: {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Error registering DataNode: {str(e)}")
            return False
    
    def heartbeat(self, available_space: int, blocks: Dict) -> bool:
        try:
            heartbeat_data = {
                "node_id": self.node_id,
                "available_space": available_space,
                "blocks": blocks
            }
            
            response = requests.post(
                f"{self.namenode_url}/datanodes/heartbeat",
                json=heartbeat_data
            )
            
            if response.status_code == 200:
                return True
            else:
                self.logger.error(f"Failed to send heartbeat: {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Error sending heartbeat: {str(e)}")
            return False
    
    def start_heartbeat_thread(self, get_storage_stats_func) -> None:
        import threading
        
        def heartbeat_loop():
            while True:
                try:
                    if not self.is_registered:
                        self.register()
                    
                    if self.is_registered:
                        stats = get_storage_stats_func()
                        self.heartbeat(stats["available_space"], stats["blocks"])
                    
                    time.sleep(self.registration_interval)
                except Exception as e:
                    self.logger.error(f"Error in heartbeat loop: {str(e)}")
                    time.sleep(self.registration_interval)
        
        heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        return heartbeat_thread
