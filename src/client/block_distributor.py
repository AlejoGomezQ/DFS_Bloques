from typing import List, Dict, Optional, Tuple
import random
from src.client.namenode_client import NameNodeClient


class BlockDistributor:
    """
    Clase encargada de distribuir los bloques entre los DataNodes disponibles.
    """
    def __init__(self, namenode_client: NameNodeClient, replication_factor: int = 3):
        """
        Inicializa el distribuidor de bloques.
        
        Args:
            namenode_client: Cliente para comunicarse con el NameNode
            replication_factor: Número de copias que se deben mantener de cada bloque
        """
        self.namenode_client = namenode_client
        self.replication_factor = replication_factor
    
    def select_datanodes_for_block(self, block_size: int, excluded_nodes: List[str] = None) -> List[Dict]:
        """
        Selecciona los DataNodes más adecuados para almacenar un bloque.
        
        Args:
            block_size: Tamaño del bloque en bytes
            excluded_nodes: Lista de IDs de DataNodes a excluir
            
        Returns:
            Lista de DataNodes seleccionados con su información
        """
        # Obtener todos los DataNodes activos
        datanodes = self.namenode_client.list_datanodes(status="active")
        
        if excluded_nodes:
            # Filtrar los DataNodes excluidos
            datanodes = [dn for dn in datanodes if dn.get('node_id') not in excluded_nodes]
        
        # Filtrar los DataNodes que tienen suficiente espacio disponible
        eligible_datanodes = [dn for dn in datanodes if dn.get('available_space', 0) >= block_size]
        
        if not eligible_datanodes:
            raise Exception("No hay DataNodes disponibles con suficiente espacio")
        
        # Si hay menos DataNodes que el factor de replicación, usar todos los disponibles
        num_nodes = min(self.replication_factor, len(eligible_datanodes))
        
        # Ordenar por espacio disponible (de mayor a menor) y agregar un factor aleatorio
        # para distribuir la carga de manera más uniforme
        eligible_datanodes.sort(key=lambda dn: (
            dn.get('available_space', 0) * (0.8 + 0.2 * random.random()),  # 20% de aleatoriedad
            random.random()  # Desempate aleatorio
        ), reverse=True)
        
        # Seleccionar los primeros nodos como candidatos
        selected_nodes = eligible_datanodes[:num_nodes]
        
        # Designar el primer nodo como líder
        for i, node in enumerate(selected_nodes):
            node['is_leader'] = (i == 0)
        
        return selected_nodes
    
    def get_alternative_datanodes(self, block_size: int, failed_nodes: List[str]) -> List[Dict]:
        """
        Obtiene DataNodes alternativos cuando fallan los principales.
        
        Args:
            block_size: Tamaño del bloque en bytes
            failed_nodes: Lista de IDs de DataNodes que han fallado
            
        Returns:
            Lista de DataNodes alternativos
        """
        try:
            return self.select_datanodes_for_block(block_size, excluded_nodes=failed_nodes)
        except Exception:
            return []
    
    def distribute_blocks(self, blocks: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Distribuye una lista de bloques entre los DataNodes disponibles.
        
        Args:
            blocks: Lista de bloques a distribuir
            
        Returns:
            Diccionario que mapea cada block_id a una lista de DataNodes seleccionados
        """
        block_distribution = {}
        
        for block in blocks:
            try:
                selected_nodes = self.select_datanodes_for_block(block['size'])
                block_distribution[block['block_id']] = selected_nodes
            except Exception as e:
                print(f"Error al distribuir el bloque {block['block_id']}: {e}")
                # Si no se puede distribuir un bloque, continuar con el siguiente
                continue
        
        return block_distribution
    
    def get_optimal_block_size(self) -> int:
        """
        Determina el tamaño óptimo de bloque basado en la configuración del sistema.
        
        Returns:
            Tamaño óptimo de bloque en bytes
        """
        # Por defecto, usar 4MB como tamaño de bloque
        default_size = 4 * 1024 * 1024
        
        try:
            # Obtener información de los DataNodes para tomar una decisión más informada
            datanodes = self.namenode_client.list_datanodes(status="active")
            
            if not datanodes:
                return default_size
            
            # Calcular el espacio promedio disponible
            avg_space = sum(dn.get('available_space', 0) for dn in datanodes) / len(datanodes)
            
            # Si el espacio promedio es muy pequeño, reducir el tamaño del bloque
            if avg_space < default_size * 10:  # Si hay menos de 10 bloques de espacio
                return min(default_size, int(avg_space / 4))  # Usar 1/4 del espacio promedio
            
            return default_size
        except Exception:
            return default_size
