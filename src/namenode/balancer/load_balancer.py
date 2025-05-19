"""
Módulo para el balanceo de carga entre DataNodes.
Implementa algoritmos para redistribuir bloques y equilibrar el uso de recursos.
"""
import logging
import time
import threading
import random
from typing import List, Dict, Any, Tuple, Optional, Set
import heapq

from src.namenode.metadata.manager import MetadataManager
from src.common.proto import datanode_pb2_grpc, datanode_pb2
import grpc

logger = logging.getLogger("LoadBalancer")

class LoadBalancer:
    """
    Clase encargada de balancear la carga entre DataNodes.
    Implementa algoritmos para redistribuir bloques y equilibrar el uso de recursos.
    """
    
    def __init__(self, metadata_manager: MetadataManager, 
                 balance_threshold: float = 0.2,
                 check_interval: int = 300,
                 auto_balance: bool = True):
        """
        Inicializa el balanceador de carga.
        
        Args:
            metadata_manager: Gestor de metadatos del NameNode
            balance_threshold: Umbral de desbalance para iniciar la redistribución (0.0-1.0)
            check_interval: Intervalo en segundos para verificar el balance
            auto_balance: Si es True, se ejecuta automáticamente el balanceo
        """
        self.metadata_manager = metadata_manager
        self.balance_threshold = balance_threshold
        self.check_interval = check_interval
        self.auto_balance = auto_balance
        self.running = False
        self.balancing_thread = None
        self.last_balance_time = 0
        self.balance_in_progress = False
        self.blocks_moved = 0
        self.total_blocks_to_move = 0
        
    def start(self):
        """Inicia el servicio de balanceo automático."""
        if self.auto_balance and not self.running:
            self.running = True
            self.balancing_thread = threading.Thread(
                target=self._balance_loop,
                daemon=True
            )
            self.balancing_thread.start()
            logger.info("Load balancer service started")
    
    def stop(self):
        """Detiene el servicio de balanceo automático."""
        self.running = False
        if self.balancing_thread:
            self.balancing_thread.join(timeout=1.0)
            logger.info("Load balancer service stopped")
    
    def _balance_loop(self):
        """Bucle principal para el balanceo automático."""
        while self.running:
            try:
                # Verificar si es necesario balancear
                if self._should_rebalance():
                    self.balance_datanodes()
                
                # Esperar hasta el próximo ciclo
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in load balancer loop: {str(e)}")
                time.sleep(60)  # Esperar un minuto en caso de error
    
    def _should_rebalance(self) -> bool:
        """
        Determina si es necesario realizar un rebalanceo.
        
        Returns:
            True si se debe rebalancear, False en caso contrario
        """
        # No rebalancear si ya hay un balance en progreso
        if self.balance_in_progress:
            return False
        
        # No rebalancear si se hizo recientemente
        if time.time() - self.last_balance_time < self.check_interval:
            return False
        
        # Obtener información de uso de los DataNodes
        datanodes = self.metadata_manager.get_active_datanodes()
        if len(datanodes) < 2:
            return False  # Se necesitan al menos 2 nodos para balancear
        
        # Calcular el uso promedio y la desviación
        usage_percentages = [self._calculate_usage_percentage(dn) for dn in datanodes]
        avg_usage = sum(usage_percentages) / len(usage_percentages)
        
        # Verificar si algún nodo está fuera del umbral
        for usage in usage_percentages:
            if abs(usage - avg_usage) > self.balance_threshold:
                return True
        
        return False
    
    def _calculate_usage_percentage(self, datanode: Dict) -> float:
        """
        Calcula el porcentaje de uso de un DataNode.
        
        Args:
            datanode: Información del DataNode
            
        Returns:
            Porcentaje de uso (0.0-1.0)
        """
        storage_capacity = datanode.get('storage_capacity', 0)
        if storage_capacity <= 0:
            return 0.0
        
        available_space = datanode.get('available_space', 0)
        used_space = storage_capacity - available_space
        
        return used_space / storage_capacity
    
    def get_balance_status(self) -> Dict[str, Any]:
        """
        Obtiene el estado actual del balanceo.
        
        Returns:
            Diccionario con información del estado del balanceo
        """
        datanodes = self.metadata_manager.get_active_datanodes()
        
        # Calcular estadísticas de uso
        usage_stats = {}
        for dn in datanodes:
            node_id = dn.get('node_id', 'unknown')
            usage_stats[node_id] = {
                'usage_percentage': self._calculate_usage_percentage(dn),
                'total_blocks': dn.get('total_blocks', 0),
                'available_space': dn.get('available_space', 0),
                'storage_capacity': dn.get('storage_capacity', 0)
            }
        
        # Calcular desviación estándar de uso
        if usage_stats:
            usage_values = [stats['usage_percentage'] for stats in usage_stats.values()]
            avg_usage = sum(usage_values) / len(usage_values)
            variance = sum((x - avg_usage) ** 2 for x in usage_values) / len(usage_values)
            std_dev = variance ** 0.5
        else:
            avg_usage = 0.0
            std_dev = 0.0
        
        return {
            'datanodes': usage_stats,
            'average_usage': avg_usage,
            'std_deviation': std_dev,
            'balance_in_progress': self.balance_in_progress,
            'blocks_moved': self.blocks_moved,
            'total_blocks_to_move': self.total_blocks_to_move,
            'last_balance_time': self.last_balance_time,
            'is_balanced': std_dev <= self.balance_threshold
        }
    
    def balance_datanodes(self) -> Dict[str, Any]:
        """
        Inicia el proceso de balanceo de carga entre DataNodes.
        
        Returns:
            Diccionario con el resultado del balanceo
        """
        if self.balance_in_progress:
            return {'status': 'error', 'message': 'Balance already in progress'}
        
        try:
            self.balance_in_progress = True
            self.blocks_moved = 0
            
            # Obtener información de los DataNodes
            datanodes = self.metadata_manager.get_active_datanodes()
            if len(datanodes) < 2:
                self.balance_in_progress = False
                return {'status': 'error', 'message': 'Need at least 2 active DataNodes to balance'}
            
            # Clasificar nodos por uso
            overloaded_nodes, underloaded_nodes = self._classify_nodes(datanodes)
            
            if not overloaded_nodes or not underloaded_nodes:
                self.balance_in_progress = False
                return {'status': 'success', 'message': 'System is already balanced', 'blocks_moved': 0}
            
            # Planificar movimientos de bloques
            move_plan = self._create_move_plan(overloaded_nodes, underloaded_nodes)
            self.total_blocks_to_move = len(move_plan)
            
            if not move_plan:
                self.balance_in_progress = False
                return {'status': 'success', 'message': 'No blocks need to be moved', 'blocks_moved': 0}
            
            # Ejecutar el plan de movimiento
            result = self._execute_move_plan(move_plan)
            
            # Actualizar tiempo del último balanceo
            self.last_balance_time = time.time()
            self.balance_in_progress = False
            
            return {
                'status': 'success',
                'message': f'Balance completed. {result["blocks_moved"]} blocks moved.',
                'blocks_moved': result['blocks_moved'],
                'failed_moves': result['failed_moves']
            }
            
        except Exception as e:
            logger.error(f"Error during load balancing: {str(e)}")
            self.balance_in_progress = False
            return {'status': 'error', 'message': f'Error during balancing: {str(e)}'}
    
    def _classify_nodes(self, datanodes: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Clasifica los nodos en sobrecargados y subcargados.
        
        Args:
            datanodes: Lista de información de DataNodes
            
        Returns:
            Tupla de (nodos sobrecargados, nodos subcargados)
        """
        # Calcular el uso promedio
        usage_percentages = [self._calculate_usage_percentage(dn) for dn in datanodes]
        avg_usage = sum(usage_percentages) / len(usage_percentages)
        
        # Clasificar nodos
        overloaded = []
        underloaded = []
        
        for dn in datanodes:
            usage = self._calculate_usage_percentage(dn)
            # Añadir información de uso al nodo para referencia
            dn['usage_percentage'] = usage
            
            if usage > avg_usage + self.balance_threshold:
                overloaded.append(dn)
            elif usage < avg_usage - self.balance_threshold:
                underloaded.append(dn)
        
        # Ordenar por uso (mayor a menor para sobrecargados, menor a mayor para subcargados)
        overloaded.sort(key=lambda x: x['usage_percentage'], reverse=True)
        underloaded.sort(key=lambda x: x['usage_percentage'])
        
        return overloaded, underloaded
    
    def _create_move_plan(self, overloaded_nodes: List[Dict], underloaded_nodes: List[Dict]) -> List[Dict]:
        """
        Crea un plan para mover bloques de nodos sobrecargados a subcargados.
        
        Args:
            overloaded_nodes: Lista de nodos sobrecargados
            underloaded_nodes: Lista de nodos subcargados
            
        Returns:
            Lista de movimientos a realizar (block_id, source_node, target_node)
        """
        move_plan = []
        
        # Para cada nodo sobrecargado
        for source_node in overloaded_nodes:
            source_id = source_node['node_id']
            
            # Obtener bloques almacenados en este nodo
            blocks = self.metadata_manager.get_blocks_by_datanode(source_id)
            
            # Calcular cuántos bloques mover para equilibrar
            blocks_to_move = self._calculate_blocks_to_move(source_node, blocks)
            
            # Seleccionar bloques para mover
            selected_blocks = self._select_blocks_to_move(blocks, blocks_to_move)
            
            # Asignar bloques a nodos destino
            for block in selected_blocks:
                # Encontrar el nodo con menor carga que no tenga ya este bloque
                target_node = self._find_best_target_node(block, underloaded_nodes)
                
                if target_node:
                    move_plan.append({
                        'block_id': block['block_id'],
                        'source_node': source_node,
                        'target_node': target_node
                    })
        
        return move_plan
    
    def _calculate_blocks_to_move(self, node: Dict, blocks: List[Dict]) -> int:
        """
        Calcula cuántos bloques deben moverse de un nodo sobrecargado.
        
        Args:
            node: Información del nodo sobrecargado
            blocks: Lista de bloques en el nodo
            
        Returns:
            Número de bloques a mover
        """
        # Obtener información de todos los nodos
        all_nodes = self.metadata_manager.get_active_datanodes()
        
        # Calcular el uso promedio objetivo
        total_usage = sum(self._calculate_usage_percentage(dn) for dn in all_nodes)
        avg_usage = total_usage / len(all_nodes)
        
        # Calcular cuánto espacio liberar para llegar cerca del promedio
        current_usage = node['usage_percentage']
        target_usage = avg_usage + (self.balance_threshold / 2)  # Un poco por encima del promedio
        
        if current_usage <= target_usage:
            return 0  # No es necesario mover bloques
        
        # Estimar el espacio a liberar
        storage_capacity = node.get('storage_capacity', 0)
        space_to_free = storage_capacity * (current_usage - target_usage)
        
        # Estimar el tamaño promedio de bloque
        if not blocks:
            return 0
        
        avg_block_size = sum(block.get('size', 0) for block in blocks) / len(blocks)
        
        if avg_block_size <= 0:
            return 0
        
        # Calcular número de bloques a mover
        blocks_to_move = int(space_to_free / avg_block_size)
        
        # Limitar al 25% de los bloques como máximo por ciclo
        max_blocks = len(blocks) // 4
        return min(blocks_to_move, max_blocks) if max_blocks > 0 else blocks_to_move
    
    def _select_blocks_to_move(self, blocks: List[Dict], count: int) -> List[Dict]:
        """
        Selecciona los bloques más adecuados para mover.
        Prioriza bloques que ya están replicados y de mayor tamaño.
        
        Args:
            blocks: Lista de bloques disponibles
            count: Número de bloques a seleccionar
            
        Returns:
            Lista de bloques seleccionados
        """
        if count <= 0:
            return []
        
        # Filtrar bloques que tienen al menos una réplica
        # (para evitar mover bloques únicos que podrían perderse)
        replicated_blocks = []
        for block in blocks:
            block_locations = self.metadata_manager.get_block_locations(block['block_id'])
            if len(block_locations) > 1:
                replicated_blocks.append(block)
        
        # Si no hay suficientes bloques replicados, usar bloques normales
        if len(replicated_blocks) < count:
            # Ordenar por tamaño (mayor a menor)
            sorted_blocks = sorted(blocks, key=lambda b: b.get('size', 0), reverse=True)
            return sorted_blocks[:count]
        else:
            # Ordenar bloques replicados por tamaño (mayor a menor)
            sorted_blocks = sorted(replicated_blocks, key=lambda b: b.get('size', 0), reverse=True)
            return sorted_blocks[:count]
    
    def _find_best_target_node(self, block: Dict, candidate_nodes: List[Dict]) -> Optional[Dict]:
        """
        Encuentra el mejor nodo destino para un bloque.
        
        Args:
            block: Información del bloque a mover
            candidate_nodes: Lista de nodos candidatos
            
        Returns:
            Mejor nodo destino o None si no hay candidatos adecuados
        """
        block_id = block['block_id']
        block_size = block.get('size', 0)
        
        # Obtener ubicaciones actuales del bloque
        current_locations = set(
            location['node_id'] 
            for location in self.metadata_manager.get_block_locations(block_id)
        )
        
        # Filtrar nodos que ya tienen el bloque
        available_nodes = [
            node for node in candidate_nodes 
            if node['node_id'] not in current_locations
        ]
        
        if not available_nodes:
            return None
        
        # Filtrar nodos con espacio suficiente
        nodes_with_space = [
            node for node in available_nodes
            if node.get('available_space', 0) >= block_size
        ]
        
        if not nodes_with_space:
            return None
        
        # Seleccionar el nodo con menor uso
        return min(nodes_with_space, key=lambda n: n['usage_percentage'])
    
    def _execute_move_plan(self, move_plan: List[Dict]) -> Dict[str, Any]:
        """
        Ejecuta el plan de movimiento de bloques.
        
        Args:
            move_plan: Lista de movimientos a realizar
            
        Returns:
            Resultado de la ejecución del plan
        """
        blocks_moved = 0
        failed_moves = []
        
        for move in move_plan:
            block_id = move['block_id']
            source_node = move['source_node']
            target_node = move['target_node']
            
            logger.info(f"Moving block {block_id} from {source_node['node_id']} to {target_node['node_id']}")
            
            try:
                # Crear solicitud de transferencia
                transfer_request = {
                    'block_id': block_id,
                    'source_datanode_id': source_node['node_id'],
                    'source_hostname': source_node['hostname'],
                    'source_port': source_node['port'],
                    'target_datanode_id': target_node['node_id'],
                    'target_hostname': target_node['hostname'],
                    'target_port': target_node['port']
                }
                
                # Solicitar la transferencia al nodo origen
                success = self._request_block_transfer(transfer_request)
                
                if success:
                    # Actualizar metadatos si la transferencia fue exitosa
                    self.metadata_manager.add_block_location(block_id, target_node['node_id'])
                    blocks_moved += 1
                else:
                    failed_moves.append(block_id)
                    logger.warning(f"Failed to move block {block_id}")
            
            except Exception as e:
                failed_moves.append(block_id)
                logger.error(f"Error moving block {block_id}: {str(e)}")
        
        return {
            'blocks_moved': blocks_moved,
            'failed_moves': failed_moves
        }
    
    def _request_block_transfer(self, transfer_request: Dict) -> bool:
        """
        Solicita la transferencia de un bloque a un DataNode.
        
        Args:
            transfer_request: Información de la transferencia
            
        Returns:
            True si la transferencia fue exitosa, False en caso contrario
        """
        try:
            # Establecer conexión con el DataNode origen
            source_hostname = transfer_request['source_hostname']
            source_port = transfer_request['source_port']
            
            channel = grpc.insecure_channel(f"{source_hostname}:{source_port}")
            stub = datanode_pb2_grpc.DataNodeServiceStub(channel)
            
            # Crear solicitud de transferencia
            request = datanode_pb2.TransferRequest(
                block_id=transfer_request['block_id'],
                source_datanode_id=transfer_request['source_datanode_id'],
                source_hostname=transfer_request['source_hostname'],
                source_port=transfer_request['source_port'],
                target_datanode_id=transfer_request['target_datanode_id'],
                target_hostname=transfer_request['target_hostname'],
                target_port=transfer_request['target_port']
            )
            
            # Enviar solicitud
            response = stub.TransferBlock(request)
            
            # Cerrar canal
            channel.close()
            
            # Verificar respuesta
            return response.status == datanode_pb2.BlockResponse.SUCCESS
            
        except Exception as e:
            logger.error(f"Error requesting block transfer: {str(e)}")
            return False
