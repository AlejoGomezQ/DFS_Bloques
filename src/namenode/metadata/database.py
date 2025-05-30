import sqlite3
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import uuid
from datetime import datetime
import logging

class MetadataDatabase:
    def __init__(self, db_path: str = None):
        if db_path is None:
            # Usar un directorio predeterminado para la base de datos
            base_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent.parent
            db_dir = base_dir / "data" / "namenode"
            os.makedirs(db_dir, exist_ok=True)
            db_path = str(db_dir / "metadata.db")
        
        self.db_path = db_path
        self.connection = None
        self.initialize_database()
    
    def get_connection(self):
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
        return self.connection
    
    def close_connection(self):
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def initialize_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Tabla para almacenar información de DataNodes
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS datanodes (
            node_id TEXT PRIMARY KEY,
            hostname TEXT NOT NULL,
            port INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            storage_capacity INTEGER NOT NULL,
            available_space INTEGER NOT NULL,
            last_heartbeat TIMESTAMP NOT NULL,
            blocks_stored INTEGER NOT NULL DEFAULT 0,
            UNIQUE(hostname, port)
        )
        ''')
        
        # Tabla para almacenar metadatos de archivos y directorios
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            path TEXT NOT NULL UNIQUE,
            type TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            owner TEXT
        )
        ''')
        
        # Tabla para almacenar información de bloques
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS blocks (
            block_id TEXT PRIMARY KEY,
            file_id TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            checksum TEXT,
            FOREIGN KEY (file_id) REFERENCES files (file_id) ON DELETE CASCADE
        )
        ''')
        
        # Tabla para almacenar ubicaciones de bloques en DataNodes
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS block_locations (
            block_id TEXT NOT NULL,
            datanode_id TEXT NOT NULL,
            is_leader BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY (block_id, datanode_id),
            FOREIGN KEY (block_id) REFERENCES blocks (block_id) ON DELETE CASCADE,
            FOREIGN KEY (datanode_id) REFERENCES datanodes (node_id) ON DELETE CASCADE
        )
        ''')
        
        # Índices para mejorar el rendimiento de las consultas
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_path ON files (path)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_blocks_file_id ON blocks (file_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_block_locations_block_id ON block_locations (block_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_block_locations_datanode_id ON block_locations (datanode_id)')
        
        conn.commit()
        logging.info("Database initialized successfully")
    
    # Métodos para gestionar DataNodes
    
    def register_datanode(self, hostname: str, port: int, storage_capacity: int, available_space: int) -> str:
        """
        Registra un nuevo DataNode en la base de datos.
        
        Args:
            hostname: Hostname del DataNode
            port: Puerto del DataNode
            storage_capacity: Capacidad total de almacenamiento
            available_space: Espacio disponible actual
            
        Returns:
            str: ID del DataNode registrado
        """
        try:
            node_id = str(uuid.uuid4())
            current_time = datetime.now()
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO datanodes (
                node_id, hostname, port, status, storage_capacity,
                available_space, last_heartbeat, blocks_stored
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                node_id, hostname, port, 'active', storage_capacity,
                available_space, current_time, 0
            ))
            
            conn.commit()
            return node_id
        except Exception as e:
            logging.error(f"Error registering DataNode: {e}")
            raise
    
    def get_datanode(self, node_id: str) -> Optional[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM datanodes WHERE node_id = ?', (node_id,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None
    
    def list_datanodes(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if status:
            cursor.execute('SELECT * FROM datanodes WHERE status = ?', (status,))
        else:
            cursor.execute('SELECT * FROM datanodes')
        
        return [dict(row) for row in cursor.fetchall()]
    
    def update_datanode_heartbeat(self, node_id: str, available_space: int) -> bool:
        """
        Actualiza el heartbeat y el espacio disponible de un DataNode.
        
        Args:
            node_id: ID del DataNode
            available_space: Espacio disponible actual
            
        Returns:
            bool: True si la actualización fue exitosa
        """
        try:
            current_time = datetime.now()
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE datanodes 
            SET last_heartbeat = ?, available_space = ?, status = 'active'
            WHERE node_id = ?
            ''', (current_time, available_space, node_id))
            
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error updating DataNode heartbeat: {e}")
            return False
    
    def update_datanode_status(self, node_id: str, status: str) -> bool:
        """
        Actualiza el estado de un DataNode.
        
        Args:
            node_id: ID del DataNode
            status: Nuevo estado
            
        Returns:
            bool: True si la actualización fue exitosa
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE datanodes 
            SET status = ?
            WHERE node_id = ?
            ''', (status.lower(), node_id))
            
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error updating DataNode status: {e}")
            return False
    
    def delete_datanode(self, node_id: str) -> bool:
        """
        Elimina un DataNode y todas sus referencias en la base de datos.
        
        Args:
            node_id: ID del DataNode a eliminar
            
        Returns:
            True si se eliminó correctamente, False si no
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Comenzar transacción
            cursor.execute('BEGIN TRANSACTION')
            
            # Eliminar las ubicaciones de bloques asociadas al DataNode
            cursor.execute('DELETE FROM block_locations WHERE datanode_id = ?', (node_id,))
            
            # Eliminar el DataNode
            cursor.execute('DELETE FROM datanodes WHERE node_id = ?', (node_id,))
            
            # Confirmar transacción
            conn.commit()
            return True
        except Exception as e:
            # Revertir transacción en caso de error
            conn.rollback()
            return False
    
    # Métodos para gestionar archivos y directorios
    
    def create_file(self, name: str, path: str, file_type: str, size: int = 0, owner: Optional[str] = None) -> str:
        file_id = str(uuid.uuid4())
        now = datetime.now()
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO files (file_id, name, path, type, size, created_at, modified_at, owner)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (file_id, name, path, file_type, size, now, now, owner))
        
        conn.commit()
        return file_id
    
    def get_file(self, file_id: str) -> Optional[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM files WHERE file_id = ?', (file_id,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None
    
    def get_file_by_path(self, path: str) -> Optional[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM files WHERE path = ?', (path,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None
    
    def list_directory(self, directory_path: str) -> List[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Normalizar el path del directorio
        if directory_path == "":
            directory_path = "/"
        
        # Verificar si el directorio existe
        cursor.execute('SELECT * FROM files WHERE path = ?', (directory_path,))
        directory = cursor.fetchone()
        if not directory:
            return []
        
        # Si es el directorio raíz, listar todos los archivos y directorios en la raíz
        if directory_path == "/":
            cursor.execute('''
            SELECT * FROM files 
            WHERE path != "/" AND (
                path NOT LIKE "/%/%" OR  -- archivos y directorios directamente en la raíz
                (path LIKE "/%/" AND path NOT LIKE "/%/%/%")  -- directorios en la raíz
            )
            ''')
        else:
            # Para otros directorios, listar su contenido directo
            if not directory_path.endswith('/'):
                directory_path += '/'
            cursor.execute('''
            SELECT * FROM files 
            WHERE path LIKE ? AND path != ? AND path NOT LIKE ?
            ''', (directory_path + '%', directory_path, directory_path + '%/%'))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def update_file(self, file_id: str, **kwargs) -> bool:
        if not kwargs:
            return False
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        set_clause = ', '.join([f"{key} = ?" for key in kwargs.keys()])
        query = f"UPDATE files SET {set_clause}, modified_at = ? WHERE file_id = ?"
        
        values = list(kwargs.values())
        values.append(datetime.now())
        values.append(file_id)
        
        cursor.execute(query, values)
        
        conn.commit()
        return cursor.rowcount > 0
    
    def delete_file(self, file_id: str) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM files WHERE file_id = ?', (file_id,))
        
        conn.commit()
        return cursor.rowcount > 0
    
    def delete_directory(self, directory_path: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Normalizar la ruta del directorio
        directory_path = directory_path.rstrip('/')
        if not directory_path:
            directory_path = '/'
        
        # Eliminar todos los archivos y directorios bajo este directorio
        if directory_path == '/':
            # Caso especial para el directorio raíz
            cursor.execute('DELETE FROM files WHERE path != "/"')
        else:
            # Para otros directorios
            cursor.execute('DELETE FROM files WHERE path LIKE ? OR path = ?', 
                         (directory_path + '/%', directory_path))
        
        conn.commit()
        return cursor.rowcount
    
    # Métodos para gestionar bloques
    
    def create_block(self, block_id: str, file_id: str, size: int, checksum: Optional[str] = None) -> bool:
        """
        Crea un nuevo bloque en la base de datos.
        
        Args:
            block_id: ID del bloque a crear
            file_id: ID del archivo al que pertenece el bloque
            size: Tamaño del bloque en bytes
            checksum: Checksum del bloque (opcional)
            
        Returns:
            bool: True si se creó correctamente, False en caso contrario
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO blocks (block_id, file_id, size, checksum)
            VALUES (?, ?, ?, ?)
            ''', (block_id, file_id, size, checksum))
            
            conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error creating block: {str(e)}")
            return False
    
    def get_block(self, block_id: str) -> Optional[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM blocks WHERE block_id = ?', (block_id,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None
        
    def update_block(self, block_id: str, **kwargs) -> bool:
        """Actualiza la información de un bloque en la base de datos.
        
        Args:
            block_id: ID del bloque a actualizar
            **kwargs: Campos a actualizar (size, checksum, etc.)
            
        Returns:
            True si la actualización fue exitosa, False en caso contrario
        """
        if not kwargs:
            return False
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Construir la consulta SQL dinámicamente
        set_clause = ', '.join([f"{key} = ?" for key in kwargs.keys()])
        query = f"UPDATE blocks SET {set_clause} WHERE block_id = ?"
        
        # Preparar los valores para la consulta
        values = list(kwargs.values())
        values.append(block_id)
        
        cursor.execute(query, values)
        
        conn.commit()
        return cursor.rowcount > 0
    
    def get_file_blocks(self, file_id: str) -> List[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM blocks WHERE file_id = ?', (file_id,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def delete_block(self, block_id: str) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM blocks WHERE block_id = ?', (block_id,))
        
        conn.commit()
        return cursor.rowcount > 0
    
    # Métodos para gestionar ubicaciones de bloques
    
    def add_block_location(self, block_id: str, datanode_id: str, is_leader: bool = False) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO block_locations (block_id, datanode_id, is_leader)
        VALUES (?, ?, ?)
        ''', (block_id, datanode_id, is_leader))
        
        conn.commit()
        return True
    
    def get_block_locations(self, block_id: str) -> List[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT bl.*, d.hostname, d.port, d.status
        FROM block_locations bl
        JOIN datanodes d ON bl.datanode_id = d.node_id
        WHERE bl.block_id = ?
        ''', (block_id,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def remove_block_location(self, block_id: str, datanode_id: str) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        DELETE FROM block_locations 
        WHERE block_id = ? AND datanode_id = ?
        ''', (block_id, datanode_id))
        
        conn.commit()
        return cursor.rowcount > 0
    
    def get_blocks_by_datanode(self, node_id: str) -> List[Dict[str, Any]]:
        """
        Obtiene todos los bloques almacenados en un DataNode específico.
        
        Args:
            node_id: ID del DataNode
            
        Returns:
            Lista de diccionarios con información de los bloques
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT b.*, bl.is_leader
        FROM blocks b
        JOIN block_locations bl ON b.block_id = bl.block_id
        WHERE bl.datanode_id = ?
        ''', (node_id,))
        
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def update_datanode_blocks_count(self, datanode_id: str) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Contar cuántos bloques tiene este DataNode
        cursor.execute('''
        SELECT COUNT(*) as count FROM block_locations WHERE datanode_id = ?
        ''', (datanode_id,))
        
        count = cursor.fetchone()['count']
        
        # Actualizar el contador de bloques del DataNode
        cursor.execute('''
        UPDATE datanodes SET blocks_stored = ? WHERE node_id = ?
        ''', (count, datanode_id))
        
        conn.commit()
        return True
    
    # Métodos de utilidad
    
    def get_block_with_locations(self, block_id: str) -> Optional[Dict[str, Any]]:
        block = self.get_block(block_id)
        if not block:
            return None
        
        locations = self.get_block_locations(block_id)
        block['locations'] = locations
        
        return block
    
    def get_file_with_blocks(self, file_id: str) -> Optional[Dict[str, Any]]:
        file = self.get_file(file_id)
        if not file:
            return None
        
        blocks = self.get_file_blocks(file_id)
        file['blocks'] = blocks
        
        return file
        
    def list_all_files(self) -> List[Dict[str, Any]]:
        """
        Obtiene todos los archivos en la base de datos.
        
        Returns:
            Lista de diccionarios con información de los archivos
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM files')
        
        return [dict(row) for row in cursor.fetchall()]

    def count_files(self) -> int:
        """
        Cuenta el número total de archivos (excluyendo directorios).
        """
        try:
            cursor = self.get_connection().cursor()
            cursor.execute("SELECT COUNT(*) FROM files WHERE type = 'file'")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logging.error(f"Error contando archivos: {e}")
            return 0

    def count_blocks(self) -> int:
        """
        Cuenta el número total de bloques.
        """
        try:
            cursor = self.get_connection().cursor()
            cursor.execute("SELECT COUNT(*) FROM blocks")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logging.error(f"Error contando bloques: {e}")
            return 0

    def get_all_blocks(self) -> List[Dict]:
        """
        Obtiene todos los bloques almacenados.
        """
        try:
            cursor = self.get_connection().cursor()
            cursor.execute("""
                SELECT b.block_id, b.file_id, b.size, b.checksum,
                       GROUP_CONCAT(bl.datanode_id) as datanode_ids,
                       GROUP_CONCAT(bl.is_leader) as is_leaders
                FROM blocks b
                LEFT JOIN block_locations bl ON b.block_id = bl.block_id
                GROUP BY b.block_id
            """)
            blocks = []
            for row in cursor.fetchall():
                block = {
                    'block_id': row[0],
                    'file_id': row[1],
                    'size': row[2],
                    'checksum': row[3],
                    'locations': []
                }
                if row[4] and row[5]:  # Si hay ubicaciones
                    datanode_ids = row[4].split(',')
                    is_leaders = row[5].split(',')
                    block['locations'] = [
                        {'datanode_id': dn_id, 'is_leader': bool(int(is_leader))}
                        for dn_id, is_leader in zip(datanode_ids, is_leaders)
                    ]
                blocks.append(block)
            cursor.close()
            return blocks
        except Exception as e:
            logging.error(f"Error obteniendo todos los bloques: {e}")
            return []

    def get_total_blocks_size(self) -> int:
        """
        Obtiene el tamaño total de todos los bloques.
        """
        try:
            cursor = self.get_connection().cursor()
            cursor.execute("SELECT COALESCE(SUM(size), 0) FROM blocks")
            total_size = cursor.fetchone()[0]
            cursor.close()
            return total_size
        except Exception as e:
            logging.error(f"Error obteniendo tamaño total de bloques: {e}")
            return 0
