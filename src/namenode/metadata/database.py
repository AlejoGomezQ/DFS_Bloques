import sqlite3
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import uuid
from datetime import datetime

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
            self.connection = sqlite3.connect(self.db_path)
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
            status TEXT NOT NULL,
            storage_capacity INTEGER NOT NULL,
            available_space INTEGER NOT NULL,
            last_heartbeat TIMESTAMP,
            blocks_stored INTEGER DEFAULT 0
        )
        ''')
        
        # Tabla para almacenar metadatos de archivos y directorios
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            type TEXT NOT NULL,
            size INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            owner TEXT
        )
        ''')
        
        # Tabla para almacenar información de bloques
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS blocks (
            block_id TEXT PRIMARY KEY,
            file_id TEXT NOT NULL,
            size INTEGER NOT NULL,
            checksum TEXT,
            FOREIGN KEY (file_id) REFERENCES files (file_id) ON DELETE CASCADE
        )
        ''')
        
        # Tabla para almacenar ubicaciones de bloques en DataNodes
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS block_locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_id TEXT NOT NULL,
            datanode_id TEXT NOT NULL,
            is_leader BOOLEAN DEFAULT 0,
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
    
    # Métodos para gestionar DataNodes
    
    def register_datanode(self, hostname: str, port: int, storage_capacity: int, available_space: int) -> str:
        node_id = str(uuid.uuid4())
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO datanodes (node_id, hostname, port, status, storage_capacity, available_space, last_heartbeat)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (node_id, hostname, port, 'active', storage_capacity, available_space, datetime.now()))
        
        conn.commit()
        return node_id
    
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
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        UPDATE datanodes 
        SET last_heartbeat = ?, available_space = ? 
        WHERE node_id = ?
        ''', (datetime.now(), available_space, node_id))
        
        conn.commit()
        return cursor.rowcount > 0
    
    def update_datanode_status(self, node_id: str, status: str) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('UPDATE datanodes SET status = ? WHERE node_id = ?', (status, node_id))
        
        conn.commit()
        return cursor.rowcount > 0
    
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
        
        # Asegurarse de que el directorio termina con /
        if not directory_path.endswith('/') and directory_path != '':
            directory_path += '/'
        
        # Listar archivos y directorios directamente en el directorio especificado
        if directory_path == '':
            # Caso especial para el directorio raíz
            cursor.execute('''
            SELECT * FROM files 
            WHERE path = '' OR path NOT LIKE '%/%' OR (path LIKE '%/%' AND path NOT LIKE '%/%/%')
            ''')
        else:
            cursor.execute('''
            SELECT * FROM files 
            WHERE path LIKE ? AND path NOT LIKE ? AND path != ?
            ''', (directory_path + '%', directory_path + '%/%', directory_path))
        
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
        
        # Asegurarse de que el directorio termina con /
        if not directory_path.endswith('/') and directory_path != '':
            directory_path += '/'
        
        # Eliminar todos los archivos y directorios bajo este directorio
        cursor.execute('DELETE FROM files WHERE path LIKE ?', (directory_path + '%',))
        
        # Eliminar el directorio en sí
        cursor.execute('DELETE FROM files WHERE path = ?', (directory_path,))
        
        conn.commit()
        return cursor.rowcount
    
    # Métodos para gestionar bloques
    
    def create_block(self, file_id: str, size: int, checksum: Optional[str] = None) -> str:
        block_id = str(uuid.uuid4())
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO blocks (block_id, file_id, size, checksum)
        VALUES (?, ?, ?, ?)
        ''', (block_id, file_id, size, checksum))
        
        conn.commit()
        return block_id
    
    def get_block(self, block_id: str) -> Optional[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM blocks WHERE block_id = ?', (block_id,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None
    
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
    
    def get_blocks_by_datanode(self, datanode_id: str) -> List[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT b.*, bl.is_leader
        FROM blocks b
        JOIN block_locations bl ON b.block_id = bl.block_id
        WHERE bl.datanode_id = ?
        ''', (datanode_id,))
        
        return [dict(row) for row in cursor.fetchall()]
    
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
