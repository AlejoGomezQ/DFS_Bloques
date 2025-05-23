# Sistema de Archivos Distribuido - Diseño y Especificación

## Índice
1. [Arquitectura General](#arquitectura-general)
2. [Componentes del Sistema](#componentes-del-sistema)
3. [Protocolos de Comunicación](#protocolos-de-comunicación)
4. [Gestión de Datos](#gestión-de-datos)
5. [Tolerancia a Fallos](#tolerancia-a-fallos)
6. [Interfaces de Usuario](#interfaces-de-usuario)
7. [Detalles de Implementación](#detalles-de-implementación)

## Arquitectura General

### Visión General
El sistema implementa una arquitectura distribuida para el almacenamiento de archivos, siguiendo un modelo similar a HDFS. Los componentes principales son:

- NameNode (Leader/Follower)
- DataNodes
- Clientes (CLI y API)

### Topología del Sistema
```
[Clientes] <----> [NameNode Leader] <----> [NameNode Follower]
     |                    |                         |
     |                    v                         v
     +-------------> [DataNode-1] <------------> [DataNode-2] <---> [DataNode-N]
```

## Componentes del Sistema

### 1. NameNode

#### 1.1 NameNode Leader
- **Responsabilidades**:
  - Gestión de metadatos del sistema de archivos
  - Coordinación de operaciones de escritura/lectura
  - Monitoreo de DataNodes
  - Gestión de replicación de bloques

- **Implementación**:
  ```python
  class NameNodeServicer:
      - RequestVote(): Manejo de elección de líder
      - Heartbeat(): Monitoreo de nodos
      - SyncMetadata(): Sincronización de metadatos
  ```

#### 1.2 NameNode Follower
- **Funcionalidades**:
  - Respaldo de metadatos
  - Participación en elección de líder
  - Sincronización con el líder
  - Failover automático

- **Sistema de Elección**:
  ```python
  class LeaderElection:
      - start(): Inicio del proceso de elección
      - _start_election(): Solicitud de votos
      - _become_leader(): Transición a líder
      - handle_vote_request(): Procesamiento de votos
  ```

### 2. DataNode

#### 2.1 Características Principales
- **Almacenamiento**:
  - Gestión de bloques de datos
  - Sistema de archivos local
  - Compresión de datos (opcional)

- **Comunicación**:
  ```protobuf
  service DataNodeService {
      rpc StoreBlock (stream BlockData)
      rpc RetrieveBlock (BlockRequest)
      rpc ReplicateBlock (ReplicationRequest)
      rpc TransferBlock (TransferRequest)
      rpc CheckBlock (BlockRequest)
      rpc DeleteBlock (BlockRequest)
  }
  ```

#### 2.2 Gestión de Bloques
- **Estructura de Bloques**:
  ```python
  class BlockData:
      - block_id: str
      - data: bytes
      - offset: int
      - total_size: int
      - compressed: bool
      - compression_metadata: bytes
  ```

### 3. Sistema de Registro y Monitoreo

#### 3.1 Registro de DataNodes
```python
class DataNodeRegistration:
    - register(): Registro inicial
    - heartbeat(): Envío periódico de estado
    - start_heartbeat_thread(): Monitoreo continuo
```

#### 3.2 Monitoreo de Estado
```python
class DataNodeMonitor:
    - start(): Inicio de monitoreo
    - _monitor_loop(): Verificación periódica
    - _cleanup_loop(): Limpieza de nodos inactivos
```

## Protocolos de Comunicación

### 1. Canal de Control (REST API)

#### 1.1 Endpoints Principales
- **Gestión de Archivos**:
  - `POST /files/upload`: Inicio de carga de archivo
  - `GET /files/download/{file_id}`: Obtención de información para descarga
  - `DELETE /files/{file_id}`: Eliminación de archivo

- **Gestión de Directorios**:
  - `GET /directories/{path}`: Listado de contenido
  - `POST /directories`: Creación de directorio
  - `DELETE /directories/{path}`: Eliminación de directorio

- **Gestión de DataNodes**:
  - `POST /datanodes/register`: Registro de nuevo DataNode
  - `PUT /datanodes/{node_id}/heartbeat`: Actualización de estado

### 2. Canal de Datos (gRPC)

#### 2.1 Servicios Definidos
```protobuf
service NameNodeService {
    rpc RequestVote(VoteRequest) returns (VoteResponse)
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse)
    rpc SyncMetadata(SyncRequest) returns (SyncResponse)
}

service DataNodeService {
    rpc StoreBlock(stream BlockData) returns (BlockResponse)
    rpc RetrieveBlock(BlockRequest) returns (stream BlockData)
    rpc ReplicateBlock(ReplicationRequest) returns (BlockResponse)
}
```

## Gestión de Datos

### 1. Particionamiento de Archivos
- **Proceso de División**:
  - Tamaño de bloque configurable
  - Distribución entre DataNodes
  - Metadata por bloque

### 2. Replicación
- **Estrategia**:
  - Mínimo 2 copias por bloque
  - Selección de DataNodes por disponibilidad
  - Replicación síncrona

### 3. Distribución de Bloques
```python
class BlockReplicator:
    - replication_factor: int = 2
    - metadata_manager: MetadataManager
    - logger: Logger
```

## Tolerancia a Fallos

### 1. Detección de Fallos
- **Mecanismos**:
  - Heartbeat periódico
  - Timeout configurable
  - Estado de nodos

### 2. Recuperación
- **Procesos**:
  - Re-replicación automática
  - Failover de NameNode
  - Redistribución de carga

### 3. Consistencia
- **Garantías**:
  - Replicación síncrona
  - Verificación de checksums
  - Registro de operaciones

## Interfaces de Usuario

### 1. CLI
- **Comandos Implementados**:
  ```bash
  ls      # Listar contenido de directorio
  cd      # Cambiar directorio
  put     # Subir archivo
  get     # Descargar archivo
  mkdir   # Crear directorio
  rmdir   # Eliminar directorio
  rm      # Eliminar archivo
  ```

### 2. API Cliente
```python
class DFSClient:
    - put_file(local_path: str, dfs_path: str)
    - get_file(dfs_path: str, local_path: str)
    - list_directory(path: str)
    - make_directory(path: str)
    - remove_directory(path: str)
    - remove_file(path: str)
```

## Detalles de Implementación

### 1. Estructuras de Datos

#### 1.1 Metadatos
```python
class DataNodeInfo:
    - node_id: str
    - hostname: str
    - port: int
    - status: DataNodeStatus
    - storage_capacity: int
    - available_space: int
    - last_heartbeat: datetime
    - blocks_stored: int
```

#### 1.2 Bloques
```python
class BlockLocation:
    - block_id: str
    - datanode_id: str
    - is_leader: bool
```

### 2. Gestión de Estado

#### 2.1 Estados de DataNode
```python
enum DataNodeStatus:
    ACTIVE
    INACTIVE
    DEAD
    MAINTENANCE
```

#### 2.2 Monitoreo de Recursos
- Espacio disponible
- Carga del sistema
- Estado de la red
- Rendimiento

### 3. Rendimiento
- Transferencia directa Cliente-DataNode
- Compresión opcional de bloques
- Caché de metadatos
- Distribución de carga

## Conclusión

El sistema implementa una arquitectura distribuida robusta con las siguientes características principales:
- Almacenamiento distribuido con replicación
- Tolerancia a fallos
- Escalabilidad horizontal
- Interfaces múltiples (CLI y API) 