# Sistema de Archivos Distribuidos por Bloques

Este proyecto implementa un sistema de archivos distribuidos por bloques minimalista que permite almacenar, recuperar y gestionar archivos distribuidos en múltiples nodos, garantizando replicación y alta disponibilidad.

## Estructura del Proyecto

```
src/
├── namenode/          # Componente NameNode (gestión de metadatos)
│   └── api/           # API REST para el canal de control
├── datanode/          # Componente DataNode (almacenamiento de bloques)
│   └── service/       # Servicios gRPC para el canal de datos
├── common/            # Código compartido entre componentes
│   └── proto/         # Definiciones de protocolo gRPC
└── client/            # Cliente CLI para interactuar con el sistema
```

## Interfaces y Contratos de Comunicación

### Canal de Control (REST API)

El NameNode expone una API REST para la gestión de metadatos con los siguientes endpoints:

- `/files` - Gestión de archivos y metadatos
- `/blocks` - Información sobre bloques y ubicaciones
- `/datanodes` - Registro y estado de DataNodes
- `/directories` - Gestión de estructura de directorios

### Canal de Datos (gRPC)

Los DataNodes exponen servicios gRPC para la transferencia de datos:

- `StoreBlock` - Almacenamiento de bloques
- `RetrieveBlock` - Recuperación de bloques
- `ReplicateBlock` - Replicación de bloques entre DataNodes
- `TransferBlock` - Transferencia de bloques entre nodos
- `CheckBlock` - Verificación de existencia de bloques
- `DeleteBlock` - Eliminación de bloques

## Requisitos

- Python 3.8+
- FastAPI
- gRPC Python
- SQLite

## Instalación

```bash
pip install -r requirements.txt
```

## Generación de código gRPC

Para generar el código Python a partir de las definiciones de protocolo:

```bash
python -m grpc_tools.protoc -I./src/common/proto --python_out=./src/common --grpc_python_out=./src/common ./src/common/proto/datanode.proto
```
