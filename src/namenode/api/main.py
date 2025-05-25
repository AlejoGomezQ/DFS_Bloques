from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends, Query, Path, Body
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
import threading
import argparse
import uuid
import sys
import datetime
import grpc
from concurrent import futures
import uvicorn

# Importaciones absolutas en lugar de relativas
from src.namenode.api.routes import files_router, blocks_router, datanodes_router, directories_router, system_router
from src.namenode.api.models import ErrorResponse, FileType, DataNodeInfo, DataNodeRegistration, HeartbeatRequest, FileMetadata, DirectoryListing
from src.namenode.metadata.manager import MetadataManager
from src.namenode.monitoring.datanode_monitor import DataNodeMonitor
from src.namenode.replication.block_replicator import BlockReplicator
from src.namenode.sync.metadata_sync import MetadataSync
from src.namenode.init_root import init_root_directory
from src.namenode.api.dependencies import set_metadata_manager, get_metadata_manager

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("NameNode")

# Variables globales para los componentes del sistema
metadata_manager = None
datanode_monitor = None
metadata_sync = None
grpc_server = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Código que se ejecuta al inicio
    global metadata_manager, datanode_monitor, metadata_sync, grpc_server
    
    try:
        # Inicializar el gestor de metadatos
        metadata_manager = MetadataManager(node_id=args.id)
        # Configurar el metadata_manager como dependencia global
        set_metadata_manager(metadata_manager)
        
        # Asegurar que existe el directorio raíz
        init_root_directory(metadata_manager)
        
        # Iniciar el monitor de DataNodes
        datanode_monitor = DataNodeMonitor(metadata_manager)
        datanode_monitor.start()
        logger.info("DataNode monitor started")
        
        # Iniciar el servidor gRPC
        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        grpc_server.add_insecure_port(f'{args.host}:{args.grpc_port}')
        grpc_server.start()
        logger.info(f"NameNode gRPC server starting at {args.host}:{args.grpc_port}")
        
        # Iniciar el servicio de sincronización de metadatos
        metadata_sync = MetadataSync(
            node_id=args.id,
            metadata_manager=metadata_manager
        )
        metadata_sync.start()
        logger.info("Metadata sync service started")
        
        logger.info(f"NameNode {args.id} started with REST API at {args.host}:{args.rest_port} and gRPC at {args.host}:{args.grpc_port}")
        
        yield
        
        # Código que se ejecuta al apagar
        if grpc_server:
            grpc_server.stop(0)
            logger.info("gRPC server stopped")
        
        if datanode_monitor:
            datanode_monitor.stop()
            logger.info("DataNode monitor stopped")
        
        if metadata_sync:
            metadata_sync.stop()
            logger.info("Metadata sync service stopped")
        
        if metadata_manager:
            metadata_manager.close()
            logger.info("Metadata manager closed")
            
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")
        raise

# Crear la aplicación FastAPI con el nuevo sistema de lifespan
app = FastAPI(
    title="DFS NameNode API",
    description="API for the Distributed File System NameNode",
    version="0.1.0",
    lifespan=lifespan
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(files_router)
app.include_router(blocks_router)
app.include_router(datanodes_router)
app.include_router(directories_router)
app.include_router(system_router, prefix="/system")

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal Server Error",
            details=str(exc)
        ).dict()
    )

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.delete("/datanodes/cleanup")
async def cleanup_inactive_datanodes(min_inactive_time: int = 3600):
    """
    Elimina los DataNodes que han estado inactivos por más del tiempo especificado.
    
    Args:
        min_inactive_time: Tiempo mínimo en segundos que un DataNode debe estar inactivo para ser eliminado (default: 1 hora)
    """
    try:
        metadata_manager = get_metadata_manager()
        datanodes = metadata_manager.list_datanodes()
        current_time = datetime.datetime.now()
        deleted_count = 0
        
        for datanode in datanodes:
            if datanode.status == "inactive" and datanode.last_heartbeat:
                inactive_time = (current_time - datanode.last_heartbeat).total_seconds()
                if inactive_time >= min_inactive_time:
                    metadata_manager.delete_datanode(datanode.node_id)
                    deleted_count += 1
                    logger.info(f"Deleted inactive DataNode {datanode.node_id}")
        
        return {
            "message": f"Cleanup completed. Deleted {deleted_count} inactive DataNodes",
            "deleted_count": deleted_count
        }
    except Exception as e:
        logger.error(f"Error during DataNode cleanup: {e}")
        raise

def cleanup_ports():
    """Intenta limpiar los puertos si están en uso."""
    import socket
    import time
    
    def try_bind_port(port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(('localhost', port))
            sock.close()
            return True
        except:
            return False
    
    # Intentar liberar puerto 8000
    if not try_bind_port(8000):
        logger.warning("Puerto 8000 en uso, intentando liberar...")
        os.system('netstat -ano | findstr "8000" > temp.txt')
        with open('temp.txt', 'r') as f:
            for line in f:
                if ':8000' in line:
                    pid = line.strip().split()[-1]
                    os.system(f'taskkill /F /PID {pid}')
        os.remove('temp.txt')
        time.sleep(1)
    
    # Intentar liberar puerto 50051
    if not try_bind_port(50051):
        logger.warning("Puerto 50051 en uso, intentando liberar...")
        os.system('netstat -ano | findstr "50051" > temp.txt')
        with open('temp.txt', 'r') as f:
            for line in f:
                if ':50051' in line:
                    pid = line.strip().split()[-1]
                    os.system(f'taskkill /F /PID {pid}')
        os.remove('temp.txt')
        time.sleep(1)

def main():
    try:
        cleanup_ports()
        # Configurar el servidor uvicorn con el host y puerto correctos
        uvicorn.run(
            app,
            host=args.host,
            port=args.rest_port,
            log_level="info"
        )
    except Exception as e:
        logger.error(f"Error running server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description="NameNode Server")
    parser.add_argument("--id", type=str, help="Unique ID for this NameNode", default=str(uuid.uuid4()))
    parser.add_argument("--host", type=str, help="Hostname or IP address", default="localhost")
    parser.add_argument("--rest-port", type=int, help="REST API port", default=8000)
    parser.add_argument("--grpc-port", type=int, help="gRPC server port", default=50051)
    parser.add_argument("--known-nodes", type=str, help="Comma-separated list of known nodes in format 'id:host:port'", default="")
    
    args = parser.parse_args()
    
    # Establecer variables de entorno para que sean accesibles en el evento de inicio
    os.environ["NAMENODE_ID"] = args.id
    os.environ["NAMENODE_HOST"] = args.host
    os.environ["NAMENODE_REST_PORT"] = str(args.rest_port)
    os.environ["NAMENODE_GRPC_PORT"] = str(args.grpc_port)
    
    # Procesar lista de nodos conocidos
    if args.known_nodes:
        os.environ["NAMENODE_KNOWN_NODES"] = args.known_nodes
    
    # Iniciar el servidor FastAPI
    logger.info(f"Starting NameNode {args.id} with REST API at {args.host}:{args.rest_port} and gRPC at {args.host}:{args.grpc_port}")
    main()
