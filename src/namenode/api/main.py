from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
import threading
import argparse
import uuid
import sys
import datetime

# Importaciones absolutas en lugar de relativas
from src.namenode.api.routes import files_router, blocks_router, datanodes_router, directories_router
from src.namenode.api.models import ErrorResponse, FileType
from src.namenode.metadata.manager import MetadataManager
from src.namenode.monitoring.datanode_monitor import DataNodeMonitor
from src.namenode.leader.leader_election import LeaderElection
from src.namenode.api.dependencies import set_metadata_manager, get_metadata_manager
from src.namenode.leader.namenode_service import serve as serve_grpc
from src.namenode.sync.metadata_sync import MetadataSync
from src.namenode.init_root import init_root_directory
import uvicorn

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("NameNode")

app = FastAPI(
    title="DFS NameNode API",
    description="API for the Distributed File System NameNode",
    version="0.1.0"
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

# Variables globales para los componentes del sistema
metadata_manager = None
datanode_monitor = None
leader_election = None
metadata_sync = None
grpc_server = None

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

@app.on_event("startup")
async def startup_event():
    global metadata_manager, datanode_monitor, leader_election, metadata_sync, grpc_server
    
    try:
        # Limpiar puertos si están en uso
        cleanup_ports()
        
        # Obtener configuración del entorno o usar valores predeterminados
        node_id = os.environ.get("NAMENODE_ID", str(uuid.uuid4()))
        hostname = os.environ.get("NAMENODE_HOST", "localhost")
        rest_port = int(os.environ.get("NAMENODE_REST_PORT", "8000"))
        grpc_port = int(os.environ.get("NAMENODE_GRPC_PORT", "50051"))
        
        # Inicializar el gestor de metadatos con el ID del nodo
        metadata_manager = MetadataManager(node_id=node_id)
        set_metadata_manager(metadata_manager)
        
        # Inicializar el directorio raíz
        try:
            init_root_directory(metadata_manager)
        except Exception as e:
            logger.error(f"Error al inicializar el directorio raíz: {e}")
            raise
        
        # Procesar nodos conocidos del entorno
        known_nodes_str = os.environ.get("NAMENODE_KNOWN_NODES", "")
        if known_nodes_str:
            for node_info in known_nodes_str.split(","):
                try:
                    node_parts = node_info.split(":")
                    if len(node_parts) == 3:
                        other_id, other_host, other_port = node_parts
                        metadata_manager.add_known_node(other_id, other_host, int(other_port))
                        logger.info(f"Added known node: {other_id} at {other_host}:{other_port}")
                except Exception as e:
                    logger.error(f"Error processing known node {node_info}: {str(e)}")
        
        # Inicializar el monitor de DataNodes
        datanode_monitor = DataNodeMonitor(metadata_manager)
        datanode_monitor.start()
        logger.info("DataNode monitor started")
        
        # Inicializar el sistema de elección de líder con los nodos conocidos del gestor de metadatos
        known_nodes = metadata_manager.get_known_nodes()
        leader_election = LeaderElection(node_id, hostname, grpc_port)
        
        # Añadir nodos conocidos después de la inicialización
        for node in known_nodes:
            leader_election.add_node(node['id'], node['host'], node['port'])
        
        # Configurar callbacks para cambios de rol
        leader_election.on_leader_elected = lambda: logger.info(f"NameNode {node_id} became leader")
        leader_election.on_leader_lost = lambda: logger.info(f"NameNode {node_id} became follower")
        
        # Inicializar el servicio de sincronización de metadatos
        metadata_sync = MetadataSync(metadata_manager)
        
        # Iniciar el servidor gRPC en un hilo separado
        def start_grpc_server():
            global grpc_server
            try:
                grpc_server, _, _ = serve_grpc(node_id, hostname, grpc_port, metadata_manager, leader_election, metadata_sync)
                grpc_server.wait_for_termination()
            except Exception as e:
                logger.error(f"Error starting gRPC server: {e}")
                sys.exit(1)
        
        grpc_thread = threading.Thread(target=start_grpc_server, daemon=True)
        grpc_thread.start()
        logger.info(f"NameNode gRPC server starting at {hostname}:{grpc_port}")
        
        # Iniciar el sistema de elección de líder y sincronización de metadatos
        leader_election.start()
        metadata_sync.start()
        
        logger.info(f"NameNode {node_id} started with REST API at {hostname}:{rest_port} and gRPC at {hostname}:{grpc_port}")
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        sys.exit(1)

@app.on_event("shutdown")
async def shutdown_event():
    global metadata_manager, datanode_monitor, leader_election, metadata_sync, grpc_server
    
    try:
        if datanode_monitor:
            datanode_monitor.stop()
            logger.info("DataNode monitor stopped")
        
        if leader_election:
            leader_election.stop()
            logger.info("Leader election system stopped")
        
        if metadata_sync:
            metadata_sync.stop()
            logger.info("Metadata synchronization service stopped")
        
        if grpc_server:
            grpc_server.stop(0)
            logger.info("gRPC server stopped")
        
        if metadata_manager:
            metadata_manager.close()
            logger.info("Metadata manager closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

def main():
    try:
        cleanup_ports()
        uvicorn.run(app, host="0.0.0.0", port=8000)
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
