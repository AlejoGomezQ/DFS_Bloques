from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
import threading
import argparse
import uuid
# Importaciones absolutas en lugar de relativas
from src.namenode.api.routes import files_router, blocks_router, datanodes_router, directories_router
from src.namenode.api.routes.balancer import router as balancer_router
from src.namenode.api.models import ErrorResponse
from src.namenode.metadata.manager import MetadataManager
from src.namenode.monitoring.datanode_monitor import DataNodeMonitor
from src.namenode.leader.leader_election import LeaderElection

# Variable global para el gestor de metadatos
_metadata_manager = None

def get_metadata_manager():
    global _metadata_manager
    return _metadata_manager
from src.namenode.leader.namenode_service import serve as serve_grpc
from src.namenode.sync.metadata_sync import MetadataSync
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
app.include_router(files_router, prefix="/files", tags=["Files"])
app.include_router(blocks_router, prefix="/blocks", tags=["Blocks"])
app.include_router(datanodes_router, prefix="/datanodes", tags=["DataNodes"])
app.include_router(directories_router, prefix="/directories", tags=["Directories"])
app.include_router(balancer_router, prefix="/balancer", tags=["LoadBalancer"])

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

# Variables globales para los componentes del sistema
datanode_monitor = None
leader_election = None
metadata_sync = None
grpc_server = None
load_balancer = None

@app.on_event("startup")
async def startup_event():
    global datanode_monitor, leader_election, metadata_sync, grpc_server, load_balancer
    
    # Obtener configuración del entorno o usar valores predeterminados
    node_id = os.environ.get("NAMENODE_ID", str(uuid.uuid4()))
    hostname = os.environ.get("NAMENODE_HOST", "localhost")
    rest_port = int(os.environ.get("NAMENODE_REST_PORT", "8000"))
    grpc_port = int(os.environ.get("NAMENODE_GRPC_PORT", "50051"))
    
    # Inicializar el gestor de metadatos con el ID del nodo
    global _metadata_manager
    _metadata_manager = MetadataManager(node_id=node_id)
    metadata_manager = _metadata_manager
    
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
    leader_election = LeaderElection(node_id, hostname, grpc_port, known_nodes=known_nodes)
    
    # Configurar callbacks para cambios de rol
    leader_election.on_leader_elected = lambda: logger.info(f"NameNode {node_id} became leader")
    leader_election.on_leader_lost = lambda: logger.info(f"NameNode {node_id} became follower")
    
    # Inicializar el servicio de sincronización de metadatos
    metadata_sync = MetadataSync(metadata_manager)
    
    # Iniciar el servidor gRPC en un hilo separado
    def start_grpc_server():
        global grpc_server
        grpc_server, _, _ = serve_grpc(node_id, hostname, grpc_port, metadata_manager, leader_election, metadata_sync)
        grpc_server.wait_for_termination()
    
    grpc_thread = threading.Thread(target=start_grpc_server, daemon=True)
    grpc_thread.start()
    logger.info(f"NameNode gRPC server starting at {hostname}:{grpc_port}")
    
    # Iniciar el sistema de elección de líder y sincronización de metadatos
    leader_election.start()
    metadata_sync.start()
    
    # Inicializar y arrancar el balanceador de carga
    from src.namenode.balancer.load_balancer import LoadBalancer
    load_balancer = LoadBalancer(metadata_manager, auto_balance=True)
    load_balancer.start()
    logger.info(f"Load balancer started with threshold {load_balancer.balance_threshold} and check interval {load_balancer.check_interval}s")
    
    logger.info(f"NameNode {node_id} started with REST API at {hostname}:{rest_port} and gRPC at {hostname}:{grpc_port}")

@app.on_event("shutdown")
async def shutdown_event():
    global datanode_monitor, leader_election, metadata_sync, grpc_server, load_balancer
    
    # Detener el monitor de DataNodes
    if datanode_monitor:
        datanode_monitor.stop()
        logger.info("DataNode monitor stopped")
    
    # Detener el sistema de elección de líder
    if leader_election:
        leader_election.stop()
        logger.info("Leader election system stopped")
    
    # Detener el servicio de sincronización de metadatos
    if metadata_sync:
        metadata_sync.stop()
        logger.info("Metadata synchronization service stopped")
    
    # Detener el servidor gRPC
    if grpc_server:
        grpc_server.stop(0)  # 0 significa detener inmediatamente
        logger.info("gRPC server stopped")
    
    # Detener el balanceador de carga
    if load_balancer:
        load_balancer.stop()
        logger.info("Load balancer stopped")
    
    # Cerrar el gestor de metadatos
    global _metadata_manager
    if _metadata_manager:
        _metadata_manager.close()
        logger.info("Metadata manager closed")

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
    uvicorn.run(app, host="0.0.0.0", port=args.rest_port)
