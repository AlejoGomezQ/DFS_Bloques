from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
# Importaciones absolutas en lugar de relativas
from src.namenode.api.routes import files_router, blocks_router, datanodes_router, directories_router
from src.namenode.api.models import ErrorResponse
from src.namenode.metadata.manager import MetadataManager, get_metadata_manager
from src.namenode.monitoring.datanode_monitor import DataNodeMonitor
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

# Inicializar el monitor de DataNodes
datanode_monitor = None

@app.on_event("startup")
async def startup_event():
    global datanode_monitor
    metadata_manager = get_metadata_manager()
    datanode_monitor = DataNodeMonitor(metadata_manager)
    datanode_monitor.start()
    logger.info("DataNode monitor started")

@app.on_event("shutdown")
async def shutdown_event():
    global datanode_monitor
    if datanode_monitor:
        datanode_monitor.stop()
        logger.info("DataNode monitor stopped")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
