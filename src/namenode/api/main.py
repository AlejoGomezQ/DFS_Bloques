from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
# Importaciones absolutas en lugar de relativas
from src.namenode.api.routes import files_router, blocks_router, datanodes_router, directories_router
from src.namenode.api.models import ErrorResponse
import uvicorn

app = FastAPI(
    title="DFS NameNode API",
    description="API for the Distributed File System NameNode",
    version="0.1.0"
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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
