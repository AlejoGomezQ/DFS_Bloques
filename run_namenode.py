"""
Script para iniciar el servidor FastAPI del NameNode.
"""

import sys
import os
from pathlib import Path

# Añadir el directorio raíz al path para poder importar los módulos
root_dir = Path(__file__).parent
sys.path.append(str(root_dir))

# Importar la aplicación FastAPI
from src.namenode.api.main import app
import uvicorn

if __name__ == "__main__":
    print("Iniciando servidor NameNode...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
