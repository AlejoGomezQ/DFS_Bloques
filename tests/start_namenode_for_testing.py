import os
import sys
import uvicorn

# Agregar el directorio raíz al path para poder importar los módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.namenode.api.main import app

if __name__ == "__main__":
    print("Iniciando NameNode para pruebas en http://localhost:8000")
    print("Presiona Ctrl+C para detener el servidor")
    uvicorn.run(app, host="0.0.0.0", port=8000)
