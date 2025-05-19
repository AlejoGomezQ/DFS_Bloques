#!/usr/bin/env python3
"""
Cliente CLI de prueba para verificar los comandos rm, rmdir y cd.
"""
import os
import sys
import time
from typing import Dict, List, Any, Optional

class TestDFSClient:
    """Cliente DFS simplificado para pruebas."""
    
    def __init__(self):
        # Sistema de archivos simulado en memoria
        self.fs = {
            "/": {
                "type": "directory",
                "contents": {}
            }
        }
    
    def create_directory(self, path: str) -> bool:
        """Crea un directorio en el sistema de archivos simulado."""
        if path == "/":
            return True
        
        parent_dir = os.path.dirname(path)
        if not self._path_exists(parent_dir):
            print(f"Error: El directorio padre {parent_dir} no existe")
            return False
        
        if self._path_exists(path):
            if self._get_node_type(path) == "directory":
                print(f"El directorio {path} ya existe")
                return True
            else:
                print(f"Error: {path} ya existe y es un archivo")
                return False
        
        # Crear el directorio
        self._create_node(path, "directory")
        print(f"Directorio {path} creado exitosamente")
        return True
    
    def create_directory_recursive(self, path: str) -> bool:
        """Crea un directorio y todos sus directorios padres si no existen."""
        if path == "/":
            return True
        
        if self._path_exists(path):
            if self._get_node_type(path) == "directory":
                return True
            else:
                print(f"Error: {path} ya existe y es un archivo")
                return False
        
        # Crear el directorio padre recursivamente
        parent_dir = os.path.dirname(path)
        if parent_dir and parent_dir != path:
            if not self.create_directory_recursive(parent_dir):
                return False
        
        # Crear el directorio
        self._create_node(path, "directory")
        print(f"Directorio {path} creado exitosamente")
        return True
    
    def list_directory(self, path: str) -> Dict[str, Any]:
        """Lista el contenido de un directorio."""
        if not self._path_exists(path):
            print(f"Error: El directorio {path} no existe")
            return {}
        
        if self._get_node_type(path) != "directory":
            print(f"Error: {path} no es un directorio")
            return {}
        
        node = self._get_node(path)
        contents = []
        
        for name, item in node["contents"].items():
            item_path = os.path.join(path, name).replace("\\", "/")
            if item_path.startswith("//"):
                item_path = item_path[1:]
            
            contents.append({
                "name": name,
                "path": item_path,
                "type": item["type"],
                "size": item.get("size", 0) if item["type"] == "file" else 0
            })
        
        return {
            "path": path,
            "contents": contents
        }
    
    def upload_file(self, local_path: str, dfs_path: str, workers: int = 1) -> bool:
        """Simula la subida de un archivo al DFS."""
        if not os.path.exists(local_path):
            print(f"Error: El archivo local {local_path} no existe")
            return False
        
        if not os.path.isfile(local_path):
            print(f"Error: {local_path} no es un archivo")
            return False
        
        parent_dir = os.path.dirname(dfs_path)
        if not self._path_exists(parent_dir):
            print(f"Error: El directorio padre {parent_dir} no existe")
            return False
        
        if self._path_exists(dfs_path):
            print(f"Error: {dfs_path} ya existe en el DFS")
            return False
        
        # Crear el archivo en el DFS simulado
        file_size = os.path.getsize(local_path)
        self._create_node(dfs_path, "file", size=file_size)
        
        print(f"Archivo {local_path} subido exitosamente a {dfs_path}")
        return True
    
    def download_file(self, dfs_path: str, local_path: str, workers: int = 1) -> bool:
        """Simula la descarga de un archivo del DFS."""
        if not self._path_exists(dfs_path):
            print(f"Error: El archivo {dfs_path} no existe en el DFS")
            return False
        
        if self._get_node_type(dfs_path) != "file":
            print(f"Error: {dfs_path} no es un archivo")
            return False
        
        # Simular la descarga
        print(f"Archivo {dfs_path} descargado exitosamente a {local_path}")
        return True
    
    def delete_file(self, dfs_path: str) -> bool:
        """Elimina un archivo del DFS simulado."""
        if not self._path_exists(dfs_path):
            print(f"Error: El archivo {dfs_path} no existe en el DFS")
            return False
        
        if self._get_node_type(dfs_path) != "file":
            print(f"Error: {dfs_path} no es un archivo")
            return False
        
        # Eliminar el archivo
        self._delete_node(dfs_path)
        print(f"Archivo {dfs_path} eliminado exitosamente")
        return True
    
    def delete_directory(self, dfs_path: str) -> bool:
        """Elimina un directorio vacío del DFS simulado."""
        if not self._path_exists(dfs_path):
            print(f"Error: El directorio {dfs_path} no existe en el DFS")
            return False
        
        if self._get_node_type(dfs_path) != "directory":
            print(f"Error: {dfs_path} no es un directorio")
            return False
        
        # Verificar que el directorio esté vacío
        node = self._get_node(dfs_path)
        if node["contents"]:
            print(f"Error: El directorio {dfs_path} no está vacío")
            return False
        
        # Eliminar el directorio
        self._delete_node(dfs_path)
        print(f"Directorio {dfs_path} eliminado exitosamente")
        return True
    
    def delete_directory_recursive(self, dfs_path: str) -> bool:
        """Elimina un directorio y todo su contenido recursivamente."""
        if not self._path_exists(dfs_path):
            print(f"Error: El directorio {dfs_path} no existe en el DFS")
            return False
        
        if self._get_node_type(dfs_path) != "directory":
            print(f"Error: {dfs_path} no es un directorio")
            return False
        
        # Eliminar el directorio y todo su contenido
        self._delete_node_recursive(dfs_path)
        print(f"Directorio {dfs_path} y todo su contenido eliminados exitosamente")
        return True
    
    def _path_exists(self, path: str) -> bool:
        """Verifica si una ruta existe en el sistema de archivos simulado."""
        return self._get_node(path) is not None
    
    def _get_node_type(self, path: str) -> Optional[str]:
        """Obtiene el tipo de nodo (archivo o directorio) para una ruta."""
        node = self._get_node(path)
        return node["type"] if node else None
    
    def _get_node(self, path: str) -> Optional[Dict[str, Any]]:
        """Obtiene un nodo del sistema de archivos simulado."""
        if path == "/":
            return self.fs["/"]
        
        path = path.rstrip("/")
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        
        parent_node = self._get_node(parent_path)
        if not parent_node or parent_node["type"] != "directory":
            return None
        
        return parent_node["contents"].get(name)
    
    def _create_node(self, path: str, node_type: str, **kwargs) -> None:
        """Crea un nodo (archivo o directorio) en el sistema de archivos simulado."""
        if path == "/":
            return
        
        path = path.rstrip("/")
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        
        parent_node = self._get_node(parent_path)
        if not parent_node or parent_node["type"] != "directory":
            return
        
        if node_type == "directory":
            parent_node["contents"][name] = {
                "type": "directory",
                "contents": {}
            }
        else:
            parent_node["contents"][name] = {
                "type": "file",
                **kwargs
            }
    
    def _delete_node(self, path: str) -> None:
        """Elimina un nodo del sistema de archivos simulado."""
        if path == "/":
            return
        
        path = path.rstrip("/")
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        
        parent_node = self._get_node(parent_path)
        if not parent_node or parent_node["type"] != "directory":
            return
        
        if name in parent_node["contents"]:
            del parent_node["contents"][name]
    
    def _delete_node_recursive(self, path: str) -> None:
        """Elimina un nodo y todo su contenido recursivamente."""
        if path == "/":
            self.fs["/"]["contents"] = {}
            return
        
        self._delete_node(path)


class TestDFSCLI:
    """CLI de prueba para verificar los comandos rm, rmdir y cd."""
    
    def __init__(self):
        self.client = TestDFSClient()
        self.current_dir = "/"
    
    def run(self):
        """Ejecuta el bucle principal del CLI de prueba."""
        print("DFS CLI de Prueba - Para verificar comandos rm, rmdir y cd")
        print("Este CLI simula un sistema de archivos en memoria.")
        print("Escribe 'help' para ver los comandos disponibles.")
        
        # Crear algunos directorios y archivos de ejemplo
        self._create_sample_data()
        
        while True:
            try:
                command_line = input(f"dfs:{self.current_dir}> ")
                if not command_line.strip():
                    continue
                
                parts = command_line.strip().split()
                command = parts[0].lower()
                args = parts[1:]
                
                if command == "exit" or command == "quit":
                    print("Saliendo del CLI de prueba...")
                    break
                elif command == "help":
                    self._show_help()
                elif command == "put":
                    self._handle_put(args)
                elif command == "get":
                    self._handle_get(args)
                elif command == "ls":
                    self._handle_ls(args)
                elif command == "mkdir":
                    self._handle_mkdir(args)
                elif command == "rmdir":
                    self._handle_rmdir(args)
                elif command == "cd":
                    self._handle_cd(args)
                elif command == "rm":
                    self._handle_rm(args)
                elif command == "pwd":
                    print(f"Directorio actual: {self.current_dir}")
                else:
                    print(f"Comando desconocido: {command}")
                    print("Escribe 'help' para ver los comandos disponibles.")
            except KeyboardInterrupt:
                print("\nOperación cancelada.")
            except Exception as e:
                print(f"Error: {e}")
    
    def _create_sample_data(self):
        """Crea algunos directorios y archivos de ejemplo para las pruebas."""
        # Crear directorios
        self.client.create_directory("/test")
        self.client.create_directory("/test/documents")
        self.client.create_directory("/test/images")
        
        # Crear archivos simulados
        self._create_node("/test/documents/file1.txt", "file", size=1024)
        self._create_node("/test/documents/file2.txt", "file", size=2048)
        self._create_node("/test/images/image1.jpg", "file", size=10240)
    
    def _create_node(self, path: str, node_type: str, **kwargs):
        """Crea un nodo en el sistema de archivos simulado."""
        self.client._create_node(path, node_type, **kwargs)
    
    def _show_help(self):
        """Muestra la ayuda de los comandos disponibles."""
        help_text = """
Comandos disponibles:
  put <archivo_local> <ruta_dfs>              - Simula subir un archivo al DFS
  get <ruta_dfs> <archivo_local>              - Simula descargar un archivo del DFS
  ls [ruta] [-l]                              - Lista el contenido de un directorio
  mkdir <ruta> [-p]                           - Crea un nuevo directorio
  rmdir <ruta> [-r] [-f]                      - Elimina un directorio ([-r] recursivamente)
  rm <ruta> [-f]                              - Elimina un archivo
  cd <ruta>                                   - Cambia el directorio actual
  pwd                                         - Muestra el directorio actual
  help                                        - Muestra esta ayuda
  exit, quit                                  - Sale del CLI

Nota: Este CLI de prueba simula un sistema de archivos en memoria.
"""
        print(help_text)
    
    def _handle_put(self, args: List[str]):
        """Maneja el comando put para simular la subida de archivos."""
        if len(args) < 2:
            print("Uso: put <archivo_local> <ruta_dfs>")
            return
        
        local_path = args[0]
        dfs_path = args[1]
        
        # Resolver la ruta completa
        if not dfs_path.startswith('/'):
            dfs_path = self._resolve_path(dfs_path)
        
        # Simular la subida del archivo
        print(f"Simulando subida de {local_path} a {dfs_path}")
        self._create_node(dfs_path, "file", size=1024)
        print(f"Archivo {local_path} subido exitosamente a {dfs_path}")
    
    def _handle_get(self, args: List[str]):
        """Maneja el comando get para simular la descarga de archivos."""
        if len(args) < 2:
            print("Uso: get <ruta_dfs> <archivo_local>")
            return
        
        dfs_path = args[0]
        local_path = args[1]
        
        # Resolver la ruta completa
        if not dfs_path.startswith('/'):
            dfs_path = self._resolve_path(dfs_path)
        
        # Verificar que el archivo existe
        if not self.client._path_exists(dfs_path):
            print(f"Error: El archivo {dfs_path} no existe en el DFS")
            return
        
        if self.client._get_node_type(dfs_path) != "file":
            print(f"Error: {dfs_path} no es un archivo")
            return
        
        # Simular la descarga
        print(f"Simulando descarga de {dfs_path} a {local_path}")
        print(f"Archivo {dfs_path} descargado exitosamente a {local_path}")
    
    def _handle_ls(self, args: List[str]):
        """Maneja el comando ls para listar el contenido de un directorio."""
        path = self.current_dir  # Usar el directorio actual por defecto
        show_details = False
        
        if args:
            for arg in args:
                if arg == "-l":
                    show_details = True
                elif not arg.startswith("-"):
                    path = arg
        
        # Resolver la ruta completa
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        # Listar el contenido del directorio
        dir_info = self.client.list_directory(path)
        
        if not dir_info:
            return
        
        contents = dir_info.get("contents", [])
        
        if not contents:
            print(f"El directorio {path} está vacío")
            return
        
        # Mostrar el contenido
        if show_details:
            print(f"{'Tipo':<10} {'Tamaño':<10} {'Nombre':<30}")
            print("-" * 50)
            for item in contents:
                item_type = "archivo" if item["type"] == "file" else "directorio"
                size = f"{item['size']} B" if item["type"] == "file" else ""
                print(f"{item_type:<10} {size:<10} {item['name']:<30}")
        else:
            for item in contents:
                if item["type"] == "directory":
                    print(f"{item['name']}/")
                else:
                    print(item["name"])
    
    def _handle_mkdir(self, args: List[str]):
        """Maneja el comando mkdir para crear directorios."""
        if not args:
            print("Uso: mkdir <ruta> [-p]")
            return
        
        path = args[0]
        create_parents = "-p" in args
        
        # Resolver la ruta completa
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        # Crear el directorio
        if create_parents:
            success = self.client.create_directory_recursive(path)
        else:
            success = self.client.create_directory(path)
    
    def _handle_rmdir(self, args: List[str]):
        """Maneja el comando rmdir para eliminar directorios."""
        if not args:
            print("Uso: rmdir <ruta> [-r] [-f]")
            print("  -r: Eliminar recursivamente (incluyendo contenido)")
            print("  -f: Forzar eliminación sin confirmación")
            return
        
        # Procesar argumentos
        path = args[0]
        recursive = "-r" in args
        force = "-f" in args
        
        # Resolver la ruta completa
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        # Verificar que no se intente eliminar el directorio raíz
        if path == '/':
            print("Error: No se puede eliminar el directorio raíz")
            return
        
        # Verificar que el directorio existe
        if not self.client._path_exists(path):
            print(f"Error: El directorio {path} no existe")
            return
        
        if self.client._get_node_type(path) != "directory":
            print(f"Error: {path} no es un directorio")
            return
        
        # Verificar si el directorio está vacío
        node = self.client._get_node(path)
        contents = node["contents"]
        
        # Si no está vacío y no se usa -r, mostrar error
        if contents and not recursive:
            print(f"Error: El directorio {path} no está vacío. Use -r para eliminar recursivamente.")
            return
        
        # Confirmar la eliminación si no se usa -f
        if not force:
            if recursive and contents:
                confirm = input(f"¿Está seguro de que desea eliminar {path} y todo su contenido? (s/n): ")
            else:
                confirm = input(f"¿Está seguro de que desea eliminar {path}? (s/n): ")
            
            if confirm.lower() != "s":
                print("Operación cancelada")
                return
        
        # Eliminar el directorio
        if recursive and contents:
            # Eliminar recursivamente
            success = self.client.delete_directory_recursive(path)
        else:
            # Eliminar directorio vacío
            success = self.client.delete_directory(path)
        
        # Si estamos en el directorio que se eliminó, volver al directorio padre
        if success and self.current_dir.startswith(path):
            self.current_dir = os.path.dirname(path) or "/"
            print(f"Directorio actual cambiado a: {self.current_dir}")
    
    def _handle_rm(self, args: List[str]):
        """Maneja el comando rm para eliminar archivos."""
        if not args:
            print("Uso: rm <ruta> [-f]")
            return
        
        # Procesar argumentos
        path = args[0]
        force = "-f" in args
        
        # Resolver la ruta completa
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        # Verificar que el archivo existe
        if not self.client._path_exists(path):
            print(f"Error: El archivo {path} no existe")
            return
        
        if self.client._get_node_type(path) != "file":
            print(f"Error: {path} no es un archivo")
            return
        
        # Confirmar la eliminación si no se usa -f
        if not force:
            confirm = input(f"¿Está seguro de que desea eliminar {path}? (s/n): ")
            if confirm.lower() != "s":
                print("Operación cancelada")
                return
        
        # Eliminar el archivo
        self.client.delete_file(path)
    
    def _handle_cd(self, args: List[str]):
        """Maneja el comando cd para cambiar de directorio."""
        if not args:
            # cd sin argumentos vuelve al directorio raíz
            self.current_dir = "/"
            return
        
        path = args[0]
        
        # Manejar casos especiales
        if path == ".":
            return
        elif path == "..":
            # Subir un nivel
            self.current_dir = os.path.dirname(self.current_dir) or "/"
            return
        
        # Resolver la ruta completa
        if not path.startswith('/'):
            new_path = self._resolve_path(path)
        else:
            new_path = path
        
        # Verificar que el directorio existe
        if not self.client._path_exists(new_path):
            print(f"Error: El directorio {new_path} no existe")
            return
        
        if self.client._get_node_type(new_path) != "directory":
            print(f"Error: {new_path} no es un directorio")
            return
        
        # Actualizar el directorio actual
        self.current_dir = new_path
    
    def _resolve_path(self, relative_path: str) -> str:
        """
        Resuelve una ruta relativa basada en el directorio actual.
        
        Args:
            relative_path: Ruta relativa
            
        Returns:
            Ruta absoluta en el DFS
        """
        # Manejar rutas con múltiples componentes
        if '/' in relative_path:
            components = relative_path.split('/')
            current = self.current_dir
            
            for component in components:
                if not component:
                    continue
                elif component == '.':
                    continue
                elif component == '..':
                    current = os.path.dirname(current) or "/"
                else:
                    current = os.path.join(current, component).replace('\\', '/')
                    if not current.startswith('/'):
                        current = f"/{current}"
            
            return current
        
        # Caso simple: un solo componente
        path = os.path.join(self.current_dir, relative_path).replace('\\', '/')
        if not path.startswith('/'):
            path = f"/{path}"
        
        return path


if __name__ == "__main__":
    cli = TestDFSCLI()
    cli.run()
