#!/usr/bin/env python3
import argparse
import os
import sys
from typing import List, Dict, Optional

from src.client.dfs_client import DFSClient


class DFSCLI:
    """
    Interfaz de línea de comandos para el sistema de archivos distribuido.
    """
    def __init__(self, namenode_url: str, block_size: Optional[int] = None):
        """
        Inicializa el cliente CLI.
        
        Args:
            namenode_url: URL del NameNode (ej: 'http://localhost:8000')
            block_size: Tamaño de bloque personalizado (opcional)
        """
        self.client = DFSClient(namenode_url, block_size)
        self.current_dir = "/"
    
    def run(self):
        """
        Ejecuta el bucle principal del CLI.
        """
        print("DFS CLI - Cliente para Sistema de Archivos Distribuido")
        print(f"Conectado a NameNode: {self.client.namenode_client.base_url}")
        print("Escribe 'help' para ver los comandos disponibles.")
        
        while True:
            try:
                command_line = input(f"dfs:{self.current_dir}> ")
                if not command_line.strip():
                    continue
                
                parts = command_line.strip().split()
                command = parts[0].lower()
                args = parts[1:]
                
                if command == "exit" or command == "quit":
                    print("Saliendo del CLI...")
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
                elif command == "cd":
                    self._handle_cd(args)
                else:
                    print(f"Comando desconocido: {command}")
                    print("Escribe 'help' para ver los comandos disponibles.")
            except KeyboardInterrupt:
                print("\nOperación cancelada.")
            except Exception as e:
                print(f"Error: {e}")
    
    def _show_help(self):
        """
        Muestra la ayuda de los comandos disponibles.
        """
        help_text = """
Comandos disponibles:
  put <archivo_local> <ruta_dfs>  - Sube un archivo al DFS
  get <ruta_dfs> <archivo_local>  - Descarga un archivo del DFS
  ls [ruta]                       - Lista el contenido de un directorio
  mkdir <ruta>                    - Crea un nuevo directorio
  cd <ruta>                       - Cambia el directorio actual
  help                            - Muestra esta ayuda
  exit, quit                      - Sale del CLI
"""
        print(help_text)
    
    def _handle_put(self, args: List[str]):
        """
        Maneja el comando put para subir archivos.
        
        Args:
            args: Argumentos del comando [archivo_local, ruta_dfs]
        """
        if len(args) != 2:
            print("Uso: put <archivo_local> <ruta_dfs>")
            return
        
        local_path, dfs_path = args
        
        # Convertir rutas relativas a absolutas para el archivo local
        local_path = os.path.abspath(local_path)
        
        # Manejar rutas relativas en el DFS
        if not dfs_path.startswith('/'):
            dfs_path = self._resolve_path(dfs_path)
        
        print(f"Subiendo {local_path} a {dfs_path}...")
        success = self.client.put_file(local_path, dfs_path)
        
        if success:
            print(f"Archivo subido exitosamente a {dfs_path}")
        else:
            print("Error al subir el archivo")
    
    def _handle_get(self, args: List[str]):
        """
        Maneja el comando get para descargar archivos.
        
        Args:
            args: Argumentos del comando [ruta_dfs, archivo_local]
        """
        if len(args) != 2:
            print("Uso: get <ruta_dfs> <archivo_local>")
            return
        
        dfs_path, local_path = args
        
        # Manejar rutas relativas en el DFS
        if not dfs_path.startswith('/'):
            dfs_path = self._resolve_path(dfs_path)
        
        # Convertir rutas relativas a absolutas para el archivo local
        local_path = os.path.abspath(local_path)
        
        print(f"Descargando {dfs_path} a {local_path}...")
        success = self.client.get_file(dfs_path, local_path)
        
        if success:
            print(f"Archivo descargado exitosamente a {local_path}")
        else:
            print("Error al descargar el archivo")
    
    def _handle_ls(self, args: List[str]):
        """
        Maneja el comando ls para listar directorios.
        
        Args:
            args: Argumentos del comando [ruta]
        """
        path = args[0] if args else self.current_dir
        
        # Manejar rutas relativas
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        try:
            directory_info = self.client.namenode_client.list_directory(path)
            
            if not directory_info:
                print(f"El directorio {path} no existe")
                return
            
            contents = directory_info.get('contents', [])
            
            if not contents:
                print(f"El directorio {path} está vacío")
                return
            
            # Formatear la salida
            print(f"Contenido de {path}:")
            print(f"{'Nombre':<30} {'Tipo':<10} {'Tamaño':<10}")
            print("-" * 50)
            
            for item in contents:
                item_type = item.get('type', 'desconocido')
                item_name = item.get('name', 'sin nombre')
                item_size = item.get('size', 0)
                
                size_str = f"{item_size} bytes" if item_type == 'file' else ""
                print(f"{item_name:<30} {item_type:<10} {size_str:<10}")
        
        except Exception as e:
            print(f"Error al listar el directorio {path}: {e}")
    
    def _handle_mkdir(self, args: List[str]):
        """
        Maneja el comando mkdir para crear directorios.
        
        Args:
            args: Argumentos del comando [ruta]
        """
        if not args:
            print("Uso: mkdir <ruta>")
            return
        
        path = args[0]
        
        # Manejar rutas relativas
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        # Obtener el nombre del directorio
        dir_name = os.path.basename(path)
        
        try:
            directory = {
                'name': dir_name,
                'path': path,
                'type': 'directory'
            }
            
            self.client.namenode_client.create_directory(directory)
            print(f"Directorio {path} creado exitosamente")
        
        except Exception as e:
            print(f"Error al crear el directorio {path}: {e}")
    
    def _handle_cd(self, args: List[str]):
        """
        Maneja el comando cd para cambiar de directorio.
        
        Args:
            args: Argumentos del comando [ruta]
        """
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
        try:
            directory_info = self.client.namenode_client.list_directory(new_path)
            
            if not directory_info:
                print(f"El directorio {new_path} no existe")
                return
            
            # Actualizar el directorio actual
            self.current_dir = new_path
        
        except Exception as e:
            print(f"Error al cambiar al directorio {new_path}: {e}")
    
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


def main():
    """
    Punto de entrada principal para el CLI.
    """
    parser = argparse.ArgumentParser(description='Cliente CLI para el Sistema de Archivos Distribuido')
    parser.add_argument('--namenode', type=str, default='http://localhost:8000',
                        help='URL del NameNode (default: http://localhost:8000)')
    parser.add_argument('--block-size', type=int, default=None,
                        help='Tamaño de bloque personalizado en bytes')
    
    args = parser.parse_args()
    
    cli = DFSCLI(args.namenode, args.block_size)
    cli.run()


if __name__ == "__main__":
    main()
