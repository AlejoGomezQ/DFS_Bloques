#!/usr/bin/env python3
import argparse
import os
import sys
import time
import datetime
from typing import List, Dict, Optional, Tuple

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
                elif command == "rmdir":
                    self._handle_rmdir(args)
                elif command == "cd":
                    self._handle_cd(args)
                elif command == "rm":
                    self._handle_rm(args)
                elif command == "pwd":
                    print(f"Directorio actual: {self.current_dir}")
                elif command == "info":
                    self._handle_info(args)
                elif command == "status":
                    self._handle_status()
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
  put <archivo_local> <ruta_dfs> [--workers=N]  - Sube un archivo al DFS
  get <ruta_dfs> <archivo_local> [--workers=N]  - Descarga un archivo del DFS
  ls [ruta] [-l]                               - Lista el contenido de un directorio
  mkdir <ruta> [-p]                            - Crea un nuevo directorio
  rmdir <ruta> [-r] [-f]                       - Elimina un directorio ([-r] recursivamente)
  rm <ruta> [-f]                               - Elimina un archivo
  cd <ruta>                                    - Cambia el directorio actual
  pwd                                          - Muestra el directorio actual
  info <ruta>                                  - Muestra información detallada de un archivo
  status                                       - Muestra el estado del sistema
  help                                         - Muestra esta ayuda
  exit, quit                                   - Sale del CLI

Opciones:
  --workers=N  - Número de trabajadores para operaciones paralelas (1-16)
  -l           - Formato largo para listar directorios
  -p           - Crear directorios padres si no existen
"""
        print(help_text)
    
    def _handle_put(self, args: List[str]):
        """
        Maneja el comando put para subir archivos.
        
        Args:
            args: Argumentos del comando [archivo_local, ruta_dfs]
        """
        if len(args) < 2:
            print("Uso: put <archivo_local> <ruta_dfs> [--workers=N]")
            return
        
        # Procesar argumentos
        local_path = args[0]
        dfs_path = args[1]
        max_workers = 4  # Valor por defecto
        
        # Procesar argumentos opcionales
        for arg in args[2:]:
            if arg.startswith("--workers="):
                try:
                    max_workers = int(arg.split("=")[1])
                    if max_workers < 1:
                        max_workers = 1
                    elif max_workers > 16:
                        max_workers = 16
                except (ValueError, IndexError):
                    print("Advertencia: Valor inválido para workers, usando valor por defecto (4)")
                    max_workers = 4
        
        # Convertir rutas relativas a absolutas para el archivo local
        local_path = os.path.abspath(local_path)
        
        # Verificar que el archivo existe
        if not os.path.exists(local_path):
            print(f"Error: El archivo local {local_path} no existe")
            return
        elif not os.path.isfile(local_path):
            print(f"Error: {local_path} no es un archivo")
            return
        
        # Manejar rutas relativas en el DFS
        if not dfs_path.startswith('/'):
            dfs_path = self._resolve_path(dfs_path)
        
        # Mostrar información del archivo
        file_size = os.path.getsize(local_path)
        print(f"\nIniciando subida de archivo:")
        print(f"  Local: {local_path}")
        print(f"  DFS:   {dfs_path}")
        print(f"  Tamaño: {self._format_size(file_size)}")
        print(f"  Workers: {max_workers}")
        print("-" * 50)
        
        try:
            # Iniciar la subida
            start_time = time.time()
            success = self.client.put_file(local_path, dfs_path, max_workers)
            end_time = time.time()
            
            print("-" * 50)
            if success:
                print(f"Operación completada en {end_time - start_time:.2f} segundos")
            else:
                print("La operación falló o se completó con errores")
        except Exception as e:
            print(f"Error al subir el archivo: {str(e)}")
            print("-" * 50)
            print("La operación falló o se completó con errores")
    
    def _format_size(self, size_bytes: int) -> str:
        """
        Formatea un tamaño en bytes a una representación legible.
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024 or unit == 'TB':
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
    
    def _handle_get(self, args: List[str]):
        """
        Maneja el comando get para descargar archivos.
        
        Args:
            args: Argumentos del comando [ruta_dfs, archivo_local]
        """
        if len(args) < 2:
            print("Uso: get <ruta_dfs> <archivo_local> [--workers=N]")
            return
        
        # Procesar argumentos
        dfs_path = args[0]
        local_path = args[1]
        max_workers = 4  # Valor por defecto
        
        # Procesar argumentos opcionales
        for arg in args[2:]:
            if arg.startswith("--workers="):
                try:
                    max_workers = int(arg.split("=")[1])
                    if max_workers < 1:
                        max_workers = 1
                    elif max_workers > 16:
                        max_workers = 16
                except (ValueError, IndexError):
                    print("Advertencia: Valor inválido para workers, usando valor por defecto (4)")
                    max_workers = 4
        
        # Manejar rutas relativas en el DFS
        if not dfs_path.startswith('/'):
            dfs_path = self._resolve_path(dfs_path)
        
        # Convertir rutas relativas a absolutas para el archivo local
        local_path = os.path.abspath(local_path)
        
        # Verificar si el directorio destino existe
        dest_dir = os.path.dirname(local_path)
        if not os.path.exists(dest_dir):
            try:
                os.makedirs(dest_dir, exist_ok=True)
                print(f"Directorio destino creado: {dest_dir}")
            except Exception as e:
                print(f"Error al crear el directorio destino: {e}")
                return
        
        # Verificar si el archivo destino ya existe
        if os.path.exists(local_path):
            overwrite = input(f"El archivo {local_path} ya existe. ¿Desea sobrescribirlo? (s/n): ").lower()
            if overwrite != 's':
                print("Operación cancelada")
                return
        
        print(f"\nIniciando descarga de archivo:")
        print(f"  DFS:   {dfs_path}")
        print(f"  Local: {local_path}")
        print(f"  Workers: {max_workers}")
        print("-" * 50)
        
        # Iniciar la descarga
        start_time = time.time()
        success = self.client.get_file(dfs_path, local_path, max_workers)
        end_time = time.time()
        
        print("-" * 50)
        if success:
            print(f"Operación completada en {end_time - start_time:.2f} segundos")
        else:
            print("La operación falló o se completó con errores")
    
    def _handle_ls(self, args: List[str]):
        """
        Maneja el comando ls para listar directorios.
        
        Args:
            args: Argumentos del comando [ruta] [-l]
        """
        # Procesar argumentos
        path = self.current_dir
        long_format = False
        
        for arg in args:
            if arg == '-l':
                long_format = True
            elif not arg.startswith('-'):
                path = arg
        
        # Manejar rutas relativas
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        try:
            directory_info = self.client.namenode_client.list_directory(path)
            
            if not directory_info:
                print(f"Error: El directorio {path} no existe")
                return
            
            contents = directory_info.get('contents', [])
            
            if not contents:
                print(f"El directorio {path} está vacío")
                return
            
            # Ordenar contenido: primero directorios, luego archivos, ambos alfabéticamente
            # Asegurarse de usar solo el nombre del archivo/directorio para ordenar
            def get_sort_key(item):
                name = item.get('name', '')
                if '/' in name:
                    name = name.split('/')[-1]
                return (0 if item.get('type') == 'directory' else 1, name.lower())
            
            contents.sort(key=get_sort_key)
            
            # Formatear la salida
            if long_format:
                self._print_long_format(path, contents)
            else:
                self._print_short_format(path, contents)
        
        except Exception as e:
            print(f"Error al listar el directorio {path}: {e}")
    
    def _print_short_format(self, path: str, contents: List[Dict]):
        """
        Imprime el contenido de un directorio en formato corto.
        """
        print(f"Contenido de {path}:")
        
        # Calcular el ancho máximo para formatear en columnas
        max_name_length = max([len(item.get('name', '')) for item in contents] + [10])
        col_width = max_name_length + 4  # Añadir espacio extra
        term_width = 80  # Ancho por defecto de la terminal
        cols = max(1, term_width // col_width)
        
        # Imprimir en columnas
        for i, item in enumerate(contents):
            item_type = item.get('type', 'desconocido')
            item_name = item.get('name', 'sin nombre')
            
            # Asegurarse de que el nombre no incluya la ruta completa
            if '/' in item_name:
                item_name = item_name.split('/')[-1]
            
            # Colorear según el tipo (directorios en azul, archivos en blanco)
            if item_type == 'directory':
                formatted_name = f"\033[1;34m{item_name}/\033[0m"  # Azul para directorios
            else:
                formatted_name = item_name
            
            # Imprimir en columnas
            print(f"{formatted_name:{col_width}}", end='')
            if (i + 1) % cols == 0 or i == len(contents) - 1:
                print()  # Nueva línea al final de cada fila
    
    def _print_long_format(self, path: str, contents: List[Dict]):
        """
        Imprime el contenido de un directorio en formato largo.
        """
        print(f"Contenido de {path}:")
        print(f"{'Tipo':<12} {'Tamaño':<10} {'Fecha Mod.':<20} {'Nombre'}")
        print("-" * 80)
        
        for item in contents:
            item_type = item.get('type', 'desconocido')
            item_name = item.get('name', 'sin nombre')
            item_size = item.get('size', 0)
            item_modified = item.get('modified_at', None)
            
            # Asegurarse de que el nombre no incluya la ruta completa
            if '/' in item_name:
                item_name = item_name.split('/')[-1]
            
            # Formatear tamaño
            if item_type == 'file':
                size_str = self._format_size(item_size)
            else:
                size_str = "-"
            
            # Formatear fecha de modificación
            if item_modified:
                try:
                    date_str = datetime.datetime.fromtimestamp(item_modified).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    date_str = "-"
            else:
                date_str = "-"
            
            # Colorear según el tipo
            if item_type == 'directory':
                type_str = "\033[1;34mDirectorio\033[0m"  # Azul para directorios
                formatted_name = f"\033[1;34m{item_name}/\033[0m"  # Azul para directorios
            else:
                type_str = "Archivo"
                formatted_name = item_name
            
            print(f"{type_str:<12} {size_str:<10} {date_str:<20} {formatted_name}")
    
    def _handle_mkdir(self, args: List[str]):
        """
        Maneja el comando mkdir para crear directorios.
        
        Args:
            args: Argumentos del comando [ruta] [-p]
        """
        if not args:
            print("Uso: mkdir <ruta> [-p]")
            return
        
        # Procesar argumentos
        create_parents = False
        path = None
        
        for arg in args:
            if arg == '-p':
                create_parents = True
            elif not arg.startswith('-'):
                path = arg
        
        if not path:
            print("Error: Debe especificar una ruta")
            return
        
        # Manejar rutas relativas
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        try:
            if create_parents:
                # Crear directorios padres recursivamente
                if self._create_directory_recursive(path):
                    print(f"Directorio {path} y sus padres creados exitosamente")
                else:
                    print(f"Error al crear el directorio {path}")
            else:
                # Verificar que el directorio padre existe
                parent_dir = os.path.dirname(path)
                if parent_dir != '/' and parent_dir:
                    parent_info = self.client.namenode_client.list_directory(parent_dir)
                    if not parent_info:
                        print(f"Error: El directorio padre {parent_dir} no existe")
                        print("Use 'mkdir -p' para crear los directorios padres automáticamente")
                        return
                
                # Obtener el nombre del directorio (solo el último componente)
                dir_name = os.path.basename(path)
                
                # Crear el directorio
                directory = {
                    'name': dir_name,
                    'path': path,
                    'type': 'directory'
                }
                
                try:
                    self.client.namenode_client.create_directory(directory)
                    print(f"Directorio {path} creado exitosamente")
                except Exception as e:
                    if "already exists" in str(e):
                        print(f"Error: El directorio {path} ya existe")
                    else:
                        raise e
        
        except Exception as e:
            print(f"Error al crear el directorio {path}: {e}")
    
    def _create_directory_recursive(self, path: str) -> bool:
        """
        Crea un directorio y todos sus directorios padres si no existen.
        
        Args:
            path: Ruta del directorio a crear
            
        Returns:
            True si se creó exitosamente, False en caso contrario
        """
        # Caso base: directorio raíz
        if path == '/':
            return True
        
        # Verificar si el directorio ya existe
        try:
            dir_info = self.client.namenode_client.list_directory(path)
            if dir_info:
                return True  # El directorio ya existe
        except Exception:
            pass  # Ignorar errores, intentaremos crear el directorio
        
        # Crear el directorio padre recursivamente
        parent_dir = os.path.dirname(path)
        if parent_dir and parent_dir != path:
            if not self._create_directory_recursive(parent_dir):
                return False
        
        # Crear el directorio actual
        try:
            dir_name = os.path.basename(path)
            directory = {
                'name': dir_name,
                'path': path,
                'type': 'directory'
            }
            self.client.namenode_client.create_directory(directory)
            return True
        except Exception as e:
            print(f"Error al crear el directorio {path}: {e}")
            return False
    
    def _handle_rmdir(self, args: List[str]):
        """
        Maneja el comando rmdir para eliminar directorios.
        
        Args:
            args: Argumentos del comando [ruta] [-r] [-f]
        """
        if not args:
            print("Uso: rmdir <ruta> [-r] [-f]")
            print("  -r: Eliminar recursivamente (incluyendo contenido)")
            print("  -f: Forzar eliminación sin confirmación")
            return
        
        # Procesar argumentos
        # Filtrar los argumentos para separar la ruta de las opciones
        path_args = [arg for arg in args if not arg.startswith('-')]
        if not path_args:
            print("Error: Debe especificar una ruta")
            return
        
        path = path_args[0]
        recursive = "-r" in args
        force = "-f" in args
        
        # Resolver la ruta completa
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        # Verificar que no se intente eliminar el directorio raíz
        if path == '/':
            print("Error: No se puede eliminar el directorio raíz")
            return
        
        try:
            # Verificar que el directorio existe y obtener la ruta completa
            dir_info = None
            try:
                dir_info = self.client.namenode_client.list_directory(path)
            except Exception:
                # Intentar buscar el directorio en el directorio actual
                current_dir_info = self.client.namenode_client.list_directory(self.current_dir)
                if current_dir_info and 'contents' in current_dir_info:
                    for item in current_dir_info['contents']:
                        if item['name'] == path.strip('/') and item['type'] == 'directory':
                            path = os.path.join(self.current_dir, path).replace('\\', '/')
                            if not path.startswith('/'):
                                path = '/' + path
                            dir_info = self.client.namenode_client.list_directory(path)
                            break
            
            if not dir_info:
                print(f"Error: El directorio {path} no existe")
                return
            
            # Confirmar la eliminación si no se usa -f
            if not force:
                try:
                    if recursive and dir_info.get('contents', []):
                        confirm = input(f"¿Desea eliminar {path} y todo su contenido? (s/n): ")
                    else:
                        confirm = input(f"¿Desea eliminar {path}? (s/n): ")
                    
                    if confirm.lower() != "s":
                        print("Operación cancelada")
                        return
                except (KeyboardInterrupt, EOFError):
                    print("\nOperación cancelada")
                    return
            
            # Eliminar el directorio
            try:
                self.client.namenode_client.delete_directory(path, recursive=recursive)
                print(f"Directorio {path} eliminado exitosamente")
            except Exception as e:
                print(f"Error al eliminar el directorio: {e}")
                return
            
            # Si estamos en el directorio que se eliminó, volver al directorio padre
            if self.current_dir.startswith(path):
                self.current_dir = os.path.dirname(path) or "/"
                print(f"Directorio actual cambiado a: {self.current_dir}")
        
        except Exception as e:
            print(f"Error al eliminar el directorio {path}: {e}")
    
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
    
    def _handle_rm(self, args: List[str]):
        """
        Maneja el comando rm para eliminar archivos.
        
        Args:
            args: Argumentos del comando [ruta] [-f]
        """
        if not args:
            print("Uso: rm <ruta> [-f]")
            return
        
        # Procesar argumentos
        path = args[0]
        force = "-f" in args
        
        # Resolver la ruta completa
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        # Confirmar la eliminación si no se usa -f
        if not force:
            confirm = input(f"\u00bfEstá seguro de que desea eliminar {path}? (s/n): ")
            if confirm.lower() != "s":
                print("Operación cancelada")
                return
        
        # Eliminar el archivo
        try:
            success = self.client.delete_file(path)
            if not success:
                print(f"No se pudo eliminar {path}")
        except Exception as e:
            print(f"Error al eliminar {path}: {e}")
    
    def _handle_info(self, args: List[str]):
        """
        Maneja el comando info para mostrar información detallada de un archivo.
        
        Args:
            args: Argumentos del comando [ruta]
        """
        if not args:
            print("Uso: info <ruta>")
            return
        
        path = args[0]
        
        # Manejar rutas relativas
        if not path.startswith('/'):
            path = self._resolve_path(path)
        
        try:
            # Obtener información del archivo
            file_info = self.client.namenode_client.get_file_info(path)
            if not file_info:
                print(f"Error: El archivo {path} no existe")
                return
            
            # Obtener información de los bloques
            blocks_info = self.client.namenode_client.get_file_blocks(path)
            
            # Mostrar información
            print("\nInformación del archivo:")
            print(f"  Ruta: {path}")
            print(f"  Tamaño: {self._format_size(file_info.get('size', 0))}")
            print(f"  Creado: {file_info.get('created_at', 'N/A')}")
            print(f"  Modificado: {file_info.get('modified_at', 'N/A')}")
            
            print("\nBloques:")
            for block in blocks_info:
                print(f"\n  Bloque: {block['block_id']}")
                print(f"  Tamaño: {self._format_size(block.get('size', 0))}")
                print("  Ubicaciones:")
                for location in block.get('locations', []):
                    leader_status = "✓ (Leader)" if location.get('is_leader') else "✓"
                    print(f"    - DataNode {location['datanode_id']}: {leader_status}")
        
        except Exception as e:
            print(f"Error al obtener información: {e}")
    
    def _handle_status(self):
        """
        Maneja el comando status para mostrar el estado del sistema.
        """
        try:
            # Obtener información de los DataNodes
            datanodes = self.client.namenode_client.list_datanodes()
            
            # Obtener estadísticas del sistema
            stats = self.client.namenode_client.get_system_stats()
            
            print("\nEstado del Sistema de Archivos Distribuido")
            print("=" * 50)
            
            # Mostrar información del NameNode
            print("\nNameNode:")
            print(f"  URL: {self.client.namenode_client.base_url}")
            print(f"  Estado: {'✓ Activo' if stats.get('namenode_active', False) else '✗ Inactivo'}")
            
            # Mostrar información de DataNodes
            print("\nDataNodes:")
            active_nodes = 0
            total_capacity = 0
            total_available = 0
            
            for node in datanodes:
                status = node.get('status', 'UNKNOWN')
                if status == 'ACTIVE':
                    active_nodes += 1
                
                capacity = node.get('storage_capacity', 0)
                available = node.get('available_space', 0)
                total_capacity += capacity
                total_available += available
                
                print(f"\n  DataNode {node.get('node_id', 'N/A')}:")
                print(f"    Estado: {'✓ Activo' if status == 'ACTIVE' else '✗ Inactivo'}")
                print(f"    Almacenamiento: {self._format_size(available)} libre de {self._format_size(capacity)}")
                print(f"    Bloques almacenados: {node.get('blocks_stored', 0)}")
                print(f"    Último heartbeat: {node.get('last_heartbeat', 'N/A')}")
            
            # Mostrar estadísticas generales
            print("\nEstadísticas Generales:")
            print(f"  DataNodes activos: {active_nodes} de {len(datanodes)}")
            print(f"  Capacidad total: {self._format_size(total_capacity)}")
            print(f"  Espacio disponible: {self._format_size(total_available)}")
            print(f"  Archivos totales: {stats.get('total_files', 0)}")
            print(f"  Bloques totales: {stats.get('total_blocks', 0)}")
            print(f"  Factor de replicación: {stats.get('replication_factor', 2)}")
        
        except Exception as e:
            print(f"Error al obtener el estado del sistema: {e}")
    
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
