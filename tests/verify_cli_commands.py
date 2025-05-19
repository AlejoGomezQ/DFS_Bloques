#!/usr/bin/env python3
"""
Script para verificar la implementación de los comandos CLI (rm, rmdir, cd).
"""
import os
import re
from pathlib import Path

# Directorio base del proyecto
BASE_DIR = Path(__file__).parent.absolute()

def read_file_content(file_path):
    """Lee el contenido de un archivo."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Error al leer el archivo {file_path}: {e}")
        return ""

def check_cli_commands():
    """Verifica la implementación de los comandos CLI (rm, rmdir, cd)."""
    print("=== Verificando Comandos CLI (rm, rmdir, cd) ===")
    
    cli_path = os.path.join(BASE_DIR, "src", "client", "cli.py")
    
    if not os.path.exists(cli_path):
        print(f"Error: No se encontró el archivo {cli_path}")
        return False
    
    content = read_file_content(cli_path)
    
    # Verificar la implementación de los comandos
    rm_implemented = bool(re.search(r"def\s+_handle_rm\s*\(", content))
    rmdir_implemented = bool(re.search(r"def\s+_handle_rmdir\s*\(", content))
    cd_implemented = bool(re.search(r"def\s+_handle_cd\s*\(", content))
    
    # Verificar las opciones de los comandos
    rm_force_option = bool(re.search(r"force\s*=\s*\"-f\"\s+in\s+args", content))
    rmdir_recursive_option = bool(re.search(r"recursive\s*=\s*\"-r\"\s+in\s+args", content))
    rmdir_force_option = bool(re.search(r"force\s*=\s*\"-f\"\s+in\s+args", content))
    
    # Verificar que los comandos están registrados en el bucle principal
    rm_registered = bool(re.search(r"elif\s+command\s*==\s*\"rm\"", content))
    rmdir_registered = bool(re.search(r"elif\s+command\s*==\s*\"rmdir\"", content))
    cd_registered = bool(re.search(r"elif\s+command\s*==\s*\"cd\"", content))
    
    # Verificar que los comandos están documentados en la ayuda
    help_includes_rm = bool(re.search(r"rm\s+<ruta>\s+\[-f\]", content))
    help_includes_rmdir = bool(re.search(r"rmdir\s+<ruta>\s+\[-r\]\s+\[-f\]", content))
    help_includes_cd = bool(re.search(r"cd\s+<ruta>", content))
    
    # Mostrar resultados
    print(f"Comando rm implementado: {'✓' if rm_implemented else '✗'}")
    print(f"Opción -f para rm: {'✓' if rm_force_option else '✗'}")
    print(f"Comando rmdir implementado: {'✓' if rmdir_implemented else '✗'}")
    print(f"Opción -r para rmdir: {'✓' if rmdir_recursive_option else '✗'}")
    print(f"Opción -f para rmdir: {'✓' if rmdir_force_option else '✗'}")
    print(f"Comando cd implementado: {'✓' if cd_implemented else '✗'}")
    print(f"Comandos registrados en el bucle principal: {'✓' if rm_registered and rmdir_registered and cd_registered else '✗'}")
    print(f"Comandos documentados en la ayuda: {'✓' if help_includes_rm and help_includes_rmdir and help_includes_cd else '✗'}")
    
    # Verificar la implementación completa
    implementation_complete = (
        rm_implemented and rm_force_option and
        rmdir_implemented and rmdir_recursive_option and rmdir_force_option and
        cd_implemented and
        rm_registered and rmdir_registered and cd_registered and
        help_includes_rm and help_includes_rmdir and help_includes_cd
    )
    
    print(f"\nImplementación del punto 5.1 (Comandos CLI restantes): {'✓' if implementation_complete else '✗'}")
    
    return implementation_complete

if __name__ == "__main__":
    check_cli_commands()
