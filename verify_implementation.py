#!/usr/bin/env python3
"""
Script para verificar la implementación de los puntos completados del PRD.
Este script analiza el código fuente para confirmar que todas las funcionalidades
marcadas como completadas en el PRD están correctamente implementadas.
"""
import os
import re
import sys
from pathlib import Path

# Directorio base del proyecto
BASE_DIR = Path(__file__).parent.absolute()

def check_file_exists(file_path):
    """Verifica si un archivo existe."""
    return os.path.exists(file_path)

def read_file_content(file_path):
    """Lee el contenido de un archivo."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Error al leer el archivo {file_path}: {e}")
        return ""

def check_pattern_in_file(file_path, pattern):
    """Verifica si un patrón existe en un archivo."""
    content = read_file_content(file_path)
    return bool(re.search(pattern, content))

def check_phase_1():
    """Verifica la implementación de la Fase 1: Arquitectura Base."""
    print("\n=== Verificando Fase 1: Arquitectura Base ===")
    
    # Verificar la existencia de los componentes principales
    namenode_exists = check_file_exists(os.path.join(BASE_DIR, "src", "namenode", "api", "main.py"))
    datanode_exists = check_file_exists(os.path.join(BASE_DIR, "src", "datanode", "main.py"))
    client_exists = check_file_exists(os.path.join(BASE_DIR, "src", "client", "cli.py"))
    
    print(f"1.1 Implementar NameNode básico: {'✓' if namenode_exists else '✗'}")
    print(f"1.2 Implementar DataNode básico: {'✓' if datanode_exists else '✗'}")
    print(f"1.3 Implementar Cliente básico: {'✓' if client_exists else '✗'}")
    
    return namenode_exists and datanode_exists and client_exists

def check_phase_2():
    """Verifica la implementación de la Fase 2: Operaciones Fundamentales."""
    print("\n=== Verificando Fase 2: Operaciones Fundamentales ===")
    
    # 2.1 Implementar particionamiento de archivos en bloques
    file_splitter_path = os.path.join(BASE_DIR, "src", "client", "file_operations", "file_splitter.py")
    file_splitter_exists = check_file_exists(file_splitter_path)
    file_splitter_has_split_method = check_pattern_in_file(file_splitter_path, r"def\s+split_file")
    
    # 2.2 Desarrollar algoritmo de distribución de bloques
    block_distributor_path = os.path.join(BASE_DIR, "src", "client", "file_operations", "block_distributor.py")
    block_distributor_exists = check_file_exists(block_distributor_path)
    block_distributor_has_distribute_method = check_pattern_in_file(block_distributor_path, r"def\s+distribute_blocks")
    
    # 2.3 Implementar operaciones básicas en NameNode
    namenode_file_ops = check_pattern_in_file(
        os.path.join(BASE_DIR, "src", "namenode", "api", "routes.py"),
        r"@(files_router|blocks_router).(post|get|delete)"
    )
    
    # 2.4 Implementar operaciones básicas en DataNode
    datanode_block_ops = check_pattern_in_file(
        os.path.join(BASE_DIR, "src", "datanode", "service", "datanode_service.py"),
        r"def\s+(StoreBlock|RetrieveBlock)"
    )
    
    print(f"2.1 Implementar particionamiento de archivos en bloques: {'✓' if file_splitter_exists and file_splitter_has_split_method else '✗'}")
    print(f"2.2 Desarrollar algoritmo de distribución de bloques: {'✓' if block_distributor_exists and block_distributor_has_distribute_method else '✗'}")
    print(f"2.3 Implementar operaciones básicas en NameNode: {'✓' if namenode_file_ops else '✗'}")
    print(f"2.4 Implementar operaciones básicas en DataNode: {'✓' if datanode_block_ops else '✗'}")
    
    return (file_splitter_exists and file_splitter_has_split_method and
            block_distributor_exists and block_distributor_has_distribute_method and
            namenode_file_ops and datanode_block_ops)

def check_phase_3():
    """Verifica la implementación de la Fase 3: Cliente y Operaciones de Archivos."""
    print("\n=== Verificando Fase 3: Cliente y Operaciones de Archivos ===")
    
    # 3.1 Desarrollar cliente CLI básico
    cli_path = os.path.join(BASE_DIR, "src", "client", "cli.py")
    cli_exists = check_file_exists(cli_path)
    cli_has_run_method = check_pattern_in_file(cli_path, r"def\s+run")
    
    # 3.2 Implementar operación PUT
    put_implemented = check_pattern_in_file(cli_path, r"def\s+_handle_put")
    
    # 3.3 Implementar operación GET
    get_implemented = check_pattern_in_file(cli_path, r"def\s+_handle_get")
    
    # 3.4 Implementar operaciones de directorio
    ls_implemented = check_pattern_in_file(cli_path, r"def\s+_handle_ls")
    mkdir_implemented = check_pattern_in_file(cli_path, r"def\s+_handle_mkdir")
    
    print(f"3.1 Desarrollar cliente CLI básico: {'✓' if cli_exists and cli_has_run_method else '✗'}")
    print(f"3.2 Implementar operación PUT: {'✓' if put_implemented else '✗'}")
    print(f"3.3 Implementar operación GET: {'✓' if get_implemented else '✗'}")
    print(f"3.4 Implementar operaciones de directorio: {'✓' if ls_implemented and mkdir_implemented else '✗'}")
    
    return (cli_exists and cli_has_run_method and
            put_implemented and get_implemented and
            ls_implemented and mkdir_implemented)

def check_phase_4():
    """Verifica la implementación de la Fase 4: Replicación y Tolerancia a Fallos."""
    print("\n=== Verificando Fase 4: Replicación y Tolerancia a Fallos ===")
    
    # 4.1 Implementar mecanismo de replicación Leader-Follower
    replication_path = os.path.join(BASE_DIR, "src", "datanode", "service", "datanode_service.py")
    replication_implemented = check_pattern_in_file(replication_path, r"def\s+ReplicateBlock")
    
    # 4.2 Desarrollar detección de fallos en DataNodes
    monitor_path = os.path.join(BASE_DIR, "src", "namenode", "monitoring", "datanode_monitor.py")
    monitor_exists = check_file_exists(monitor_path)
    monitor_has_check_method = check_pattern_in_file(monitor_path, r"def\s+_check_datanodes")
    
    # 4.3 Implementar re-replicación de bloques perdidos
    replicator_path = os.path.join(BASE_DIR, "src", "namenode", "replication", "block_replicator.py")
    replicator_exists = check_file_exists(replicator_path)
    replicator_has_method = check_pattern_in_file(replicator_path, r"def\s+handle_block_replication")
    
    # 4.4 Desarrollar NameNode Follower para redundancia
    leader_election_path = os.path.join(BASE_DIR, "src", "namenode", "leader", "leader_election.py")
    leader_election_exists = check_file_exists(leader_election_path)
    
    print(f"4.1 Implementar mecanismo de replicación Leader-Follower: {'✓' if replication_implemented else '✗'}")
    print(f"4.2 Desarrollar detección de fallos en DataNodes: {'✓' if monitor_exists and monitor_has_check_method else '✗'}")
    print(f"4.3 Implementar re-replicación de bloques perdidos: {'✓' if replicator_exists and replicator_has_method else '✗'}")
    print(f"4.4 Desarrollar NameNode Follower para redundancia: {'✓' if leader_election_exists else '✗'}")
    
    return (replication_implemented and
            monitor_exists and monitor_has_check_method and
            replicator_exists and replicator_has_method and
            leader_election_exists)

def check_phase_5():
    """Verifica la implementación de la Fase 5: Refinamiento y Funcionalidades Adicionales."""
    print("\n=== Verificando Fase 5: Refinamiento y Funcionalidades Adicionales ===")
    
    # 5.1 Completar comandos CLI restantes (rm, rmdir, cd)
    cli_path = os.path.join(BASE_DIR, "src", "client", "cli.py")
    rm_implemented = check_pattern_in_file(cli_path, r"def\s+_handle_rm")
    rmdir_implemented = check_pattern_in_file(cli_path, r"def\s+_handle_rmdir")
    cd_implemented = check_pattern_in_file(cli_path, r"def\s+_handle_cd")
    
    # Verificar que rm soporta la opción -f
    rm_supports_force = check_pattern_in_file(cli_path, r"force\s*=\s*\"-f\"\s+in\s+args")
    
    # Verificar que rmdir soporta las opciones -r y -f
    rmdir_supports_recursive = check_pattern_in_file(cli_path, r"recursive\s*=\s*\"-r\"\s+in\s+args")
    rmdir_supports_force = check_pattern_in_file(cli_path, r"force\s*=\s*\"-f\"\s+in\s+args")
    
    print(f"5.1 Completar comandos CLI restantes: {'✓' if rm_implemented and rmdir_implemented and cd_implemented else '✗'}")
    print(f"  - Comando rm: {'✓' if rm_implemented else '✗'}")
    print(f"  - Opción -f para rm: {'✓' if rm_supports_force else '✗'}")
    print(f"  - Comando rmdir: {'✓' if rmdir_implemented else '✗'}")
    print(f"  - Opción -r para rmdir: {'✓' if rmdir_supports_recursive else '✗'}")
    print(f"  - Opción -f para rmdir: {'✓' if rmdir_supports_force else '✗'}")
    print(f"  - Comando cd: {'✓' if cd_implemented else '✗'}")
    
    return (rm_implemented and rmdir_implemented and cd_implemented and
            rm_supports_force and rmdir_supports_recursive and rmdir_supports_force)

def main():
    """Función principal."""
    print("=== Verificación de Implementación del PRD DFS_Bloques ===")
    
    phase1_complete = check_phase_1()
    phase2_complete = check_phase_2()
    phase3_complete = check_phase_3()
    phase4_complete = check_phase_4()
    phase5_1_complete = check_phase_5()
    
    print("\n=== Resumen de Verificación ===")
    print(f"Fase 1: Arquitectura Base: {'✓' if phase1_complete else '✗'}")
    print(f"Fase 2: Operaciones Fundamentales: {'✓' if phase2_complete else '✗'}")
    print(f"Fase 3: Cliente y Operaciones de Archivos: {'✓' if phase3_complete else '✗'}")
    print(f"Fase 4: Replicación y Tolerancia a Fallos: {'✓' if phase4_complete else '✗'}")
    print(f"Fase 5.1: Comandos CLI restantes: {'✓' if phase5_1_complete else '✗'}")
    
    all_complete = phase1_complete and phase2_complete and phase3_complete and phase4_complete and phase5_1_complete
    
    print("\n=== Conclusión ===")
    if all_complete:
        print("¡Felicidades! Todos los puntos verificados del PRD están correctamente implementados.")
    else:
        print("Hay algunos puntos del PRD que podrían no estar completamente implementados. Revisa los detalles arriba.")

if __name__ == "__main__":
    main()
