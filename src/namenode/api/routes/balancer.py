"""
Router para operaciones de balanceo de carga entre DataNodes.
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, List

from src.namenode.api.main import get_metadata_manager
from src.namenode.balancer.load_balancer import LoadBalancer

router = APIRouter()

# Variable global para el balanceador de carga
_load_balancer = None

def get_load_balancer():
    """
    Obtiene la instancia del balanceador de carga.
    Si no existe, la crea utilizando el gestor de metadatos.
    """
    global _load_balancer
    if _load_balancer is None:
        metadata_manager = get_metadata_manager()
        if metadata_manager is None:
            raise HTTPException(status_code=500, detail="Metadata manager not initialized")
        
        _load_balancer = LoadBalancer(metadata_manager)
    
    return _load_balancer

@router.get("/status")
async def get_balance_status(
    load_balancer: LoadBalancer = Depends(get_load_balancer)
) -> Dict[str, Any]:
    """
    Obtiene el estado actual del balanceo de carga.
    
    Returns:
        Dict con información sobre el estado de balanceo de los DataNodes
    """
    return load_balancer.get_balance_status()

@router.post("/start")
async def start_balancing(
    load_balancer: LoadBalancer = Depends(get_load_balancer)
) -> Dict[str, Any]:
    """
    Inicia manualmente el proceso de balanceo de carga.
    
    Returns:
        Dict con el resultado del proceso de balanceo
    """
    return load_balancer.balance_datanodes()

@router.post("/auto/enable")
async def enable_auto_balancing(
    load_balancer: LoadBalancer = Depends(get_load_balancer)
) -> Dict[str, str]:
    """
    Habilita el balanceo automático de carga.
    
    Returns:
        Dict con mensaje de confirmación
    """
    load_balancer.auto_balance = True
    load_balancer.start()
    return {"status": "success", "message": "Auto-balancing enabled"}

@router.post("/auto/disable")
async def disable_auto_balancing(
    load_balancer: LoadBalancer = Depends(get_load_balancer)
) -> Dict[str, str]:
    """
    Deshabilita el balanceo automático de carga.
    
    Returns:
        Dict con mensaje de confirmación
    """
    load_balancer.auto_balance = False
    load_balancer.stop()
    return {"status": "success", "message": "Auto-balancing disabled"}

@router.get("/config")
async def get_balancer_config(
    load_balancer: LoadBalancer = Depends(get_load_balancer)
) -> Dict[str, Any]:
    """
    Obtiene la configuración actual del balanceador de carga.
    
    Returns:
        Dict con la configuración actual
    """
    return {
        "balance_threshold": load_balancer.balance_threshold,
        "check_interval": load_balancer.check_interval,
        "auto_balance": load_balancer.auto_balance,
        "last_balance_time": load_balancer.last_balance_time,
        "balance_in_progress": load_balancer.balance_in_progress
    }

@router.post("/config")
async def update_balancer_config(
    config: Dict[str, Any],
    load_balancer: LoadBalancer = Depends(get_load_balancer)
) -> Dict[str, Any]:
    """
    Actualiza la configuración del balanceador de carga.
    
    Args:
        config: Dict con los parámetros a actualizar
        
    Returns:
        Dict con la configuración actualizada
    """
    if "balance_threshold" in config:
        threshold = float(config["balance_threshold"])
        if 0.0 <= threshold <= 1.0:
            load_balancer.balance_threshold = threshold
        else:
            raise HTTPException(
                status_code=400, 
                detail="balance_threshold must be between 0.0 and 1.0"
            )
    
    if "check_interval" in config:
        interval = int(config["check_interval"])
        if interval >= 60:  # Al menos 1 minuto
            load_balancer.check_interval = interval
        else:
            raise HTTPException(
                status_code=400, 
                detail="check_interval must be at least 60 seconds"
            )
    
    if "auto_balance" in config:
        auto_balance = bool(config["auto_balance"])
        if auto_balance and not load_balancer.auto_balance:
            # Habilitar auto-balanceo
            load_balancer.auto_balance = True
            load_balancer.start()
        elif not auto_balance and load_balancer.auto_balance:
            # Deshabilitar auto-balanceo
            load_balancer.auto_balance = False
            load_balancer.stop()
    
    return {
        "balance_threshold": load_balancer.balance_threshold,
        "check_interval": load_balancer.check_interval,
        "auto_balance": load_balancer.auto_balance,
        "last_balance_time": load_balancer.last_balance_time,
        "balance_in_progress": load_balancer.balance_in_progress
    }
