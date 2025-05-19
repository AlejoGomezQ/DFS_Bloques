"""
Módulo para compresión y descompresión de datos.
Optimiza la transferencia de bloques reduciendo su tamaño.
"""
import zlib
import lzma
import time
from typing import Tuple, Dict, Optional, Callable

# Niveles de compresión disponibles
COMPRESSION_NONE = 0
COMPRESSION_FAST = 1
COMPRESSION_BALANCED = 5
COMPRESSION_MAX = 9

# Algoritmos de compresión disponibles
ALGORITHM_ZLIB = "zlib"
ALGORITHM_LZMA = "lzma"

class DataCompressor:
    """
    Clase para comprimir y descomprimir datos con diferentes algoritmos.
    Permite seleccionar el mejor algoritmo según el tipo de datos.
    """
    
    def __init__(self, default_algorithm: str = ALGORITHM_ZLIB, 
                 default_level: int = COMPRESSION_BALANCED,
                 auto_select: bool = True):
        """
        Inicializa el compresor de datos.
        
        Args:
            default_algorithm: Algoritmo de compresión por defecto
            default_level: Nivel de compresión por defecto
            auto_select: Si es True, selecciona automáticamente el mejor algoritmo
        """
        self.default_algorithm = default_algorithm
        self.default_level = default_level
        self.auto_select = auto_select
        
        # Diccionario de funciones de compresión
        self.compressors = {
            ALGORITHM_ZLIB: self._compress_zlib,
            ALGORITHM_LZMA: self._compress_lzma
        }
        
        # Diccionario de funciones de descompresión
        self.decompressors = {
            ALGORITHM_ZLIB: self._decompress_zlib,
            ALGORITHM_LZMA: self._decompress_lzma
        }
    
    def compress(self, data: bytes, algorithm: Optional[str] = None, 
                level: Optional[int] = None) -> Tuple[bytes, Dict]:
        """
        Comprime los datos utilizando el algoritmo especificado.
        
        Args:
            data: Datos a comprimir
            algorithm: Algoritmo de compresión (None para usar el predeterminado)
            level: Nivel de compresión (None para usar el predeterminado)
            
        Returns:
            Tupla con los datos comprimidos y metadatos de compresión
        """
        if not data:
            return data, {"compressed": False, "algorithm": None, "ratio": 1.0}
        
        # Si no se especifica algoritmo, usar el predeterminado o auto-seleccionar
        if algorithm is None:
            if self.auto_select:
                algorithm = self._select_best_algorithm(data)
            else:
                algorithm = self.default_algorithm
        
        # Si no se especifica nivel, usar el predeterminado
        if level is None:
            level = self.default_level
        
        # Si el nivel es 0, no comprimir
        if level == COMPRESSION_NONE:
            return data, {"compressed": False, "algorithm": None, "ratio": 1.0}
        
        # Obtener la función de compresión
        compress_func = self.compressors.get(algorithm)
        if not compress_func:
            raise ValueError(f"Algoritmo de compresión no soportado: {algorithm}")
        
        # Comprimir los datos
        start_time = time.time()
        compressed_data = compress_func(data, level)
        compression_time = time.time() - start_time
        
        # Calcular la tasa de compresión
        original_size = len(data)
        compressed_size = len(compressed_data)
        compression_ratio = original_size / compressed_size if compressed_size > 0 else 1.0
        
        # Si la compresión no es efectiva, devolver los datos originales
        if compressed_size >= original_size:
            return data, {"compressed": False, "algorithm": None, "ratio": 1.0}
        
        # Devolver los datos comprimidos y metadatos
        return compressed_data, {
            "compressed": True,
            "algorithm": algorithm,
            "level": level,
            "original_size": original_size,
            "compressed_size": compressed_size,
            "ratio": compression_ratio,
            "time": compression_time
        }
    
    def decompress(self, data: bytes, metadata: Dict) -> bytes:
        """
        Descomprime los datos utilizando la información de los metadatos.
        
        Args:
            data: Datos comprimidos
            metadata: Metadatos de compresión
            
        Returns:
            Datos descomprimidos
        """
        # Si los datos no están comprimidos, devolverlos tal cual
        if not metadata.get("compressed", False):
            return data
        
        # Obtener el algoritmo de compresión
        algorithm = metadata.get("algorithm")
        if not algorithm:
            return data
        
        # Obtener la función de descompresión
        decompress_func = self.decompressors.get(algorithm)
        if not decompress_func:
            raise ValueError(f"Algoritmo de descompresión no soportado: {algorithm}")
        
        # Descomprimir los datos
        return decompress_func(data)
    
    def _select_best_algorithm(self, data: bytes) -> str:
        """
        Selecciona el mejor algoritmo de compresión para los datos.
        
        Args:
            data: Datos a comprimir
            
        Returns:
            Nombre del algoritmo seleccionado
        """
        # Para archivos pequeños, usar zlib que es más rápido
        if len(data) < 1024 * 1024:  # Menos de 1MB
            return ALGORITHM_ZLIB
        
        # Para archivos más grandes, probar ambos algoritmos con muestras
        sample_size = min(len(data), 64 * 1024)  # Máximo 64KB de muestra
        sample = data[:sample_size]
        
        best_algorithm = ALGORITHM_ZLIB
        best_ratio = 0
        
        # Probar cada algoritmo con la muestra
        for algorithm, compress_func in self.compressors.items():
            compressed = compress_func(sample, COMPRESSION_BALANCED)
            ratio = len(sample) / len(compressed) if len(compressed) > 0 else 0
            
            if ratio > best_ratio:
                best_ratio = ratio
                best_algorithm = algorithm
        
        return best_algorithm
    
    def _compress_zlib(self, data: bytes, level: int) -> bytes:
        """Comprime datos usando zlib."""
        return zlib.compress(data, level)
    
    def _decompress_zlib(self, data: bytes) -> bytes:
        """Descomprime datos usando zlib."""
        return zlib.decompress(data)
    
    def _compress_lzma(self, data: bytes, level: int) -> bytes:
        """Comprime datos usando lzma."""
        # Convertir nivel de compresión al rango de lzma (0-9)
        lzma_preset = min(9, max(0, level))
        return lzma.compress(data, preset=lzma_preset)
    
    def _decompress_lzma(self, data: bytes) -> bytes:
        """Descomprime datos usando lzma."""
        return lzma.decompress(data)
