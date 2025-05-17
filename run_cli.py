#!/usr/bin/env python3
import sys
import os

# Asegurar que el directorio raíz del proyecto esté en el path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.client.cli import main

if __name__ == "__main__":
    main()
