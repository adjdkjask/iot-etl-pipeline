"""
Módulo de writers para la fase de carga.
"""

from load.writers.base import DataWriter
from load.writers.csv_writer import CSVWriter

__all__ = ["DataWriter", "CSVWriter"]
