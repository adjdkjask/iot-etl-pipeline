"""
Módulo orquestador de la fase de carga del ETL.
"""

from pathlib import Path
from typing import Dict, List, Optional, Union

import pyarrow.parquet as pq
import pyarrow as pa

from load.writers.base import DataWriter


class Loader:
    """
    Orquesta la fase de carga del ETL.

    Lee tablas Parquet del directorio de output y las escribe
    al destino configurado mediante el writer inyectado.

    Attributes:
        source_path: Directorio con los Parquet de salida (data/output).
        writer: Implementación de DataWriter para el destino.
    """

    def __init__(self, source_path: Union[Path, str], writer: DataWriter):
        """
        Inicializa el Loader.

        Args:
            source_path: Path al directorio con Parquet de salida.
            writer: Writer concreto para el destino (CSV, Postgres, etc.).
        """
        self.source_path = Path(source_path)
        self.writer = writer

    def list_tables(self) -> List[str]:
        """
        Lista las tablas disponibles en el directorio de salida.

        Returns:
            Lista de nombres de tablas (subdirectorios con Parquet).
        """
        if not self.source_path.exists():
            return []

        return [
            d.name
            for d in self.source_path.iterdir()
            if d.is_dir() and self._has_parquet_files(d)
        ]

    def _has_parquet_files(self, directory: Path) -> bool:
        """Verifica si un directorio contiene archivos Parquet."""
        return any(directory.glob("*.parquet")) or any(directory.glob("**/*.parquet"))

    def _read_table(self, table_name: str) -> pa.Table:
        """
        Lee una tabla Parquet del directorio de salida.

        Args:
            table_name: Nombre de la tabla (subdirectorio).

        Returns:
            PyArrow Table con los datos.

        Raises:
            FileNotFoundError: Si la tabla no existe.
        """
        table_path = self.source_path / table_name

        if not table_path.exists():
            raise FileNotFoundError(
                f"Tabla '{table_name}' no encontrada en {self.source_path}"
            )

        # Lee todo el directorio como dataset (soporta particiones)
        return pq.read_table(table_path)

    def load(self, tables: Optional[List[str]] = None) -> Dict[str, Union[Path, str]]:
        """
        Carga las tablas especificadas al destino configurado.

        Args:
            tables: Lista de nombres de tablas a cargar.
                   Si es None, carga todas las tablas disponibles.

        Returns:
            Diccionario {nombre_tabla: path/identificador destino}.
        """
        if tables is None:
            tables = self.list_tables()

        loaded_tables: Dict[str, pa.Table] = {}
        for table_name in tables:
            loaded_tables[table_name] = self._read_table(table_name)

        return self.writer.write_all(loaded_tables)

    def load_single(self, table_name: str) -> Union[Path, str]:
        """
        Carga una única tabla al destino.

        Args:
            table_name: Nombre de la tabla a cargar.

        Returns:
            Path o identificador del destino.
        """
        table = self._read_table(table_name)
        return self.writer.write(table, table_name)
