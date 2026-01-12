"""
Writer para exportar datos a formato CSV.
"""

from pathlib import Path
from typing import Dict, Union

import pyarrow as pa
import pyarrow.csv as pcsv

from load.writers.base import DataWriter


class CSVWriter(DataWriter):
    """
    Exporta tablas PyArrow a archivos CSV.

    Attributes:
        base_path: Directorio base donde se guardarán los CSV.
        delimiter: Separador de columnas (default: ',').
        include_header: Si incluir encabezados (default: True).
    """

    def __init__(
        self,
        base_path: Union[Path, str],
        delimiter: str = ",",
        include_header: bool = True,
    ):
        self.base_path = Path(base_path)
        self.delimiter = delimiter
        self.include_header = include_header
        self.base_path.mkdir(parents=True, exist_ok=True)

    def write(self, table: pa.Table, table_name: str) -> Union[Path, str]:
        """
        Escribe una tabla PyArrow a un archivo CSV.

        Args:
            table: Tabla PyArrow a escribir.
            table_name: Nombre del archivo (sin extensión).

        Returns:
            Path del archivo CSV creado.
        """
        file_path = self.base_path / f"{table_name}.csv"

        write_options = pcsv.WriteOptions(
            delimiter=self.delimiter,
            include_header=self.include_header,
        )

        pcsv.write_csv(table, file_path, write_options=write_options)

        return file_path

    def write_all(self, tables: Dict[str, pa.Table]) -> Dict[str, Union[Path, str]]:
        """
        Escribe múltiples tablas a archivos CSV.

        Args:
            tables: Diccionario {nombre_tabla: PyArrow Table}.

        Returns:
            Diccionario {nombre_tabla: Path del CSV creado}.
        """
        results = {}
        for table_name, table in tables.items():
            results[table_name] = self.write(table, table_name)
        return results
