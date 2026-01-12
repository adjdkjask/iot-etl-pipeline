"""
Clase base abstracta para writers de datos.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Union

import pyarrow as pa


class DataWriter(ABC):
    """
    Interfaz abstracta para escritura de datos a diferentes destinos.
    """

    @abstractmethod
    def write(self, table: pa.Table, table_name: str) -> Union[Path, str]:
        """
        Escribe una tabla a su destino.

        Args:
            table: Tabla PyArrow a escribir.
            table_name: Nombre de la tabla (usado para nombrar archivo/tabla destino).

        Returns:
            Path del archivo creado o identificador del destino.
        """
        pass

    @abstractmethod
    def write_all(self, tables: Dict[str, pa.Table]) -> Dict[str, Union[Path, str]]:
        """
        Escribe múltiples tablas a su destino.

        Args:
            tables: Diccionario {nombre_tabla: PyArrow Table}.

        Returns:
            Diccionario {nombre_tabla: path/identificador destino}.
        """
        pass
