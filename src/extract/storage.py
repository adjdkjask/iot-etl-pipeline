"""
Módulo que se encarga del guardado de datos en bruto.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq


class RawStorage:
    """
    Clase que maneja el almacenamiento de datos en bruto
    """

    def __init__(self, base_path: Path):
        self.base_path = base_path

    def save(self, file_name: str, payload: List[Dict[str, Any]]) -> Path:
        """
        Guarda los datos en bruto en formato Parquet.

        Args:
            file_name: Nombre de la tabla/archivo.
            payload: Lista de diccionarios (registros).
        """
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        file_dir = self.base_path / file_name
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / f"{timestamp}.parquet"
        table = pa.Table.from_pylist(payload)
        pq.write_table(table, file_path)

        return file_path
