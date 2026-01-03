"""
Módulo con lógica de extracción de API.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional

from extract.storage import RawStorage
from extract.http_client import RequestsHttpClient

# Configuración de la API
load_dotenv()
USER_EMAIL = os.getenv("USER_EMAIL")
API_BASE_URL = os.getenv("API_BASE_URL")
API_KEY = os.getenv("API_KEY")
DATASET_TYPE = os.getenv("DATASET_TYPE")
ROWS = os.getenv("ROWS")
API_URL = (
    f"{API_BASE_URL}?email={USER_EMAIL}&key={API_KEY}&type={DATASET_TYPE}&rows={ROWS}"
)

# Configuración de guardado
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"


class Extractor:
    """
    Clase que se encarga de la extracción de información
    dentro del flujo ETL.
    """

    def __init__(self):
        self.client = RequestsHttpClient(url=API_URL)
        self.storage = RawStorage(base_path=RAW_DATA_DIR)

    def extract(self) -> Dict[str, Path]:
        """
        Extrae y guarda todas las tablas de interés.
        Retorna un diccionario con los paths de los archivos guardados.
        """
        payload = self.client.get()
        saved_files = {}

        for table_name in payload.get("tables", {}).keys():
            table_data = self._get_table_from_payload(payload, table_name)
            if table_data:
                file_path = self.storage.save(
                    file_name=table_name,
                    payload=table_data,
                )
                saved_files[table_name] = file_path

        return saved_files

    def _get_table_from_payload(
        self, payload: Dict[str, Any], table_name: str
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Extrae una tabla específica del payload.
        """
        tables = payload.get("tables", {})
        return tables.get(table_name)
