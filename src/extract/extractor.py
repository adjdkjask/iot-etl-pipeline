"""
Módulo con lógica de extracción de API.
"""

from pathlib import Path
from typing import Dict, Any, List, Optional

from extract.storage import RawStorage
from extract.http_client import RequestsHttpClient


class Extractor:
    """
    Clase que se encarga de la extracción de información
    dentro del flujo ETL.
    """

    def __init__(self, http_client: RequestsHttpClient, storage: RawStorage):
        self.client = http_client
        self.storage = storage

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


if __name__ == "__main__":
    from config.path_config import RAW_DATA_DIR, get_api_url

    http_client = RequestsHttpClient(url=get_api_url())
    storage = RawStorage(base_path=RAW_DATA_DIR)
    extractor = Extractor(http_client=http_client, storage=storage)
    saved_files = extractor.extract()
