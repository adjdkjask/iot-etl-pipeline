"""
Pruebas unitarias para clase RawStorage.
"""

import pytest
import pytest_check as check
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime

from extract.storage import RawStorage


@pytest.fixture
def raw_storage(tmp_path: Path) -> RawStorage:
    """Fixture que crea una instancia de RawStorage con directorio temporal."""
    return RawStorage(base_path=tmp_path)


@pytest.fixture
def sample_payload() -> list[dict]:
    """Fixture con datos de ejemplo."""
    return [
        {"id": 1, "sensor": "temp_01", "value": 23.5},
        {"id": 2, "sensor": "temp_02", "value": 24.1},
        {"id": 3, "sensor": "hum_01", "value": 65.0},
    ]


class TestRawStorageSave:
    """Pruebas para el método save de RawStorage."""

    def test_save_should_create_parquet_file_when_valid_payload(
        self, raw_storage: RawStorage, sample_payload: list[dict]
    ):
        """
        Verifica que se cree un archivo parquet cuando se proporciona un payload válido.
        """
        file_path = raw_storage.save(file_name="sensores", payload=sample_payload)

        check.is_true(file_path.exists(), "El archivo parquet debe existir")
        check.equal(file_path.suffix, ".parquet", "La extensión debe ser .parquet")

    def test_save_should_create_subdirectory_with_table_name_when_saving(
        self, raw_storage: RawStorage, sample_payload: list[dict]
    ):
        """
        Verifica que se cree un subdirectorio con el nombre de la tabla al guardar.
        """
        table_name = "lecturas"
        file_path = raw_storage.save(file_name=table_name, payload=sample_payload)

        check.is_true(
            table_name in file_path.parts,
            "El directorio debe contener el nombre de la tabla",
        )
        check.is_true(file_path.parent.is_dir(), "El directorio padre debe existir")

    def test_save_should_include_timestamp_in_filename_when_saving(
        self, raw_storage: RawStorage, sample_payload: list[dict]
    ):
        """
        Verifica que se incluya la fecha en el nombre del archivo.
        """
        file_path = raw_storage.save(file_name="eventos", payload=sample_payload)
        filename = file_path.stem
        today = datetime.utcnow().strftime("%Y%m%d")

        check.is_true(
            today in filename,
            f"El nombre del archivo debe contener la fecha {today}",
        )

    def test_save_should_persist_correct_data_when_reading_back(
        self, raw_storage: RawStorage, sample_payload: list[dict]
    ):
        """
        Verifica que los datos guardados sean correctos al leerlos de nuevo.
        """
        file_path = raw_storage.save(file_name="datos", payload=sample_payload)
        table = pq.read_table(file_path)
        records = table.to_pylist()

        check.equal(
            len(records), len(sample_payload), "Debe tener el mismo número de registros"
        )
        check.equal(records[0]["sensor"], "temp_01", "El primer sensor debe coincidir")
        check.equal(records[1]["value"], 24.1, "El segundo valor debe coincidir")

    def test_save_should_return_path_object_when_successful(
        self, raw_storage: RawStorage, sample_payload: list[dict]
    ):
        """
        Verifica que se retorne un objeto Path cuando la operación es exitosa.
        """
        result = raw_storage.save(file_name="test", payload=sample_payload)

        check.is_instance(result, Path, "Debe retornar un objeto Path")
