"""
Pruebas unitarias para clase Extractor.
"""

import pytest
import pytest_check as check
from pathlib import Path
from unittest.mock import Mock, patch


class TestExtractorGetTableFromPayload:
    """Pruebas para el método _get_table_from_payload."""

    @pytest.fixture
    def sample_payload(self) -> dict:
        """Fixture con payload de ejemplo."""
        return {
            "tables": {
                "sensores": [
                    {"id": 1, "nombre": "temp_01"},
                    {"id": 2, "nombre": "hum_01"},
                ],
                "dispositivos": [
                    {"id": 1, "ubicacion": "sala"},
                ],
            }
        }

    @patch("extract.extractor.RequestsHttpClient")
    @patch("extract.extractor.RawStorage")
    def test_get_table_should_return_table_data_when_table_exists(
        self,
        mock_storage: Mock,
        mock_client: Mock,
        sample_payload: dict,
    ):
        """
        Verifica que se retornen los datos correctos cuando
        la tabla existe en el payload.
        """
        from extract.extractor import Extractor

        extractor = Extractor()
        result = extractor._get_table_from_payload(sample_payload, "sensores")

        check.is_not_none(result, "Debe retornar datos cuando la tabla existe")
        check.equal(len(result), 2, "Debe contener 2 registros")

    @patch("extract.extractor.RequestsHttpClient")
    @patch("extract.extractor.RawStorage")
    def test_get_table_should_return_none_when_table_not_exists(
        self, mock_storage: Mock, mock_client: Mock, sample_payload: dict
    ):
        """
        Verifica que se retorne None cuando la tabla no existe en el payload.
        """
        from extract.extractor import Extractor

        extractor = Extractor()
        result = extractor._get_table_from_payload(sample_payload, "tabla_inexistente")

        check.is_none(result, "Debe retornar None cuando la tabla no existe")

    @patch("extract.extractor.RequestsHttpClient")
    @patch("extract.extractor.RawStorage")
    def test_get_table_should_return_none_when_payload_has_no_tables(
        self, mock_storage: Mock, mock_client: Mock
    ):
        """
        Verifica que se retorne None cuando el payload no contiene la clave 'tables'.
        """
        from extract.extractor import Extractor

        extractor = Extractor()
        result = extractor._get_table_from_payload({}, "sensores")

        check.is_none(result, "Debe retornar None cuando no hay tablas")


class TestExtractorExtract:
    """Pruebas para el método extract."""

    @patch("extract.extractor.RequestsHttpClient")
    @patch("extract.extractor.RawStorage")
    def test_extract_should_return_dict_with_saved_paths_when_successful(
        self, mock_storage_class: Mock, mock_client_class: Mock
    ):
        """
        Verifica que se retorne un diccionario con las rutas
        guardadas cuando la extracción es exitosa.
        """
        from extract.extractor import Extractor

        mock_client = Mock()
        mock_client.get.return_value = {
            "tables": {
                "sensores": [{"id": 1}],
            }
        }
        mock_client_class.return_value = mock_client

        mock_storage = Mock()
        mock_storage.save.return_value = Path("/tmp/sensores/file.parquet")
        mock_storage_class.return_value = mock_storage

        extractor = Extractor()
        result = extractor.extract()

        check.is_instance(result, dict, "Debe retornar un diccionario")
        check.is_true("sensores" in result, "Debe contener la tabla 'sensores'")

    @patch("extract.extractor.RequestsHttpClient")
    @patch("extract.extractor.RawStorage")
    def test_extract_should_call_storage_save_for_each_table_when_extracting(
        self, mock_storage_class: Mock, mock_client_class: Mock
    ):
        """
        Verifica que se llame a save del almacenamiento para cada tabla durante la extracción.
        """
        from extract.extractor import Extractor

        mock_client = Mock()
        mock_client.get.return_value = {
            "tables": {
                "sensores": [{"id": 1}],
                "lecturas": [{"valor": 25}],
            }
        }
        mock_client_class.return_value = mock_client

        mock_storage = Mock()
        mock_storage.save.return_value = Path("/tmp/test.parquet")
        mock_storage_class.return_value = mock_storage

        extractor = Extractor()
        extractor.extract()

        check.equal(
            mock_storage.save.call_count, 2, "Debe llamar save una vez por tabla"
        )

    @patch("extract.extractor.RequestsHttpClient")
    @patch("extract.extractor.RawStorage")
    def test_extract_should_skip_missing_tables_when_not_in_payload(
        self, mock_storage_class: Mock, mock_client_class: Mock
    ):
        """
        Verifica que se salten tablas faltantes durante la extracción si estas no se encuentran
        en el payload.
        """
        from extract.extractor import Extractor

        mock_client = Mock()
        mock_client.get.return_value = {
            "tables": {
                "sensores": [{"id": 1}],
            }
        }
        mock_client_class.return_value = mock_client

        mock_storage = Mock()
        mock_storage.save.return_value = Path("/tmp/test.parquet")
        mock_storage_class.return_value = mock_storage

        extractor = Extractor()
        result = extractor.extract()

        check.equal(len(result), 1, "Solo debe guardar tablas que existen")
        check.is_true(
            "tabla_no_existe" not in result, "No debe incluir tablas faltantes"
        )
