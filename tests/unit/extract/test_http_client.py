"""
Pruebas unitarias para clase RequestsHttpClient.
"""

import pytest
import pytest_check as check
from unittest.mock import patch, Mock

from extract.http_client import RequestsHttpClient


@pytest.fixture
def http_client() -> RequestsHttpClient:
    """Fixture que crea una instancia del cliente HTTP."""
    return RequestsHttpClient(url="https://api.example.com/data")


@pytest.fixture
def mock_api_response() -> dict:
    """Fixture con respuesta simulada de la API."""
    return {
        "tables": {
            "sensores": [
                {"id": 1, "nombre": "sensor_temp"},
                {"id": 2, "nombre": "sensor_hum"},
            ],
            "lecturas": [
                {"sensor_id": 1, "valor": 25.5},
            ],
        }
    }


class TestRequestsHttpClientGet:
    """Pruebas para el método get de RequestsHttpClient."""

    @patch("extract.http_client.requests.get")
    def test_get_should_return_json_payload_when_api_responds_successfully(
        self, mock_get: Mock, http_client: RequestsHttpClient, mock_api_response: dict
    ):
        """
        Verifica que el método get retorne el payload JSON correcto
        cuando la API responde exitosamente.
        """
        mock_get.return_value.json.return_value = mock_api_response
        mock_get.return_value.raise_for_status = Mock()

        result = http_client.get()

        check.is_instance(result, dict, "Debe retornar un diccionario")
        check.is_true("tables" in result, "Debe contener la clave 'tables'")

    @patch("extract.http_client.requests.get")
    def test_get_should_call_correct_url_when_invoked(
        self, mock_get: Mock, mock_api_response: dict
    ):
        """
        Verifica que el método get llame a la URL correcta.
        """
        mock_get.return_value.json.return_value = mock_api_response
        mock_get.return_value.raise_for_status = Mock()
        url = "https://api.test.com/endpoint"
        client = RequestsHttpClient(url=url)

        client.get()

        mock_get.assert_called_once_with(url, timeout=10)

    @patch("extract.http_client.requests.get")
    def test_get_should_return_tables_data_when_payload_contains_tables(
        self, mock_get: Mock, http_client: RequestsHttpClient, mock_api_response: dict
    ):
        """
        Verifica que el método get retorne los datos de las tablas
        cuando el payload contiene la clave 'tables'.
        """
        mock_get.return_value.json.return_value = mock_api_response
        mock_get.return_value.raise_for_status = Mock()

        result = http_client.get()

        check.equal(len(result["tables"]), 2, "Debe contener 2 tablas")
        check.is_true("sensores" in result["tables"], "Debe contener tabla 'sensores'")
        check.is_true("lecturas" in result["tables"], "Debe contener tabla 'lecturas'")


class TestRequestsHttpClientInit:
    """Pruebas para la inicialización de RequestsHttpClient."""

    def test_init_should_store_url_when_instantiated(self):
        """
        Verifica que la URL se almacene correctamente al instanciar.
        """
        url = "https://api.example.com/test"
        client = RequestsHttpClient(url=url)

        check.equal(client.url, url, "La URL debe almacenarse correctamente")
