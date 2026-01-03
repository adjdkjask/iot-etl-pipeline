"""
Módulo de cliente para realizar peticiones HTTP.
"""

import time
import requests
from typing import Any, Dict


class RequestsHttpClient:
    """
    Clase para realizar peticiones HTTP con manejo de reintentos y tiempos de espera.
    """

    def __init__(
        self,
        url: str,
        timeout: int = 10,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self.url = url
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def get(self) -> Dict[str, Any]:
        """
        Realiza una petición GET con reintentos.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(
                    self.url,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return response.json()

            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    raise exc

                sleep_time = self.backoff_factor * (2 ** (attempt - 1))
                time.sleep(sleep_time)

        return {}
