"""
Configuración centralizada de rutas y variables de entorno del proyecto.

Este módulo centraliza todas las rutas y configuraciones utilizadas
a lo largo del pipeline ETL.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Raíz del proyecto (funciona tanto en desarrollo local como en Docker)
# En Docker: /app
# En local: directorio padre de 'config'
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Directorio base de datos
DATA_DIR = PROJECT_ROOT / "data"

# Capas del Data Lake
RAW_DATA_DIR = DATA_DIR / "raw"  # Bronze: datos crudos de la API
PROCESSED_DATA_DIR = DATA_DIR / "processed"  # Silver: datos limpios
OUTPUT_DATA_DIR = DATA_DIR / "output"  # Gold: modelo dimensional
EXPORTS_DIR = DATA_DIR / "exports"  # Exportaciones (CSV, etc.)

# Cargar variables desde .env
load_dotenv(PROJECT_ROOT / ".env")

# Configuración de la API
API_CONFIG = {
    "base_url": os.getenv("API_BASE_URL"),
    "email": os.getenv("USER_EMAIL"),
    "key": os.getenv("API_KEY"),
    "dataset_type": os.getenv("DATASET_TYPE"),
    "rows": os.getenv("ROWS"),
}


def get_api_url() -> str:
    """
    Construye la URL completa de la API con los parámetros configurados.

    Returns:
        URL formateada para la petición a la API.
    """
    return (
        f"{API_CONFIG['base_url']}?"
        f"email={API_CONFIG['email']}&"
        f"key={API_CONFIG['key']}&"
        f"type={API_CONFIG['dataset_type']}&"
        f"rows={API_CONFIG['rows']}"
    )


def ensure_directories() -> None:
    """
    Crea los directorios de datos si no existen.
    Útil para inicialización del proyecto.
    """
    for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DATA_DIR, EXPORTS_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def get_path_for_environment(local_path: Path) -> str:
    """
    Retorna la ruta apropiada según el entorno.

    Args:
        local_path: Path local al recurso.

    Returns:
        String con la ruta (local o cloud).
    """
    # TODO: Implementar lógica para cloud cuando se migre
    # if os.getenv("STORAGE_BACKEND") == "s3":
    #     bucket = os.getenv("S3_BUCKET")
    #     relative = local_path.relative_to(DATA_DIR)
    #     return f"s3://{bucket}/{relative}"
    return str(local_path)
