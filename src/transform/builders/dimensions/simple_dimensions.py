"""
Configuración de dimensiones simples.

Las dimensiones simples son aquellas que mapean 1:1 con una tabla fuente,
básicamente renombrando la PK a SK y seleccionando columnas específicas.
"""

from typing import Dict, List, Optional
from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class SimpleDimensionConfig:
    """Configuración para una dimensión simple."""

    source_table: str
    natural_key: str
    surrogate_key: str
    # Columnas a incluir (None = todas)
    columns: Optional[List[str]] = None


# Configuración de dimensiones simples según el esquema estrella
SIMPLE_DIMENSIONS: Dict[str, SimpleDimensionConfig] = {
    "dim_factory": SimpleDimensionConfig(
        source_table="factories",
        natural_key="factory_id",
        surrogate_key="factory_sk",
        columns=[
            "factory_id",  # Se convertirá en factory_sk
            "factory_name",
            "location",
            "region",
            "manager_name",
            "total_area_sqm",
        ],
    ),
    "dim_production_line": SimpleDimensionConfig(
        source_table="production_lines",
        natural_key="line_id",
        surrogate_key="line_sk",
        columns=[
            "line_id",
            "factory_id",  
            "line_name",
            "line_type",
            "capacity_units_hour",
            "installed_date",
            "status",
        ],
    ),
    "dim_machine": SimpleDimensionConfig(
        source_table="machines",
        natural_key="machine_id",
        surrogate_key="machine_sk",
        columns=[
            "machine_id",
            "line_id",  # FK para joins con dim_production_line
            "machine_name",
            "machine_type",
            "manufacturer",
            "model",
            "serial_number",
            "installation_date",
            "operating_hours",
            "is_active",
        ],
    ),
    "dim_sensor": SimpleDimensionConfig(
        source_table="sensors",
        natural_key="sensor_id",
        surrogate_key="sensor_sk",
        columns=[
            "sensor_id",
            "sensor_name",
            "sensor_type",
            "unit",
            "min_threshold",
            "max_threshold",
            "machine_name",  # Desnormalizado
            "is_active",
        ],
    ),
    "dim_shift": SimpleDimensionConfig(
        source_table="shifts",
        natural_key="shift_id",
        surrogate_key="shift_sk",
        columns=[
            "shift_id",
            "shift_name",
            "start_time",
            "end_time",
            "shift_duration_hrs",
        ],
    ),
    "dim_operator": SimpleDimensionConfig(
        source_table="operators",
        natural_key="operator_id",
        surrogate_key="operator_sk",
        columns=[
            "operator_id",
            "full_name",
            "employee_code",
            "certification_level",
            "role_type",
            "hire_date",
        ],
    ),
    "dim_order": SimpleDimensionConfig(
        source_table="production_orders",
        natural_key="order_id",
        surrogate_key="order_sk",
        columns=[
            "order_id",
            "product_code",
            "quantity_ordered",
            "start_date",
            "target_end_date",
            "priority",
            "status",
        ],
    ),
}


def build_simple_dimension(
    source_df: DataFrame,
    config: SimpleDimensionConfig,
) -> DataFrame:
    """
    Construye una dimensión simple a partir de su configuración.

    Args:
        source_df: DataFrame fuente (tabla limpia de Silver)
        config: Configuración de la dimensión

    Returns:
        DataFrame con la dimensión construida
    """
    # Seleccionar columnas si están especificadas
    if config.columns:
        # Filtrar solo las columnas que existen en el DataFrame
        available_cols = [c for c in config.columns if c in source_df.columns]
        df = source_df.select(available_cols)
    else:
        df = source_df

    # Renombrar la natural key a surrogate key
    # Usamos el valor de la NK como SK (son equivalentes en dims simples)
    df = df.withColumn(config.surrogate_key, F.col(config.natural_key))

    # Reordenar para que SK sea la primera columna
    cols = [config.surrogate_key] + [c for c in df.columns if c != config.surrogate_key]

    return df.select(cols)
