"""
Módulo orquestador del proceso de transformación.
"""

from typing import Dict, Type
from pathlib import Path

from pyspark.sql import DataFrame

from utils.spark_io import SparkIO
from transform.cleaners.base_cleaner import BaseCleaner
from transform.cleaners.alerts_cleaner import AlertsCleaner
from transform.cleaners.quality_checks_cleaner import QualityChecksCleaner
from transform.cleaners.defects_cleaner import DefectsCleaner
from transform.cleaners.maintenance_logs_cleaner import MaintenanceLogsCleaner

# Builders de dimensiones
from transform.builders.dimensions import (
    build_dim_date,
    build_dim_time,
    build_simple_dimension,
    build_dim_product,
    build_dim_alert_type,
    build_dim_maintenance_type,
    SIMPLE_DIMENSIONS,
)

# Builders de hechos
from transform.builders.facts import (
    build_fact_production,
    build_fact_quality,
    build_fact_sensor_readings,
    build_fact_maintenance,
    build_fact_alerts,
)


class Transformer:
    """
    Orquesta el proceso de transformación del pipeline ETL.

    Flujo:
        Bronze (raw) → Cleaners → Silver (processed) → Builders → Gold (output)
    """

    # Registro de cleaners especializados por tabla
    CLEANERS: Dict[str, Type[BaseCleaner]] = {
        "alerts": AlertsCleaner,
        "quality_checks": QualityChecksCleaner,
        "defects": DefectsCleaner,
        "maintenance_logs": MaintenanceLogsCleaner,
    }

    # Diccionario de tablas a procesar con su id de columna principal
    TABLES = {
        "alerts": "alert_id",
        "defects": "defect_id",
        "factories": "factory_id",
        "machines": "machine_id",
        "maintenance_logs": "log_id",
        "maintenance_schedules": "schedule_id",
        "operators": "operator_id",
        "production_lines": "line_id",
        "production_orders": "order_id",
        "production_output": "output_id",
        "quality_checks": "check_id",
        "sensor_readings": "reading_id",
        "sensors": "sensor_id",
        "shifts": "shift_id",
    }

    def __init__(
        self,
        spark_io: SparkIO,
        raw_data_dir: Path,
        processed_data_dir: Path,
        output_data_dir: Path,
    ):
        """
        Args:
            spark_io: Instancia de SparkIO para operaciones de lectura/escritura
        """
        self.spark_io = spark_io
        self.raw_data_dir = raw_data_dir
        self.processed_data_dir = processed_data_dir
        self.output_data_dir = output_data_dir

    def transform(self) -> None:
        """
        Ejecuta el pipeline completo de transformación.
        """
        # Fase 1: Limpiar tablas (Bronze → Silver)
        cleaned_tables = self._clean_tables()

        # Fase 2: Construir dimensiones (Silver → Gold)
        dimensions = self._build_dimensions(cleaned_tables)

        # Fase 3: Construir hechos (Silver + Dims → Gold)
        self._build_facts(cleaned_tables, dimensions)

    def _clean_tables(self) -> Dict[str, DataFrame]:
        """
        Limpia todas las tablas raw y las guarda en processed (Silver).

        Returns:
            Diccionario con los DataFrames limpios
        """
        cleaned = {}

        for table_name, id_column in self.TABLES.items():
            # Leer la versión más reciente de la tabla raw
            df = self.spark_io.read_latest_parquet(table_name, self.raw_data_dir)
            if df is None:
                continue

            # Obtener cleaner apropiado (específico o base)
            cleaner = self.CLEANERS.get(table_name, BaseCleaner)
            cleaner = cleaner(df, id_column)

            # Ejecutar limpieza
            df_cleaned = cleaner.clean()

            # Guardar en processed con timestamp (permite versionado)
            self.spark_io.write_timestamped_parquet(
                df_cleaned, self.processed_data_dir / table_name
            )
            cleaned[table_name] = df_cleaned

        return cleaned

    def _build_dimensions(self, cleaned: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """
        Construye todas las tablas dimensionales y las guarda en el
        directorio 'output'.

        Args:
            cleaned: Diccionario de DataFrames limpios

        Returns:
            Diccionario con los DataFrames de dimensiones
        """
        dimensions = {}

        # 1. Dimensiones generadas (no dependen de datos)
        dimensions["dim_date"] = build_dim_date(self.spark_io.session)
        dimensions["dim_time"] = build_dim_time(self.spark_io.session)

        # 2. Dimensiones simples (1:1 con tablas fuente)
        for dim_name, config in SIMPLE_DIMENSIONS.items():
            source_df = cleaned.get(config.source_table)
            if source_df is not None:
                dimensions[dim_name] = build_simple_dimension(source_df, config)

        # 3. Dimensiones derivadas (extraen valores únicos)
        if "production_orders" in cleaned:
            dimensions["dim_product"] = build_dim_product(cleaned["production_orders"])

        if "alerts" in cleaned:
            dimensions["dim_alert_type"] = build_dim_alert_type(cleaned["alerts"])

        if "maintenance_logs" in cleaned:
            dimensions["dim_maintenance_type"] = build_dim_maintenance_type(
                cleaned["maintenance_logs"]
            )

        # Guardar cada dimensión en output
        for dim_name, df in dimensions.items():
            self.spark_io.write_timestamped_parquet(df, self.output_data_dir / dim_name)

        return dimensions

    def _build_facts(
        self, cleaned: Dict[str, DataFrame], dimensions: Dict[str, DataFrame]
    ) -> Dict[str, DataFrame]:
        """
        Construye todas las tablas de hechos y las guarda en el
        directorio 'output'

        Args:
            cleaned: Diccionario de DataFrames limpios
            dimensions: Diccionario de DataFrames de dimensiones

        Returns:
            Diccionario con los DataFrames de hechos
        """
        facts = {}

        # fact_production
        if all(
            k in cleaned for k in ["production_output", "production_orders"]
        ) and all(
            k in dimensions
            for k in [
                "dim_date",
                "dim_shift",
                "dim_factory",
                "dim_production_line",
                "dim_product",
                "dim_order",
            ]
        ):
            facts["fact_production"] = build_fact_production(
                production_output_df=cleaned["production_output"],
                production_orders_df=cleaned["production_orders"],
                dim_date=dimensions["dim_date"],
                dim_shift=dimensions["dim_shift"],
                dim_factory=dimensions["dim_factory"],
                dim_production_line=dimensions["dim_production_line"],
                dim_product=dimensions["dim_product"],
                dim_order=dimensions["dim_order"],
            )

        # fact_quality
        if "quality_checks" in cleaned and all(
            k in dimensions
            for k in [
                "dim_date",
                "dim_production_line",
                "dim_product",
                "dim_order",
                "dim_operator",
            ]
        ):
            facts["fact_quality"] = build_fact_quality(
                quality_checks_df=cleaned["quality_checks"],
                dim_date=dimensions["dim_date"],
                dim_production_line=dimensions["dim_production_line"],
                dim_product=dimensions["dim_product"],
                dim_order=dimensions["dim_order"],
                dim_operator=dimensions["dim_operator"],
            )

        # fact_sensor_readings (evento)
        if "sensor_readings" in cleaned and all(
            k in dimensions
            for k in [
                "dim_date",
                "dim_time",
                "dim_sensor",
                "dim_machine",
                "dim_production_line",
            ]
        ):
            facts["fact_sensor_readings"] = build_fact_sensor_readings(
                sensor_readings_df=cleaned["sensor_readings"],
                dim_date=dimensions["dim_date"],
                dim_time=dimensions["dim_time"],
                dim_sensor=dimensions["dim_sensor"],
                dim_machine=dimensions["dim_machine"],
                dim_production_line=dimensions["dim_production_line"],
            )

        # fact_maintenance
        if "maintenance_logs" in cleaned and all(
            k in dimensions
            for k in [
                "dim_date",
                "dim_machine",
                "dim_production_line",
                "dim_maintenance_type",
                "dim_operator",
            ]
        ):
            facts["fact_maintenance"] = build_fact_maintenance(
                maintenance_logs_df=cleaned["maintenance_logs"],
                dim_date=dimensions["dim_date"],
                dim_machine=dimensions["dim_machine"],
                dim_production_line=dimensions["dim_production_line"],
                dim_maintenance_type=dimensions["dim_maintenance_type"],
                dim_operator=dimensions["dim_operator"],
            )

        # fact_alerts
        if "alerts" in cleaned and all(
            k in dimensions
            for k in [
                "dim_date",
                "dim_time",
                "dim_sensor",
                "dim_machine",
                "dim_production_line",
                "dim_alert_type",
            ]
        ):
            facts["fact_alerts"] = build_fact_alerts(
                alerts_df=cleaned["alerts"],
                dim_date=dimensions["dim_date"],
                dim_time=dimensions["dim_time"],
                dim_sensor=dimensions["dim_sensor"],
                dim_machine=dimensions["dim_machine"],
                dim_production_line=dimensions["dim_production_line"],
                dim_alert_type=dimensions["dim_alert_type"],
            )

        # Guardar cada fact en output
        for fact_name, df in facts.items():
            self.spark_io.write_timestamped_parquet(
                df, self.output_data_dir / fact_name
            )

        return facts


if __name__ == "__main__":
    from config.path_config import RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DATA_DIR

    spark_io = SparkIO(app_name="IoT_ETL_Transform")
    transformer = Transformer(
        spark_io, RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DATA_DIR
    )
    transformer.transform()
