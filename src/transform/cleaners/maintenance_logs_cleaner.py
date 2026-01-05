"""
Módulo que se encarga de la limpieza específica para la tabla 'MaintenanceLogs'.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from transform.cleaners.base_cleaner import BaseCleaner


class MaintenanceLogsCleaner(BaseCleaner):
    """
    Clase específica para la limpieza de la tabla 'MaintenanceLogs'.
    """

    def __init__(self, df: DataFrame, id_column: str = "log_id"):
        super().__init__(df, id_column)

    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        """
        Implementa lógica de manejo de valores nulos con reglas de negocio
        específicas para la tabla MaintenanceLogs.

        Reglas:
        1. Eliminar registros con log_id nulo
        2. Si parts_replaced es NULL, asignar "No parts replaced"
        """

        # 1. Filtrar registros con id nulo
        df_cleaned = df.filter(F.col(self.id_column).isNotNull())

        # 2. Si parts_replaced es NULL, asignar "No parts replaced"
        df_cleaned = df_cleaned.withColumn(
            "parts_replaced",
            F.when(
                F.col("parts_replaced").isNull(),
                F.lit("No parts replaced"),
            ).otherwise(F.col("parts_replaced")),
        )

        return df_cleaned
