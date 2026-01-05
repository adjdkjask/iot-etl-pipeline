"""
Módulo que se encarga de la limpieza específica para la tabla 'Defects'.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from transform.cleaners.base_cleaner import BaseCleaner


class DefectsCleaner(BaseCleaner):
    """
    Clase específica para la limpieza de la tabla 'Defects'.
    """

    def __init__(self, df: DataFrame, id_column: str = "defect_id"):
        super().__init__(df, id_column)

    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        """
        Implementa lógica de manejo de valores nulos con reglas de negocio
        específicas para la tabla Defects.

        Reglas:
        1. Eliminar registros con defect_id nulo
        2. Si corrective_action es NULL, asignar "Corrective action not taken"
        """

        # 1. Filtrar registros con id nulo
        df_cleaned = df.filter(F.col(self.id_column).isNotNull())

        # 2. Si corrective_action es NULL, asignar "Corrective action not taken"
        df_cleaned = df_cleaned.withColumn(
            "corrective_action",
            F.when(
                F.col("corrective_action").isNull(),
                F.lit("Corrective action not taken"),
            ).otherwise(F.col("corrective_action")),
        )

        return df_cleaned
