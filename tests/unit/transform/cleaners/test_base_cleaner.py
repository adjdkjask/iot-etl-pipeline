"""
Pruebas unitarias para clase BaseCleaner.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]").appName("TestBaseCleaner").getOrCreate()
    )


class TestBaseCleanerHandleNulls:
    """Pruebas para el método _handle_nulls."""

    @pytest.fixture
    def df_with_null_ids(self, spark: SparkSession):
        """DataFrame con IDs nulos."""
        return spark.createDataFrame(
            [
                {"id": 1, "name": "item_1"},
                {"id": None, "name": "item_2"},
                {"id": 3, "name": "item_3"},
                {"id": None, "name": "item_4"},
            ]
        )

    def test_handle_nulls_should_remove_rows_with_null_id(self, df_with_null_ids):
        """
        Verifica que se eliminen las filas con id nulo.
        """
        from src.transform.cleaners.base_cleaner import BaseCleaner

        cleaner = BaseCleaner(df_with_null_ids, id_column="id")
        result = cleaner._handle_nulls(df_with_null_ids)

        check.equal(result.count(), 2, "Debe quedar 2 registros sin id nulo")

    def test_handle_nulls_should_keep_rows_with_valid_id(self, spark: SparkSession):
        """
        Verifica que se mantengan las filas con id válido.
        """
        from src.transform.cleaners.base_cleaner import BaseCleaner

        df = spark.createDataFrame(
            [
                {"sensor_id": "S001", "value": 23.5},
                {"sensor_id": "S002", "value": None},
            ]
        )

        cleaner = BaseCleaner(df, id_column="sensor_id")
        result = cleaner._handle_nulls(df)

        check.equal(result.count(), 2, "Ambos registros tienen id válido")


class TestBaseCleanerHandleDuplicates:
    """Pruebas para el método _handle_duplicates."""

    @pytest.fixture
    def df_with_duplicates(self, spark: SparkSession):
        """DataFrame con filas duplicadas."""
        return spark.createDataFrame(
            [
                {"id": 1, "name": "item_1"},
                {"id": 1, "name": "item_1"},  # Duplicado exacto
                {"id": 2, "name": "item_2"},
                {"id": 3, "name": "item_3"},
                {"id": 3, "name": "item_3"},  # Duplicado exacto
            ]
        )

    def test_handle_duplicates_should_remove_exact_duplicates(self, df_with_duplicates):
        """
        Verifica que se eliminen los duplicados exactos.
        """
        from src.transform.cleaners.base_cleaner import BaseCleaner

        cleaner = BaseCleaner(df_with_duplicates, id_column="id")
        result = cleaner._handle_duplicates(df_with_duplicates)

        check.equal(result.count(), 3, "Debe quedar 3 registros únicos")


class TestBaseCleanerClean:
    """Pruebas para el método clean (pipeline completo)."""

    @pytest.fixture
    def df_dirty(self, spark: SparkSession):
        """DataFrame con varios problemas de calidad."""
        return spark.createDataFrame(
            [
                {"id": 1, "name": "item_1"},
                {"id": None, "name": "item_2"},  # ID nulo
                {"id": 3, "name": "item_3"},
                {"id": 3, "name": "item_3"},  # Duplicado
            ]
        )

    def test_clean_should_apply_all_cleaning_steps(self, df_dirty):
        """
        Verifica que el método clean aplique todos los pasos de limpieza.
        """
        from src.transform.cleaners.base_cleaner import BaseCleaner

        cleaner = BaseCleaner(df_dirty, id_column="id")
        result = cleaner.clean()

        check.equal(
            result.count(), 2, "Debe quedar 2 registros (sin nulos ni duplicados)"
        )

    def test_clean_should_return_dataframe(self, df_dirty):
        """
        Verifica que clean retorne un DataFrame.
        """
        from src.transform.cleaners.base_cleaner import BaseCleaner
        from pyspark.sql import DataFrame

        cleaner = BaseCleaner(df_dirty, id_column="id")
        result = cleaner.clean()

        check.is_instance(result, DataFrame, "Debe retornar un DataFrame")
