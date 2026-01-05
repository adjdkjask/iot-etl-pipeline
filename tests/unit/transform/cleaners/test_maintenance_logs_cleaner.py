"""
Pruebas unitarias para clase MaintenanceLogsCleaner.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestMaintenanceLogsCleaner")
        .getOrCreate()
    )


class TestMaintenanceLogsCleanerHandleNulls:
    """Pruebas para el método _handle_nulls de MaintenanceLogsCleaner."""

    @pytest.fixture
    def df_logs_with_nulls(self, spark: SparkSession):
        """DataFrame de logs de mantenimiento con nulos."""
        return spark.createDataFrame(
            [
                {"log_id": "L001", "machine_id": "M001", "parts_replaced": "bearing"},
                {"log_id": "L002", "machine_id": "M002", "parts_replaced": None},
                {"log_id": None, "machine_id": "M003", "parts_replaced": "motor"},
                {"log_id": "L004", "machine_id": "M004", "parts_replaced": None},
            ]
        )

    def test_handle_nulls_should_remove_rows_with_null_log_id(self, df_logs_with_nulls):
        """
        Verifica que se eliminen las filas con log_id nulo.
        """
        from src.transform.cleaners.maintenance_logs_cleaner import (
            MaintenanceLogsCleaner,
        )

        cleaner = MaintenanceLogsCleaner(df_logs_with_nulls)
        result = cleaner._handle_nulls(df_logs_with_nulls)

        check.equal(result.count(), 3, "Debe quedar 3 registros sin log_id nulo")

    def test_handle_nulls_should_fill_parts_replaced_when_null(
        self, df_logs_with_nulls
    ):
        """
        Verifica que parts_replaced se llene con valor por defecto cuando es NULL.
        """
        from src.transform.cleaners.maintenance_logs_cleaner import (
            MaintenanceLogsCleaner,
        )

        cleaner = MaintenanceLogsCleaner(df_logs_with_nulls)
        result = cleaner._handle_nulls(df_logs_with_nulls)

        row_l002 = result.filter("log_id = 'L002'").collect()[0]
        row_l004 = result.filter("log_id = 'L004'").collect()[0]

        check.equal(
            row_l002["parts_replaced"],
            "No parts replaced",
            "L002 debe tener valor por defecto",
        )
        check.equal(
            row_l004["parts_replaced"],
            "No parts replaced",
            "L004 debe tener valor por defecto",
        )

    def test_handle_nulls_should_keep_existing_parts_replaced(self, df_logs_with_nulls):
        """
        Verifica que se mantengan los valores existentes de parts_replaced.
        """
        from src.transform.cleaners.maintenance_logs_cleaner import (
            MaintenanceLogsCleaner,
        )

        cleaner = MaintenanceLogsCleaner(df_logs_with_nulls)
        result = cleaner._handle_nulls(df_logs_with_nulls)

        row_l001 = result.filter("log_id = 'L001'").collect()[0]

        check.equal(
            row_l001["parts_replaced"],
            "bearing",
            "L001 debe mantener su valor original",
        )
