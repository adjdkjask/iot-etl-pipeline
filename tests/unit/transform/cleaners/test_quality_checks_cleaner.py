"""
Pruebas unitarias para clase QualityChecksCleaner.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestQualityChecksCleaner")
        .getOrCreate()
    )


class TestQualityChecksCleanerHandleNulls:
    """Pruebas para el método _handle_nulls de QualityChecksCleaner."""

    @pytest.fixture
    def df_checks_with_nulls(self, spark: SparkSession):
        """DataFrame de quality checks con nulos."""
        return spark.createDataFrame(
            [
                {"check_id": "C001", "result": "pass", "notes": "All OK"},
                {"check_id": "C002", "result": "fail", "notes": None},
                {"check_id": None, "result": "pass", "notes": "Good condition"},
                {"check_id": "C004", "result": "pass", "notes": None},
            ]
        )

    def test_handle_nulls_should_remove_rows_with_null_check_id(
        self, df_checks_with_nulls
    ):
        """
        Verifica que se eliminen las filas con check_id nulo.
        """
        from src.transform.cleaners.quality_checks_cleaner import QualityChecksCleaner

        cleaner = QualityChecksCleaner(df_checks_with_nulls)
        result = cleaner._handle_nulls(df_checks_with_nulls)

        check.equal(result.count(), 3, "Debe quedar 3 registros sin check_id nulo")

    def test_handle_nulls_should_fill_notes_when_null(self, df_checks_with_nulls):
        """
        Verifica que notes se llene con valor por defecto cuando es NULL.
        """
        from src.transform.cleaners.quality_checks_cleaner import QualityChecksCleaner

        cleaner = QualityChecksCleaner(df_checks_with_nulls)
        result = cleaner._handle_nulls(df_checks_with_nulls)

        row_c002 = result.filter("check_id = 'C002'").collect()[0]
        row_c004 = result.filter("check_id = 'C004'").collect()[0]

        check.equal(
            row_c002["notes"], "No notes provided", "C002 debe tener valor por defecto"
        )
        check.equal(
            row_c004["notes"], "No notes provided", "C004 debe tener valor por defecto"
        )

    def test_handle_nulls_should_keep_existing_notes(self, df_checks_with_nulls):
        """
        Verifica que se mantengan los valores existentes de notes.
        """
        from src.transform.cleaners.quality_checks_cleaner import QualityChecksCleaner

        cleaner = QualityChecksCleaner(df_checks_with_nulls)
        result = cleaner._handle_nulls(df_checks_with_nulls)

        row_c001 = result.filter("check_id = 'C001'").collect()[0]

        check.equal(row_c001["notes"], "All OK", "C001 debe mantener su valor original")
