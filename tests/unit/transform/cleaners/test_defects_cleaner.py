"""
Pruebas unitarias para clase DefectsCleaner.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestDefectsCleaner")
        .getOrCreate()
    )


class TestDefectsCleanerHandleNulls:
    """Pruebas para el método _handle_nulls de DefectsCleaner."""

    @pytest.fixture
    def df_defects_with_nulls(self, spark: SparkSession):
        """DataFrame de defectos con nulos."""
        return spark.createDataFrame(
            [
                {
                    "defect_id": "D001",
                    "defect_type": "crack",
                    "corrective_action": "replaced",
                },
                {
                    "defect_id": "D002",
                    "defect_type": "scratch",
                    "corrective_action": None,
                },
                {
                    "defect_id": None,
                    "defect_type": "dent",
                    "corrective_action": "repaired",
                },
                {"defect_id": "D004", "defect_type": "chip", "corrective_action": None},
            ]
        )

    def test_handle_nulls_should_remove_rows_with_null_defect_id(
        self, df_defects_with_nulls
    ):
        """
        Verifica que se eliminen las filas con defect_id nulo.
        """
        from src.transform.cleaners.defects_cleaner import DefectsCleaner

        cleaner = DefectsCleaner(df_defects_with_nulls)
        result = cleaner._handle_nulls(df_defects_with_nulls)

        check.equal(result.count(), 3, "Debe quedar 3 registros sin defect_id nulo")

    def test_handle_nulls_should_fill_corrective_action_when_null(
        self, df_defects_with_nulls
    ):
        """
        Verifica que corrective_action se llene con valor por defecto cuando es NULL.
        """
        from src.transform.cleaners.defects_cleaner import DefectsCleaner

        cleaner = DefectsCleaner(df_defects_with_nulls)
        result = cleaner._handle_nulls(df_defects_with_nulls)

        row_d002 = result.filter("defect_id = 'D002'").collect()[0]
        row_d004 = result.filter("defect_id = 'D004'").collect()[0]

        check.equal(
            row_d002["corrective_action"],
            "Corrective action not taken",
            "D002 debe tener valor por defecto",
        )
        check.equal(
            row_d004["corrective_action"],
            "Corrective action not taken",
            "D004 debe tener valor por defecto",
        )

    def test_handle_nulls_should_keep_existing_corrective_action(
        self, df_defects_with_nulls
    ):
        """
        Verifica que se mantengan los valores existentes de corrective_action.
        """
        from src.transform.cleaners.defects_cleaner import DefectsCleaner

        cleaner = DefectsCleaner(df_defects_with_nulls)
        result = cleaner._handle_nulls(df_defects_with_nulls)

        row_d001 = result.filter("defect_id = 'D001'").collect()[0]

        check.equal(
            row_d001["corrective_action"],
            "replaced",
            "D001 debe mantener su valor original",
        )
