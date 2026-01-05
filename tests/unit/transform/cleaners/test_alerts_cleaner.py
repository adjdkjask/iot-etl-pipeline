"""
Pruebas unitarias para clase AlertsCleaner.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession
from datetime import datetime


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestAlertsCleaner")
        .getOrCreate()
    )


class TestAlertsCleanerHandleNulls:
    """Pruebas para el método _handle_nulls de AlertsCleaner."""

    @pytest.fixture
    def df_alerts_with_nulls(self, spark: SparkSession):
        """DataFrame de alertas con varios escenarios de nulos."""
        return spark.createDataFrame(
            [
                # Caso 1: Todo completo
                {
                    "alert_id": "A001",
                    "acknowledged_at": datetime(2026, 1, 1, 10, 0),
                    "resolved_at": datetime(2026, 1, 1, 12, 0),
                    "resolved_by": "user_1",
                },
                # Caso 2: acknowledged_at NULL pero resolved_at tiene valor
                {
                    "alert_id": "A002",
                    "acknowledged_at": None,
                    "resolved_at": datetime(2026, 1, 2, 14, 0),
                    "resolved_by": "user_2",
                },
                # Caso 3: resolved_by NULL
                {
                    "alert_id": "A003",
                    "acknowledged_at": datetime(2026, 1, 3, 8, 0),
                    "resolved_at": datetime(2026, 1, 3, 9, 0),
                    "resolved_by": None,
                },
                # Caso 4: Todas las columnas opcionales NULL (pending)
                {
                    "alert_id": "A004",
                    "acknowledged_at": None,
                    "resolved_at": None,
                    "resolved_by": None,
                },
                # Caso 5: alert_id NULL (debe eliminarse)
                {
                    "alert_id": None,
                    "acknowledged_at": datetime(2026, 1, 4, 10, 0),
                    "resolved_at": datetime(2026, 1, 4, 11, 0),
                    "resolved_by": "user_3",
                },
            ]
        )

    def test_handle_nulls_should_remove_rows_with_null_alert_id(
        self, df_alerts_with_nulls
    ):
        """
        Verifica que se eliminen las filas con alert_id nulo.
        """
        from src.transform.cleaners.alerts_cleaner import AlertsCleaner

        cleaner = AlertsCleaner(df_alerts_with_nulls)
        result = cleaner._handle_nulls(df_alerts_with_nulls)

        check.equal(result.count(), 4, "Debe quedar 4 registros (sin alert_id nulo)")

    def test_handle_nulls_should_fill_acknowledged_at_from_resolved_at(
        self, df_alerts_with_nulls
    ):
        """
        Verifica que acknowledged_at se llene con resolved_at cuando es NULL.
        """
        from src.transform.cleaners.alerts_cleaner import AlertsCleaner

        cleaner = AlertsCleaner(df_alerts_with_nulls)
        result = cleaner._handle_nulls(df_alerts_with_nulls)

        row_a002 = result.filter("alert_id = 'A002'").collect()[0]

        check.is_not_none(
            row_a002["acknowledged_at"], "acknowledged_at debe tener valor"
        )
        check.equal(
            row_a002["acknowledged_at"],
            row_a002["resolved_at"],
            "acknowledged_at debe ser igual a resolved_at",
        )

    def test_handle_nulls_should_fill_resolved_by_with_unknown(
        self, df_alerts_with_nulls
    ):
        """
        Verifica que resolved_by se llene con 'unknown' cuando es NULL.
        """
        from src.transform.cleaners.alerts_cleaner import AlertsCleaner

        cleaner = AlertsCleaner(df_alerts_with_nulls)
        result = cleaner._handle_nulls(df_alerts_with_nulls)

        row_a003 = result.filter("alert_id = 'A003'").collect()[0]
        row_a004 = result.filter("alert_id = 'A004'").collect()[0]

        check.equal(
            row_a003["resolved_by"],
            "unknown",
            "resolved_by debe ser 'unknown' para A003",
        )
        check.equal(
            row_a004["resolved_by"],
            "unknown",
            "resolved_by debe ser 'unknown' para A004",
        )

    def test_handle_nulls_should_add_alert_status_column(self, df_alerts_with_nulls):
        """
        Verifica que se agregue la columna alert_status.
        """
        from src.transform.cleaners.alerts_cleaner import AlertsCleaner

        cleaner = AlertsCleaner(df_alerts_with_nulls)
        result = cleaner._handle_nulls(df_alerts_with_nulls)

        check.is_true(
            "alert_status" in result.columns, "Debe existir columna alert_status"
        )


class TestAlertsCleanerAlertStatus:
    """Pruebas para la lógica de alert_status."""

    @pytest.fixture
    def df_for_status(self, spark: SparkSession):
        """DataFrame para probar lógica de status."""
        return spark.createDataFrame(
            [
                # resolved: tiene resolved_at
                {
                    "alert_id": "A001",
                    "acknowledged_at": datetime(2026, 1, 1, 10, 0),
                    "resolved_at": datetime(2026, 1, 1, 12, 0),
                    "resolved_by": "user_1",
                },
                # acknowledged: tiene acknowledged_at pero no resolved_at
                {
                    "alert_id": "A002",
                    "acknowledged_at": datetime(2026, 1, 2, 10, 0),
                    "resolved_at": None,
                    "resolved_by": None,
                },
                # pending: ambos NULL
                {
                    "alert_id": "A003",
                    "acknowledged_at": None,
                    "resolved_at": None,
                    "resolved_by": None,
                },
            ]
        )

    def test_alert_status_should_be_resolved_when_resolved_at_exists(
        self, df_for_status
    ):
        """
        Verifica que status sea 'resolved' cuando resolved_at tiene valor.
        """
        from src.transform.cleaners.alerts_cleaner import AlertsCleaner

        cleaner = AlertsCleaner(df_for_status)
        result = cleaner._handle_nulls(df_for_status)

        row = result.filter("alert_id = 'A001'").collect()[0]
        check.equal(row["alert_status"], "resolved")

    def test_alert_status_should_be_acknowledged_when_only_acknowledged(
        self, df_for_status
    ):
        """
        Verifica que status sea 'acknowledged' cuando solo acknowledged_at tiene valor.
        """
        from src.transform.cleaners.alerts_cleaner import AlertsCleaner

        cleaner = AlertsCleaner(df_for_status)
        result = cleaner._handle_nulls(df_for_status)

        row = result.filter("alert_id = 'A002'").collect()[0]
        check.equal(row["alert_status"], "acknowledged")

    def test_alert_status_should_be_pending_when_both_null(self, df_for_status):
        """
        Verifica que status sea 'pending' cuando ambos son NULL.
        """
        from src.transform.cleaners.alerts_cleaner import AlertsCleaner

        cleaner = AlertsCleaner(df_for_status)
        result = cleaner._handle_nulls(df_for_status)

        row = result.filter("alert_id = 'A003'").collect()[0]
        check.equal(row["alert_status"], "pending")
