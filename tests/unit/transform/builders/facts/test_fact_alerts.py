"""
Pruebas unitarias para build_fact_alerts.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession
from datetime import datetime


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]").appName("TestFactAlerts").getOrCreate()
    )


class TestBuildFactAlerts:
    """Pruebas para el builder de fact_alerts."""

    @pytest.fixture
    def alerts_df(self, spark: SparkSession):
        """DataFrame de alertas con acknowledged_at."""
        return spark.createDataFrame(
            [
                {
                    "alert_id": 1,
                    "sensor_id": 10,
                    "machine_id": 100,
                    "alert_type": "threshold_exceeded",
                    "severity": "high",
                    "triggered_at": datetime(2024, 1, 15, 10, 30),
                    "resolved_at": datetime(2024, 1, 15, 10, 45),
                    "acknowledged_at": datetime(2024, 1, 15, 10, 32),
                },
            ]
        )

    @pytest.fixture
    def dim_sensor_df(self, spark: SparkSession):
        """DataFrame de dim_sensor con sensor_id."""
        return spark.createDataFrame(
            [
                {"sensor_sk": 10, "sensor_id": 10, "machine_name": "Machine A"},
            ]
        )

    @pytest.fixture
    def dim_machine_df(self, spark: SparkSession):
        """DataFrame de dim_machine con machine_id y line_id."""
        return spark.createDataFrame(
            [
                {
                    "machine_sk": 100,
                    "machine_id": 100,
                    "line_id": 1,
                    "machine_name": "Machine A",
                },
            ]
        )

    @pytest.fixture
    def dim_production_line_df(self, spark: SparkSession):
        """DataFrame de dim_production_line con line_id."""
        return spark.createDataFrame(
            [
                {"line_sk": 1, "line_id": 1},
            ]
        )

    @pytest.fixture
    def dim_date_df(self, spark: SparkSession):
        """DataFrame de dim_date."""
        return spark.createDataFrame(
            [{"date_sk": 20240115, "date_actual": datetime(2024, 1, 15).date()}]
        )

    @pytest.fixture
    def dim_time_df(self, spark: SparkSession):
        """DataFrame de dim_time."""
        return spark.createDataFrame(
            [
                {"time_sk": 10, "hour_of_day": 10},
            ]
        )

    @pytest.fixture
    def dim_alert_type_df(self, spark: SparkSession):
        """DataFrame de dim_alert_type con alert_type_name."""
        return spark.createDataFrame(
            [
                {
                    "alert_type_sk": 1,
                    "alert_type_name": "threshold_exceeded",
                    "severity": "high",
                },
            ]
        )

    def test_should_calculate_resolution_time(
        self,
        spark: SparkSession,
        alerts_df,
        dim_sensor_df,
        dim_machine_df,
        dim_production_line_df,
        dim_date_df,
        dim_time_df,
        dim_alert_type_df,
    ):
        """
        Verifica que se calcule resolution_time_min.
        """
        from transform.builders.facts.fact_alerts import build_fact_alerts

        result = build_fact_alerts(
            alerts_df=alerts_df,
            dim_date=dim_date_df,
            dim_time=dim_time_df,
            dim_sensor=dim_sensor_df,
            dim_machine=dim_machine_df,
            dim_production_line=dim_production_line_df,
            dim_alert_type=dim_alert_type_df,
        )

        check.greater(result.count(), 0, "Debe generar filas")

        # Verificar que tiene la columna resolution_time_min
        check.is_true(
            "resolution_time_min" in result.columns, "Debe tener resolution_time_min"
        )

    def test_should_join_dimension_keys(
        self,
        spark: SparkSession,
        alerts_df,
        dim_sensor_df,
        dim_machine_df,
        dim_production_line_df,
        dim_date_df,
        dim_time_df,
        dim_alert_type_df,
    ):
        """
        Verifica que se unan las SKs de dimensiones.
        """
        from transform.builders.facts.fact_alerts import build_fact_alerts

        result = build_fact_alerts(
            alerts_df=alerts_df,
            dim_date=dim_date_df,
            dim_time=dim_time_df,
            dim_sensor=dim_sensor_df,
            dim_machine=dim_machine_df,
            dim_production_line=dim_production_line_df,
            dim_alert_type=dim_alert_type_df,
        )

        # Verificar columnas SK presentes
        check.is_true("date_sk" in result.columns, "Debe tener date_sk")
        check.is_true("time_sk" in result.columns, "Debe tener time_sk")
        check.is_true("sensor_sk" in result.columns, "Debe tener sensor_sk")
