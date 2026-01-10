"""
Pruebas unitarias para build_fact_sensor_readings.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession
from datetime import date, datetime


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestFactSensorReadings")
        .getOrCreate()
    )


class TestBuildFactSensorReadings:
    """Pruebas para el builder de fact_sensor_readings."""

    @pytest.fixture
    def sensor_readings_df(self, spark: SparkSession):
        """DataFrame de lecturas de sensores."""
        return spark.createDataFrame(
            [
                {
                    "reading_id": 1001,
                    "sensor_id": 10,
                    "machine_id": 100,
                    "timestamp": "2024-01-15 10:30:45",
                    "value": 75.5,
                    "unit": "°C",
                    "quality_score": 95,
                    "is_anomaly": False,
                },
                {
                    "reading_id": 1002,
                    "sensor_id": 10,
                    "machine_id": 100,
                    "timestamp": "2024-01-15 10:31:20",
                    "value": 76.2,
                    "unit": "°C",
                    "quality_score": 98,
                    "is_anomaly": False,
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
                {"line_sk": 1, "line_id": 1, "line_name": "Line 1"},
            ]
        )

    @pytest.fixture
    def dim_date_df(self, spark: SparkSession):
        """DataFrame de dim_date."""
        return spark.createDataFrame(
            [{"date_sk": 20240115, "date_actual": date(2024, 1, 15)}]
        )

    @pytest.fixture
    def dim_time_df(self, spark: SparkSession):
        """DataFrame de dim_time."""
        return spark.createDataFrame(
            [
                {"time_sk": 10, "hour_of_day": 10},
            ]
        )

    def test_should_derive_date_and_time_keys_from_timestamp(
        self,
        spark: SparkSession,
        sensor_readings_df,
        dim_sensor_df,
        dim_machine_df,
        dim_production_line_df,
        dim_date_df,
        dim_time_df,
    ):
        """
        Verifica que se deriven correctamente date_sk y time_sk del timestamp.
        """
        from transform.builders.facts.fact_sensor_readings import (
            build_fact_sensor_readings,
        )

        result = build_fact_sensor_readings(
            sensor_readings_df=sensor_readings_df,
            dim_date=dim_date_df,
            dim_time=dim_time_df,
            dim_sensor=dim_sensor_df,
            dim_machine=dim_machine_df,
            dim_production_line=dim_production_line_df,
        )

        # Verificar que hay filas
        check.greater(result.count(), 0, "Debe generar filas")

        # Verificar que tiene las columnas date_sk y time_sk
        check.is_true("date_sk" in result.columns, "Debe tener date_sk")
        check.is_true("time_sk" in result.columns, "Debe tener time_sk")

    def test_should_maintain_event_granularity(
        self,
        spark: SparkSession,
        sensor_readings_df,
        dim_sensor_df,
        dim_machine_df,
        dim_production_line_df,
        dim_date_df,
        dim_time_df,
    ):
        """
        Verifica que se mantenga granularidad a nivel de evento (sin agregación).
        """
        from transform.builders.facts.fact_sensor_readings import (
            build_fact_sensor_readings,
        )

        result = build_fact_sensor_readings(
            sensor_readings_df=sensor_readings_df,
            dim_date=dim_date_df,
            dim_time=dim_time_df,
            dim_sensor=dim_sensor_df,
            dim_machine=dim_machine_df,
            dim_production_line=dim_production_line_df,
        )

        # Debe haber 2 lecturas (el input tenía 2)
        check.equal(result.count(), 2, "Debe mantener las 2 lecturas originales")
