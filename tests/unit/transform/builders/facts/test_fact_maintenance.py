"""
Pruebas unitarias para build_fact_maintenance.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession
from datetime import date


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestFactMaintenance")
        .getOrCreate()
    )


class TestBuildFactMaintenance:
    """Pruebas para el builder de fact_maintenance."""

    @pytest.fixture
    def maintenance_logs_df(self, spark: SparkSession):
        """DataFrame de logs de mantenimiento."""
        return spark.createDataFrame(
            [
                {
                    "log_id": 1,
                    "machine_id": 10,
                    "performed_by": "Technician 5",
                    "maintenance_type": "preventive",
                    "performed_date": date(2024, 1, 15),
                    "downtime_hours": 2.5,
                    "parts_replaced": "belt,filter",
                    "cost": 150.0,
                },
            ]
        )

    @pytest.fixture
    def dim_date_df(self, spark: SparkSession):
        """DataFrame de dim_date."""
        return spark.createDataFrame(
            [{"date_sk": 20240115, "date_actual": date(2024, 1, 15)}]
        )

    @pytest.fixture
    def dim_machine_df(self, spark: SparkSession):
        """DataFrame de dim_machine con machine_id y line_id."""
        return spark.createDataFrame(
            [
                {"machine_sk": 10, "machine_id": 10, "line_id": 1},
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
    def dim_operator_df(self, spark: SparkSession):
        """DataFrame de dim_operator con operator_id."""
        return spark.createDataFrame(
            [
                {"operator_sk": 5, "operator_id": 5},
            ]
        )

    @pytest.fixture
    def dim_maintenance_type_df(self, spark: SparkSession):
        """DataFrame de dim_maintenance_type con maint_type_name."""
        return spark.createDataFrame(
            [
                {"maint_type_sk": 1, "maint_type_name": "preventive"},
            ]
        )

    def test_should_join_dimension_keys(
        self,
        spark: SparkSession,
        maintenance_logs_df,
        dim_date_df,
        dim_machine_df,
        dim_production_line_df,
        dim_operator_df,
        dim_maintenance_type_df,
    ):
        """
        Verifica que se unan correctamente las SKs de dimensiones.
        """
        from transform.builders.facts.fact_maintenance import build_fact_maintenance

        result = build_fact_maintenance(
            maintenance_logs_df=maintenance_logs_df,
            dim_date=dim_date_df,
            dim_machine=dim_machine_df,
            dim_production_line=dim_production_line_df,
            dim_maintenance_type=dim_maintenance_type_df,
            dim_operator=dim_operator_df,
        )

        check.greater(result.count(), 0, "Debe generar filas")

        # Verificar que tiene las columnas de SK esperadas
        check.is_true("date_sk" in result.columns, "Debe tener date_sk")
        check.is_true("machine_sk" in result.columns, "Debe tener machine_sk")

    def test_should_include_maintenance_metrics(
        self,
        spark: SparkSession,
        maintenance_logs_df,
        dim_date_df,
        dim_machine_df,
        dim_production_line_df,
        dim_operator_df,
        dim_maintenance_type_df,
    ):
        """
        Verifica que se incluyan las métricas de mantenimiento.
        """
        from transform.builders.facts.fact_maintenance import build_fact_maintenance

        result = build_fact_maintenance(
            maintenance_logs_df=maintenance_logs_df,
            dim_date=dim_date_df,
            dim_machine=dim_machine_df,
            dim_production_line=dim_production_line_df,
            dim_maintenance_type=dim_maintenance_type_df,
            dim_operator=dim_operator_df,
        )

        # Verificar métricas presentes
        check.is_true("cost_amount" in result.columns, "Debe tener cost_amount")
        check.is_true("downtime_hours" in result.columns, "Debe tener downtime_hours")
