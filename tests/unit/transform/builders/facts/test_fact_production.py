"""
Pruebas unitarias para build_fact_production.
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
        .appName("TestFactProduction")
        .getOrCreate()
    )


class TestBuildFactProduction:
    """Pruebas para el builder de fact_production."""

    @pytest.fixture
    def production_output_df(self, spark: SparkSession):
        """DataFrame de producción con todos los campos requeridos."""
        return spark.createDataFrame(
            [
                {
                    "output_id": 1,
                    "order_id": 100,
                    "line_id": 10,
                    "shift_id": 1,
                    "production_date": date(2024, 1, 15),
                    "units_produced": 500,
                    "units_defective": 5,
                    "downtime_minutes": 30,
                    "efficiency_percentage": 95.0,
                },
            ]
        )

    @pytest.fixture
    def production_orders_df(self, spark: SparkSession):
        """DataFrame de órdenes de producción."""
        return spark.createDataFrame(
            [
                {
                    "order_id": 100,
                    "product_code": "PROD-A",
                    "quantity_ordered": 1000,
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
    def dim_shift_df(self, spark: SparkSession):
        """DataFrame de dim_shift con shift_id."""
        return spark.createDataFrame([{"shift_sk": 1, "shift_id": 1}])

    @pytest.fixture
    def dim_factory_df(self, spark: SparkSession):
        """DataFrame de dim_factory con factory_id."""
        return spark.createDataFrame([{"factory_sk": 1, "factory_id": 1}])

    @pytest.fixture
    def dim_production_line_df(self, spark: SparkSession):
        """DataFrame de dim_production_line con line_id y factory_id."""
        return spark.createDataFrame([{"line_sk": 10, "line_id": 10, "factory_id": 1}])

    @pytest.fixture
    def dim_product_df(self, spark: SparkSession):
        """DataFrame de dim_product con product_code."""
        return spark.createDataFrame([{"product_sk": 50, "product_code": "PROD-A"}])

    @pytest.fixture
    def dim_order_df(self, spark: SparkSession):
        """DataFrame de dim_order con order_id."""
        return spark.createDataFrame(
            [{"order_sk": 100, "order_id": 100, "product_code": "PROD-A"}]
        )

    def test_should_join_dimension_surrogate_keys(
        self,
        spark: SparkSession,
        production_output_df,
        production_orders_df,
        dim_date_df,
        dim_shift_df,
        dim_factory_df,
        dim_production_line_df,
        dim_product_df,
        dim_order_df,
    ):
        """
        Verifica que se unan correctamente las SKs de dimensiones.
        """
        from transform.builders.facts.fact_production import build_fact_production

        result = build_fact_production(
            production_output_df=production_output_df,
            production_orders_df=production_orders_df,
            dim_date=dim_date_df,
            dim_shift=dim_shift_df,
            dim_factory=dim_factory_df,
            dim_production_line=dim_production_line_df,
            dim_product=dim_product_df,
            dim_order=dim_order_df,
        )

        check.greater(result.count(), 0, "Debe haber al menos 1 fila después de joins")

        row = result.collect()[0]

        # Verificar SKs de dimensiones
        check.equal(row["date_sk"], 20240115, "date_sk debe ser 20240115")

    def test_should_include_production_metrics(
        self,
        spark: SparkSession,
        production_output_df,
        production_orders_df,
        dim_date_df,
        dim_shift_df,
        dim_factory_df,
        dim_production_line_df,
        dim_product_df,
        dim_order_df,
    ):
        """
        Verifica que las métricas de producción estén presentes.
        """
        from transform.builders.facts.fact_production import build_fact_production

        result = build_fact_production(
            production_output_df=production_output_df,
            production_orders_df=production_orders_df,
            dim_date=dim_date_df,
            dim_shift=dim_shift_df,
            dim_factory=dim_factory_df,
            dim_production_line=dim_production_line_df,
            dim_product=dim_product_df,
            dim_order=dim_order_df,
        )

        row = result.collect()[0]

        # Verificar métricas están presentes
        check.equal(row["units_produced"], 500, "units_produced debe ser 500")
        check.equal(row["units_defective"], 5, "units_defective debe ser 5")
