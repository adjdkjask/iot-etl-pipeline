"""
Pruebas unitarias para build_fact_quality.
"""

import pytest
import pytest_check as check
from pyspark.sql import SparkSession
from datetime import date


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Fixture que crea una sesión de Spark para tests."""
    return (
        SparkSession.builder.master("local[1]").appName("TestFactQuality").getOrCreate()
    )


class TestBuildFactQuality:
    """Pruebas para el builder de fact_quality."""

    @pytest.fixture
    def quality_checks_df(self, spark: SparkSession):
        """DataFrame de quality checks."""
        return spark.createDataFrame(
            [
                {
                    "check_id": 1,
                    "order_id": 100,
                    "line_id": 10,
                    "inspector_id": 5,
                    "check_date": date(2024, 1, 15),
                    "result": "pass",
                    "defects_found": 0,
                    "sample_size": 50,
                },
                {
                    "check_id": 2,
                    "order_id": 100,
                    "line_id": 10,
                    "inspector_id": 5,
                    "check_date": date(2024, 1, 15),
                    "result": "fail",
                    "defects_found": 5,
                    "sample_size": 50,
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
    def dim_production_line_df(self, spark: SparkSession):
        """DataFrame de dim_production_line con line_id."""
        return spark.createDataFrame([{"line_sk": 10, "line_id": 10}])

    @pytest.fixture
    def dim_product_df(self, spark: SparkSession):
        """DataFrame de dim_product."""
        return spark.createDataFrame([{"product_sk": 50, "product_code": "PROD-A"}])

    @pytest.fixture
    def dim_order_df(self, spark: SparkSession):
        """DataFrame de dim_order con order_id y product_code."""
        return spark.createDataFrame(
            [{"order_sk": 100, "order_id": 100, "product_code": "PROD-A"}]
        )

    @pytest.fixture
    def dim_operator_df(self, spark: SparkSession):
        """DataFrame de dim_operator con operator_id."""
        return spark.createDataFrame([{"operator_sk": 5, "operator_id": 5}])

    def test_should_calculate_defect_rate(
        self,
        spark: SparkSession,
        quality_checks_df,
        dim_date_df,
        dim_production_line_df,
        dim_product_df,
        dim_order_df,
        dim_operator_df,
    ):
        """
        Verifica que se calcule correctamente la tasa de defectos.
        """
        from transform.builders.facts.fact_quality import build_fact_quality

        result = build_fact_quality(
            quality_checks_df=quality_checks_df,
            dim_date=dim_date_df,
            dim_production_line=dim_production_line_df,
            dim_product=dim_product_df,
            dim_order=dim_order_df,
            dim_operator=dim_operator_df,
        )

        check.greater(result.count(), 0, "Debe generar filas")

        # Verificar que tiene la columna defect_rate_pct
        check.is_true("defect_rate_pct" in result.columns, "Debe tener defect_rate_pct")

    def test_should_include_pass_flag(
        self,
        spark: SparkSession,
        quality_checks_df,
        dim_date_df,
        dim_production_line_df,
        dim_product_df,
        dim_order_df,
        dim_operator_df,
    ):
        """
        Verifica que se incluya flag de pass.
        """
        from transform.builders.facts.fact_quality import build_fact_quality

        result = build_fact_quality(
            quality_checks_df=quality_checks_df,
            dim_date=dim_date_df,
            dim_production_line=dim_production_line_df,
            dim_product=dim_product_df,
            dim_order=dim_order_df,
            dim_operator=dim_operator_df,
        )

        # Verificar columna check_passed
        check.is_true(
            "check_passed" in result.columns, "Debe tener columna check_passed"
        )
