"""
Builders para dimensiones derivadas.

Las dimensiones derivadas son aquellas que extraen valores únicos
de una o más columnas de las tablas fuente y les asignan una SK o que
añaden métricas adicionales.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_product(production_orders_df: DataFrame) -> DataFrame:
    """
    Construye dim_product extrayendo productos únicos de production_orders.

    Extrae combinaciones únicas de product_code y atributos relacionados.
    """
    df = production_orders_df.select(
        "product_code",
    ).distinct()

    # Generar SK
    window = Window.orderBy("product_code")
    df = df.withColumn("product_sk", F.row_number().over(window))

    # Crear nombre descriptivo a partir del código
    df = df.withColumn("product_name", F.col("product_code"))
    df = df.withColumn("unit_of_measure", F.lit("units"))
    df = df.withColumn("priority_default", F.lit("medium"))

    return df.select(
        "product_sk",
        "product_code",
        "product_name",
        "unit_of_measure",
        "priority_default",
    )


def build_dim_alert_type(alerts_df: DataFrame) -> DataFrame:
    """
    Construye dim_alert_type extrayendo tipos de alertas únicos.
    """
    df = alerts_df.select(
        "alert_type",
        "severity",
    ).distinct()

    # Generar SK
    window = Window.orderBy("alert_type", "severity")
    df = df.withColumn("alert_type_sk", F.row_number().over(window))

    # Agregar atributos
    df = df.withColumn("alert_type_name", F.col("alert_type"))
    df = df.withColumn("severity_level", F.col("severity"))
    df = df.withColumn(
        "category",
        F.when(F.col("alert_type").contains("threshold"), "Quality")
        .when(F.col("alert_type").contains("quality"), "Environmental")
        .when(F.col("alert_type").contains("machine"), "Mechanical")
        .when(F.col("alert_type").contains("offline"), "Electrical")
        .otherwise("Operational"),
    )
    df = df.withColumn(
        "requires_ack",
        F.when(F.col("severity").isin("critical", "high"), True).otherwise(False),
    )

    return df.select(
        "alert_type_sk",
        "alert_type_name",
        "severity_level",
        "category",
        "requires_ack",
    )


def build_dim_maintenance_type(maintenance_logs_df: DataFrame) -> DataFrame:
    """
    Construye dim_maintenance_type extrayendo tipos de mantenimiento únicos.
    """
    df = maintenance_logs_df.select(
        "maintenance_type",
    ).distinct()

    # Generar SK
    window = Window.orderBy("maintenance_type")
    df = df.withColumn("maint_type_sk", F.row_number().over(window))

    # Cambio de nombre
    df = df.withColumn("maint_type_name", F.col("maintenance_type"))

    return df.select(
        "maint_type_sk",
        "maint_type_name"
    )
