"""
Builder para fact_production.

Granularidad: order + line + shift + date
Combina production_output con production_orders para métricas de producción.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_fact_production(
    production_output_df: DataFrame,
    production_orders_df: DataFrame,
    dim_date: DataFrame,
    dim_shift: DataFrame,
    dim_factory: DataFrame,
    dim_production_line: DataFrame,
    dim_product: DataFrame,
    dim_order: DataFrame,
) -> DataFrame:
    """
    Construye la tabla de hechos de producción.

    Métricas:
        - units_produced: Unidades producidas
        - units_defective: Unidades defectuosas
        - downtime_minutes: Minutos de tiempo muerto
        - efficiency_pct: Porcentaje de eficiencia
        - target_units: Unidades objetivo (de la orden)
        - cycle_time_avg: Tiempo de ciclo promedio

    Args:
        production_output_df: Tabla limpia de production_output
        production_orders_df: Tabla limpia de production_orders
        dim_*: Dimensiones ya construidas

    Returns:
        DataFrame con fact_production
    """
    # Preparar output con fecha extraída
    output = production_output_df.withColumn(
        "date_key", F.date_format("production_date", "yyyyMMdd").cast("int")
    )

    # Join con orders para obtener product_code y target
    output = output.join(
        production_orders_df.select(
            "order_id",
            "product_code",
            F.col("quantity_ordered").alias("target_units"),
        ),
        on="order_id",
        how="left",
    )

    # Join con dimensiones para obtener SKs
    # dim_date
    output = output.join(
        dim_date.select(F.col("date_sk"), F.col("date_actual")),
        output["date_key"] == dim_date["date_sk"],
        how="left",
    )

    # dim_shift (por shift_id)
    output = output.join(
        dim_shift.select(F.col("shift_sk"), F.col("shift_id")),
        on="shift_id",
        how="left",
    )

    # dim_product (por product_code)
    output = output.join(
        dim_product.select(
            F.col("product_sk"), F.col("product_code").alias("dim_product_code")
        ),
        output["product_code"] == F.col("dim_product_code"),
        how="left",
    )

    # dim_order (por order_id)
    output = output.join(
        dim_order.select(F.col("order_sk"), F.col("order_id").alias("dim_order_id")),
        output["order_id"] == F.col("dim_order_id"),
        how="left",
    )

    # Para factory_sk y line_sk necesitamos el line_id y factory_id
    output = output.join(
        dim_production_line.select(
            F.col("line_sk"),
            F.col("line_id"),
            F.col("factory_id"),
        ),
        on="line_id",
        how="left",
    )

    # Hacemos join con dim_factory para factory_sk
    output = output.join(
        dim_factory.select(
            F.col("factory_sk"),
            F.col("factory_id").alias("dim_factory_id"),
        ),
        output["factory_id"] == F.col("dim_factory_id"),
        how="left",
    )

    # Generar SK para el fact 
    window = Window.orderBy("output_id")
    output = output.withColumn("production_sk", F.row_number().over(window))

    # Calcular métricas derivadas
    output = output.withColumn(
        "cycle_time_avg",
        F.when(
            F.col("units_produced") > 0,
            (F.lit(480) - F.col("downtime_minutes")) # 480 minutos en un turno de 8 horas
            / F.col("units_produced"),  # Asumiendo turno de 8 hrs
        ).otherwise(F.lit(None)),
    )

    # Seleccionar columnas finales según el esquema
    return output.select(
        "production_sk",
        "date_sk",
        F.col("date_key").alias("time_sk"),  # Simplificado
        "factory_sk",
        "line_sk",
        "product_sk",
        "shift_sk",
        "order_sk",
        "units_produced",
        "units_defective",
        "downtime_minutes",
        "efficiency_percentage",
        "target_units",
        "cycle_time_avg",
    )
