"""
Builder para fact_maintenance.

Granularidad: maintenance_log + machine + date
Registra eventos de mantenimiento con sus costos y tiempos.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_maintenance(
    maintenance_logs_df: DataFrame,
    dim_date: DataFrame,
    dim_machine: DataFrame,
    dim_production_line: DataFrame,
    dim_maintenance_type: DataFrame,
    dim_operator: DataFrame,
) -> DataFrame:
    """
    Construye la tabla de hechos de mantenimiento.

    Métricas:
        - cost_amount: Costo del mantenimiento
        - downtime_hours: Horas de tiempo muerto
        - parts_replaced_count: Partes reemplazadas
        - labor_hours: Horas de trabajo

    Args:
        maintenance_logs_df: Tabla limpia de maintenance_logs
        dim_*: Dimensiones ya construidas

    Returns:
        DataFrame con fact_maintenance
    """
    # Preparar logs con fecha extraída
    logs = maintenance_logs_df.withColumn(
        "date_key", F.date_format("performed_date", "yyyyMMdd").cast("int")
    )

    # Labor hours: derivado del downtime o estimado
    logs = logs.withColumn("labor_hours", F.coalesce(F.col("downtime_hours"), F.lit(0)))

    # Join con dimensiones para obtener SKs
    # dim_date
    logs = logs.join(
        dim_date.select(F.col("date_sk"), F.col("date_actual")),
        logs["date_key"] == dim_date["date_sk"],
        how="left",
    )

    # dim_machine (incluye line_id para derivar line_sk)
    logs = logs.join(
        dim_machine.select(
            F.col("machine_sk"),
            F.col("machine_id").alias("dim_machine_id"),
            F.col("line_id"),
        ),
        logs["machine_id"] == F.col("dim_machine_id"),
        how="left",
    )

    # dim_production_line (line_sk via line_id de machine)
    logs = logs.join(
        dim_production_line.select(
            F.col("line_sk"),
            F.col("line_id").alias("dim_line_id"),
        ),
        logs["line_id"] == F.col("dim_line_id"),
        how="left",
    )

    # dim_maintenance_type
    logs = logs.join(
        dim_maintenance_type.select(
            F.col("maint_type_sk").alias("maintenance_type_sk"),
            F.col("maint_type_name"),
        ),
        logs["maintenance_type"] == F.col("maint_type_name"),
        how="left",
    )

    # dim_operator (performed_by -> operator_sk como technician)
    # performed_by contiene 'Technician X', extraemos el número para mapear con operator_id
    logs = logs.withColumn(
        "technician_id_extracted",
        F.when(
            F.col("performed_by").rlike(r"\d+"),
            F.regexp_extract(F.col("performed_by"), r"(\d+)", 1).cast("long"),
        ).otherwise(F.lit(None).cast("long")),
    )

    logs = logs.join(
        dim_operator.select(
            F.col("operator_sk").alias("technician_sk"),
            F.col("operator_id").alias("dim_technician_id"),
        ),
        logs["technician_id_extracted"] == F.col("dim_technician_id"),
        how="left",
    )

    # Generar SK para el fact
    logs = logs.withColumn("maintenance_sk", F.monotonically_increasing_id())

    # Seleccionar columnas finales según el esquema
    return logs.select(
        "maintenance_sk",
        "date_sk",
        "machine_sk",
        "line_sk",
        "maintenance_type_sk",
        F.col("technician_sk").alias("operator_sk"),
        F.col("cost").alias("cost_amount"),
        "downtime_hours",
        "labor_hours",
    )
