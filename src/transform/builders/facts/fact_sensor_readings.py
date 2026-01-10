""" 
Builder para fact_sensor_readings.

Granularidad: 1 fila por lectura (evento).
Registra cada lectura del sensor con sus claves a dimensiones.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_sensor_readings(
    sensor_readings_df: DataFrame,
    dim_date: DataFrame,
    dim_time: DataFrame,
    dim_sensor: DataFrame,
    dim_machine: DataFrame,
    dim_production_line: DataFrame,
) -> DataFrame:
    """
    Construye la tabla de hechos de lecturas de sensores (evento).

    Cada registro representa una lectura individual del sensor.

    Métricas (a nivel evento):
        - value
        - is_anomaly
        - quality_score

    Args:
        sensor_readings_df: Tabla limpia de sensor_readings
        dim_*: Dimensiones ya construidas

    Returns:
        DataFrame con fact_sensor_readings
    """
    # Normalizar timestamp (en silver viene como string)
    readings = sensor_readings_df.withColumn(
        "timestamp_ts", F.to_timestamp(F.col("timestamp"))
    )

    # Claves derivadas para dimensión de fecha y tiempo (hora)
    readings = readings.withColumn(
        "date_key", F.date_format(F.col("timestamp_ts"), "yyyyMMdd").cast("int")
    ).withColumn("hour_key", F.hour(F.col("timestamp_ts")))

    # Join con dimensiones para obtener SKs
    # dim_date
    readings = readings.join(
        dim_date.select(F.col("date_sk"), F.col("date_actual")),
        readings["date_key"] == dim_date["date_sk"],
        how="left",
    )

    # dim_time
    readings = readings.join(
        dim_time.select(F.col("time_sk"), F.col("hour_of_day")),
        readings["hour_key"] == dim_time["hour_of_day"],
        how="left",
    )

    # dim_sensor
    readings = readings.join(
        dim_sensor.select(
            F.col("sensor_sk"), F.col("sensor_id").alias("dim_sensor_id")
        ),
        readings["sensor_id"] == F.col("dim_sensor_id"),
        how="left",
    )

    # dim_machine (incluye line_id para derivar line_sk)
    readings = readings.join(
        dim_machine.select(
            F.col("machine_sk"),
            F.col("machine_id").alias("dim_machine_id"),
            F.col("line_id"),
        ),
        readings["machine_id"] == F.col("dim_machine_id"),
        how="left",
    )

    # dim_production_line (line_sk via line_id de machine)
    readings = readings.join(
        dim_production_line.select(
            F.col("line_sk"),
            F.col("line_id").alias("dim_line_id"),
        ),
        readings["line_id"] == F.col("dim_line_id"),
        how="left",
    )

    # SK del evento: usar reading_id
    readings = readings.withColumn("sensor_reading_sk", F.col("reading_id").cast("long"))

    # Seleccionar columnas finales (evento)
    return readings.select(
        "sensor_reading_sk",
        "date_sk",
        "time_sk",
        "sensor_sk",
        "machine_sk",
        "line_sk",
        F.col("timestamp_ts").alias("timestamp"),
        F.col("value").cast("double").alias("value"),
        F.col("is_anomaly").cast("boolean").alias("is_anomaly"),
        F.col("quality_score").cast("int").alias("quality_score"),
    )
