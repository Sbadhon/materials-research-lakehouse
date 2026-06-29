from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_bronze_events(
    spark: SparkSession,
    source_path: Path,
) -> DataFrame:
    return (
        spark.read
        .text(str(source_path))
        .withColumnRenamed("value", "raw_payload")
        .filter(F.length(F.trim(F.col("raw_payload"))) > 0)
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn(
            "payload_sha256",
            F.sha2(F.col("raw_payload"), 256),
        )
    )
