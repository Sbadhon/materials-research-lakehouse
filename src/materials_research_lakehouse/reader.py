from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from materials_research_lakehouse.schemas import experiment_event_schema


def read_experiment_events(
    spark: SparkSession,
    source_path: Path,
) -> DataFrame:
    return (
        spark.read
        .schema(experiment_event_schema)
        .json(str(source_path))
    )
