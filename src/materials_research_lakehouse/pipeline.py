from pyspark import pipelines as dp
from pyspark.sql import functions as F

from materials_research_lakehouse.gold import (
    build_material_candidate_scores,
)
from materials_research_lakehouse.silver import (
    build_silver_events,
    quarantined_silver_events,
    valid_silver_events,
)


SOURCE_PATH = spark.conf.get("materials.source_path")


@dp.table(
    name="bronze_experiment_events",
    comment="Raw materials research experiment events with ingestion metadata.",
)
def bronze_experiment_events():
    raw_events = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load(SOURCE_PATH)
    )

    return (
        raw_events
        .select(
            F.col("value").alias("raw_payload"),
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingested_at"),
            F.sha2(F.col("value"), 256).alias(
                "payload_sha256"
            ),
        )
        .filter(
            F.length(F.trim(F.col("raw_payload"))) > 0
        )
    )


@dp.table(
    name="validated_experiment_events",
    comment="Parsed experiment events with validation results.",
    temporary=True,
)
def validated_experiment_events():
    bronze_events = spark.readStream.table(
        "bronze_experiment_events"
    )

    return build_silver_events(bronze_events)


@dp.table(
    name="silver_experiment_events",
    comment="Validated and normalized experiment events.",
)
def silver_experiment_events():
    validated_events = spark.readStream.table(
        "validated_experiment_events"
    )

    return valid_silver_events(validated_events)


@dp.table(
    name="quarantined_experiment_events",
    comment="Experiment events that failed technical or scientific validation.",
)
def quarantined_experiment_events():
    validated_events = spark.readStream.table(
        "validated_experiment_events"
    )

    return quarantined_silver_events(validated_events)


@dp.materialized_view(
    name="gold_material_candidates",
    comment="Program-level material candidate scores and rankings.",
)
def gold_material_candidates():
    silver_events = spark.read.table(
        "silver_experiment_events"
    )

    return build_material_candidate_scores(
        silver_events
    )
