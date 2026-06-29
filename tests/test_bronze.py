import hashlib
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from materials_research_lakehouse.bronze import read_bronze_events


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_EVENTS_PATH = (
    PROJECT_ROOT / "data" / "sample" / "experiment-events.jsonl"
)


def test_bronze_preserves_every_nonempty_source_record(
    spark: SparkSession,
) -> None:
    bronze_events = read_bronze_events(
        spark,
        SAMPLE_EVENTS_PATH,
    )

    assert bronze_events.count() == 3
    assert bronze_events.columns == [
        "raw_payload",
        "source_file",
        "ingested_at",
        "payload_sha256",
    ]


def test_bronze_adds_ingestion_metadata_and_payload_hash(
    spark: SparkSession,
) -> None:
    bronze_events = read_bronze_events(
        spark,
        SAMPLE_EVENTS_PATH,
    )

    first_event = (
        bronze_events
        .filter(
            F.col("raw_payload").contains(
                '"experiment_id":"EXP-2026-001"'
            )
        )
        .first()
    )

    assert first_event is not None
    assert first_event.source_file.endswith(
        "experiment-events.jsonl"
    )
    assert first_event.ingested_at is not None

    expected_hash = hashlib.sha256(
        first_event.raw_payload.encode("utf-8")
    ).hexdigest()

    assert first_event.payload_sha256 == expected_hash
