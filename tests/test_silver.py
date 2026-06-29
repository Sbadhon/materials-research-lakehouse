import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from materials_research_lakehouse.bronze import read_bronze_events
from materials_research_lakehouse.silver import (
    build_silver_events,
    quarantined_silver_events,
    valid_silver_events,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_EVENTS_PATH = (
    PROJECT_ROOT
    / "data"
    / "sample"
    / "experiment-events.jsonl"
)


def test_builds_typed_valid_silver_events(
    spark: SparkSession,
) -> None:
    bronze_events = read_bronze_events(
        spark,
        SAMPLE_EVENTS_PATH,
    )

    silver_events = build_silver_events(bronze_events)

    assert silver_events.count() == 3
    assert valid_silver_events(silver_events).count() == 3
    assert quarantined_silver_events(silver_events).count() == 0

    first_event = (
        silver_events
        .withColumn(
            "occurred_at_epoch",
            F.unix_timestamp("occurred_at"),
        )
        .orderBy("experiment_id")
        .first()
    )

    assert first_event is not None
    assert first_event.experiment_id == "EXP-2026-001"
    assert first_event.material_family == "aluminum-alloy"
    assert first_event.composition_mass_percent_total == 100.0

    expected_epoch = int(
        datetime(
            2026,
            6,
            20,
            14,
            15,
            tzinfo=timezone.utc,
        ).timestamp()
    )

    assert first_event.occurred_at_epoch == expected_epoch
    assert first_event.validation_errors == []
    assert first_event.is_valid is True


def test_quarantines_malformed_and_invalid_events(
    spark: SparkSession,
) -> None:
    valid_payload = json.loads(
        SAMPLE_EVENTS_PATH.read_text(
            encoding="utf-8"
        ).splitlines()[0]
    )

    invalid_composition = json.loads(
        json.dumps(valid_payload)
    )
    invalid_composition["event_id"] = (
        "629cebee-4e90-49ce-a9fd-492d92e155a5"
    )
    invalid_composition["experiment"]["experiment_id"] = (
        "EXP-2026-INVALID"
    )
    invalid_composition["experiment"]["material"][
        "composition"
    ][0]["mass_percent"] = 80.0

    raw_payloads = [
        json.dumps(
            valid_payload,
            separators=(",", ":"),
        ),
        '{"schema_version":',
        json.dumps(
            invalid_composition,
            separators=(",", ":"),
        ),
    ]

    bronze_rows = [
        (
            raw_payload,
            f"memory://event-{index}.json",
            datetime(2026, 6, 28, 12, 0),
            hashlib.sha256(
                raw_payload.encode("utf-8")
            ).hexdigest(),
        )
        for index, raw_payload in enumerate(
            raw_payloads,
            start=1,
        )
    ]

    bronze_events = spark.createDataFrame(
        bronze_rows,
        [
            "raw_payload",
            "source_file",
            "ingested_at",
            "payload_sha256",
        ],
    )

    silver_events = build_silver_events(bronze_events)

    assert valid_silver_events(silver_events).count() == 1
    assert quarantined_silver_events(silver_events).count() == 2

    malformed_event = (
        silver_events
        .filter(
            F.col("source_file")
            == "memory://event-2.json"
        )
        .first()
    )

    invalid_composition_event = (
        silver_events
        .filter(
            F.col("experiment_id")
            == "EXP-2026-INVALID"
        )
        .first()
    )

    assert malformed_event is not None
    assert malformed_event.validation_errors == [
        "MALFORMED_JSON"
    ]

    assert invalid_composition_event is not None
    assert (
        "COMPOSITION_TOTAL_NOT_100"
        in invalid_composition_event.validation_errors
    )
