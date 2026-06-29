from pathlib import Path
from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession

from materials_research_lakehouse.reader import read_experiment_events


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_EVENTS_PATH = (
    PROJECT_ROOT / "data" / "sample" / "experiment-events.jsonl"
)


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("materials-research-lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield session

    session.stop()


def test_reads_all_sample_experiment_events(
    spark: SparkSession,
) -> None:
    events = read_experiment_events(spark, SAMPLE_EVENTS_PATH)

    assert events.count() == 3


def test_preserves_nested_experiment_data(
    spark: SparkSession,
) -> None:
    events = read_experiment_events(spark, SAMPLE_EVENTS_PATH)

    first_event = (
        events
        .select(
            "event_id",
            "experiment.experiment_id",
            "experiment.material.family",
            "experiment.measurements.tensile_strength_mpa",
        )
        .orderBy("experiment_id")
        .first()
    )

    assert first_event is not None
    assert first_event.event_id == "3d9af7b0-b136-4a7f-b384-d3ab66c64a01"
    assert first_event.experiment_id == "EXP-2026-001"
    assert first_event.family == "aluminum-alloy"
    assert first_event.tensile_strength_mpa == 310.0
