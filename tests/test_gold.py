from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from materials_research_lakehouse.bronze import read_bronze_events
from materials_research_lakehouse.gold import (
    build_material_candidate_scores,
)
from materials_research_lakehouse.silver import (
    build_silver_events,
    valid_silver_events,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_EVENTS_PATH = (
    PROJECT_ROOT
    / "data"
    / "sample"
    / "experiment-events.jsonl"
)


def create_valid_events(
    spark: SparkSession,
):
    bronze_events = read_bronze_events(
        spark,
        SAMPLE_EVENTS_PATH,
    )

    silver_events = build_silver_events(bronze_events)

    return valid_silver_events(silver_events)


def test_builds_ranked_material_candidate_scores(
    spark: SparkSession,
) -> None:
    candidate_scores = build_material_candidate_scores(
        create_valid_events(spark)
    )

    assert candidate_scores.count() == 3

    alloy_candidates = (
        candidate_scores
        .filter(
            F.col("program_id")
            == "LIGHTWEIGHT-ALLOYS"
        )
        .orderBy("candidate_rank")
        .collect()
    )

    assert len(alloy_candidates) == 2

    first_candidate = alloy_candidates[0]
    second_candidate = alloy_candidates[1]

    assert first_candidate.material_id == "MAT-AL-001"
    assert first_candidate.candidate_rank == 1
    assert first_candidate.candidate_score == 0.55

    assert second_candidate.material_id == "MAT-AL-002"
    assert second_candidate.candidate_rank == 2
    assert second_candidate.candidate_score == 0.45


def test_gold_model_ignores_duplicate_event_ids(
    spark: SparkSession,
) -> None:
    valid_events = create_valid_events(spark)

    duplicated_events = valid_events.unionByName(
        valid_events
    )

    candidate_scores = build_material_candidate_scores(
        duplicated_events
    )

    first_material = (
        candidate_scores
        .filter(
            F.col("material_id") == "MAT-AL-001"
        )
        .first()
    )

    assert first_material is not None
    assert first_material.experiment_count == 1
    assert (
        first_material.average_tensile_strength_mpa
        == 310.0
    )
