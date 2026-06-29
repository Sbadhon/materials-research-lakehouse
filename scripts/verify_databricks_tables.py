import argparse
import json
import subprocess
import sys
from typing import Any


EXPECTED_COUNTS = {
    "bronze": 3,
    "silver": 3,
    "quarantine": 0,
    "gold": 3,
}

EXPECTED_RANKING = [
    {
        "program_id": "CONDUCTIVE-COMPOSITES",
        "material_id": "MAT-COMP-001",
        "candidate_rank": 1,
        "candidate_score": 1.0,
    },
    {
        "program_id": "LIGHTWEIGHT-ALLOYS",
        "material_id": "MAT-AL-001",
        "candidate_rank": 1,
        "candidate_score": 0.55,
    },
    {
        "program_id": "LIGHTWEIGHT-ALLOYS",
        "material_id": "MAT-AL-002",
        "candidate_rank": 2,
        "candidate_score": 0.45,
    },
]


def execute_statement(
    profile: str,
    warehouse_id: str,
    statement: str,
) -> list[list[str | None]]:
    payload = {
        "warehouse_id": warehouse_id,
        "catalog": "workspace",
        "schema": "materials_research_dev",
        "statement": statement,
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
    }

    completed_process = subprocess.run(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/sql/statements",
            "--profile",
            profile,
            "--json",
            json.dumps(payload),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    response: dict[str, Any] = json.loads(
        completed_process.stdout
    )

    status = response.get("status", {}).get("state")

    if status != "SUCCEEDED":
        raise RuntimeError(
            f"Databricks statement did not succeed: {response}"
        )

    return response.get("result", {}).get("data_array", [])


def verify_counts(
    profile: str,
    warehouse_id: str,
) -> None:
    rows = execute_statement(
        profile,
        warehouse_id,
        """
        SELECT 'bronze' AS layer, COUNT(*) AS row_count
        FROM bronze_experiment_events

        UNION ALL

        SELECT 'silver' AS layer, COUNT(*) AS row_count
        FROM silver_experiment_events

        UNION ALL

        SELECT 'quarantine' AS layer, COUNT(*) AS row_count
        FROM quarantined_experiment_events

        UNION ALL

        SELECT 'gold' AS layer, COUNT(*) AS row_count
        FROM gold_material_candidates
        """,
    )

    actual_counts = {
        str(layer): int(row_count)
        for layer, row_count in rows
    }

    if actual_counts != EXPECTED_COUNTS:
        raise AssertionError(
            "Unexpected table counts.\n"
            f"Expected: {EXPECTED_COUNTS}\n"
            f"Actual:   {actual_counts}"
        )

    print("Table counts:")
    for layer, count in actual_counts.items():
        print(f"  {layer}: {count}")


def verify_gold_ranking(
    profile: str,
    warehouse_id: str,
) -> None:
    rows = execute_statement(
        profile,
        warehouse_id,
        """
        SELECT
            program_id,
            material_id,
            candidate_rank,
            candidate_score
        FROM gold_material_candidates
        ORDER BY
            program_id,
            candidate_rank
        """,
    )

    actual_ranking = [
        {
            "program_id": str(program_id),
            "material_id": str(material_id),
            "candidate_rank": int(candidate_rank),
            "candidate_score": float(candidate_score),
        }
        for (
            program_id,
            material_id,
            candidate_rank,
            candidate_score,
        ) in rows
    ]

    if actual_ranking != EXPECTED_RANKING:
        raise AssertionError(
            "Unexpected Gold candidate ranking.\n"
            f"Expected: {EXPECTED_RANKING}\n"
            f"Actual:   {actual_ranking}"
        )

    print("Gold candidate ranking:")
    for candidate in actual_ranking:
        print(
            "  "
            f"{candidate['program_id']} | "
            f"rank {candidate['candidate_rank']} | "
            f"{candidate['material_id']} | "
            f"score {candidate['candidate_score']}"
        )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify deployed materials research Databricks tables."
        )
    )

    parser.add_argument(
        "--profile",
        default="materials-research",
        help="Databricks CLI profile name.",
    )
    parser.add_argument(
        "--warehouse-id",
        required=True,
        help="Databricks SQL warehouse ID.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    verify_counts(
        arguments.profile,
        arguments.warehouse_id,
    )
    verify_gold_ranking(
        arguments.profile,
        arguments.warehouse_id,
    )

    print("Databricks smoke test passed.")


if __name__ == "__main__":
    try:
        main()
    except (
        AssertionError,
        RuntimeError,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
    ) as error:
        print(f"Smoke test failed: {error}", file=sys.stderr)
        sys.exit(1)
