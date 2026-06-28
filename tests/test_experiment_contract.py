import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "contracts" / "experiment-event.schema.json"
EVENTS_PATH = PROJECT_ROOT / "data" / "sample" / "experiment-events.jsonl"


def load_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def load_events() -> list[dict]:
    return [
        json.loads(line)
        for line in EVENTS_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_experiment_schema_is_valid() -> None:
    schema = load_schema()

    Draft202012Validator.check_schema(schema)


def test_sample_events_match_experiment_schema() -> None:
    schema = load_schema()
    events = load_events()

    assert events, "The sample event file must contain at least one event."

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
    )

    validation_errors: list[str] = []

    for line_number, event in enumerate(events, start=1):
        errors = sorted(
            validator.iter_errors(event),
            key=lambda error: list(error.absolute_path),
        )

        for error in errors:
            field_path = ".".join(str(part) for part in error.absolute_path)
            location = field_path or "<root>"
            validation_errors.append(
                f"Line {line_number}, field {location}: {error.message}"
            )

    assert not validation_errors, "\n".join(validation_errors)
