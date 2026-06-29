from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from materials_research_lakehouse.schemas import experiment_event_schema


experiment_event_parsing_schema = StructType(
    [
        *experiment_event_schema.fields,
        StructField(
            "_corrupt_record",
            StringType(),
            nullable=True,
        ),
    ]
)


def _is_blank(column_name: str) -> Column:
    column = F.col(column_name)

    return column.isNull() | (F.trim(column) == "")


def build_silver_events(bronze_events: DataFrame) -> DataFrame:
    silver_events = (
        bronze_events
        .withColumn(
            "parsed_event",
            F.from_json(
                F.col("raw_payload"),
                experiment_event_parsing_schema,
                {
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                },
            ),
        )
        .withColumn(
            "occurred_at",
            F.to_timestamp(F.col("parsed_event.occurred_at")),
        )
        .withColumn(
            "composition_mass_percent_total",
            F.expr(
                """
                aggregate(
                    parsed_event.experiment.material.composition,
                    0D,
                    (total, item) -> total + item.mass_percent
                )
                """
            ),
        )
        .withColumn(
            "schema_version",
            F.col("parsed_event.schema_version"),
        )
        .withColumn(
            "event_id",
            F.col("parsed_event.event_id"),
        )
        .withColumn(
            "event_type",
            F.col("parsed_event.event_type"),
        )
        .withColumn(
            "source_system",
            F.col("parsed_event.source_system"),
        )
        .withColumn(
            "experiment_id",
            F.col("parsed_event.experiment.experiment_id"),
        )
        .withColumn(
            "program_id",
            F.col("parsed_event.experiment.program_id"),
        )
        .withColumn(
            "lab_id",
            F.col("parsed_event.experiment.lab_id"),
        )
        .withColumn(
            "material_id",
            F.col(
                "parsed_event.experiment.material.material_id"
            ),
        )
        .withColumn(
            "material_family",
            F.col("parsed_event.experiment.material.family"),
        )
        .withColumn(
            "composition",
            F.col(
                "parsed_event.experiment.material.composition"
            ),
        )
        .withColumn(
            "temperature_c",
            F.col(
                "parsed_event.experiment.process_conditions.temperature_c"
            ),
        )
        .withColumn(
            "pressure_mpa",
            F.col(
                "parsed_event.experiment.process_conditions.pressure_mpa"
            ),
        )
        .withColumn(
            "duration_minutes",
            F.col(
                "parsed_event.experiment.process_conditions.duration_minutes"
            ),
        )
        .withColumn(
            "tensile_strength_mpa",
            F.col(
                "parsed_event.experiment.measurements.tensile_strength_mpa"
            ),
        )
        .withColumn(
            "conductivity_s_per_m",
            F.col(
                "parsed_event.experiment.measurements.conductivity_s_per_m"
            ),
        )
        .withColumn(
            "density_g_per_cm3",
            F.col(
                "parsed_event.experiment.measurements.density_g_per_cm3"
            ),
        )
    )

    parsed_successfully = (
        F.col("parsed_event").isNotNull()
        & F.col("parsed_event._corrupt_record").isNull()
    )

    validation_errors = F.filter(
        F.array(
            F.when(
                ~parsed_successfully,
                F.lit("MALFORMED_JSON"),
            ),
            F.when(
                parsed_successfully
                & _is_blank("schema_version"),
                F.lit("MISSING_SCHEMA_VERSION"),
            ),
            F.when(
                parsed_successfully
                & F.col("schema_version").isNotNull()
                & (F.col("schema_version") != "1.0.0"),
                F.lit("UNSUPPORTED_SCHEMA_VERSION"),
            ),
            F.when(
                parsed_successfully
                & _is_blank("event_id"),
                F.lit("MISSING_EVENT_ID"),
            ),
            F.when(
                parsed_successfully
                & _is_blank("event_type"),
                F.lit("MISSING_EVENT_TYPE"),
            ),
            F.when(
                parsed_successfully
                & F.col("event_type").isNotNull()
                & (
                    F.col("event_type")
                    != "experiment.completed"
                ),
                F.lit("UNSUPPORTED_EVENT_TYPE"),
            ),
            F.when(
                parsed_successfully
                & (
                    _is_blank("parsed_event.occurred_at")
                    | F.col("occurred_at").isNull()
                ),
                F.lit("INVALID_OCCURRED_AT"),
            ),
            F.when(
                parsed_successfully
                & _is_blank("experiment_id"),
                F.lit("MISSING_EXPERIMENT_ID"),
            ),
            F.when(
                parsed_successfully
                & _is_blank("program_id"),
                F.lit("MISSING_PROGRAM_ID"),
            ),
            F.when(
                parsed_successfully
                & _is_blank("lab_id"),
                F.lit("MISSING_LAB_ID"),
            ),
            F.when(
                parsed_successfully
                & _is_blank("material_id"),
                F.lit("MISSING_MATERIAL_ID"),
            ),
            F.when(
                parsed_successfully
                & _is_blank("material_family"),
                F.lit("MISSING_MATERIAL_FAMILY"),
            ),
            F.when(
                parsed_successfully
                & (
                    F.col("composition").isNull()
                    | (F.size(F.col("composition")) == 0)
                ),
                F.lit("MISSING_COMPOSITION"),
            ),
            F.when(
                parsed_successfully
                & F.col("composition").isNotNull()
                & (
                    F.abs(
                        F.col(
                            "composition_mass_percent_total"
                        )
                        - F.lit(100.0)
                    )
                    > F.lit(0.01)
                ),
                F.lit("COMPOSITION_TOTAL_NOT_100"),
            ),
            F.when(
                parsed_successfully
                & (
                    F.col("temperature_c").isNull()
                    | (F.col("temperature_c") < -273.15)
                ),
                F.lit("INVALID_TEMPERATURE"),
            ),
            F.when(
                parsed_successfully
                & (
                    F.col("pressure_mpa").isNull()
                    | (F.col("pressure_mpa") < 0.0)
                ),
                F.lit("INVALID_PRESSURE"),
            ),
            F.when(
                parsed_successfully
                & (
                    F.col("duration_minutes").isNull()
                    | (F.col("duration_minutes") <= 0.0)
                ),
                F.lit("INVALID_DURATION"),
            ),
            F.when(
                parsed_successfully
                & (
                    F.col("tensile_strength_mpa").isNull()
                    | (
                        F.col("tensile_strength_mpa")
                        < 0.0
                    )
                ),
                F.lit("INVALID_TENSILE_STRENGTH"),
            ),
            F.when(
                parsed_successfully
                & (
                    F.col("conductivity_s_per_m").isNull()
                    | (
                        F.col("conductivity_s_per_m")
                        < 0.0
                    )
                ),
                F.lit("INVALID_CONDUCTIVITY"),
            ),
            F.when(
                parsed_successfully
                & (
                    F.col("density_g_per_cm3").isNull()
                    | (F.col("density_g_per_cm3") <= 0.0)
                ),
                F.lit("INVALID_DENSITY"),
            ),
        ),
        lambda error: error.isNotNull(),
    )

    return (
        silver_events
        .withColumn(
            "validation_errors",
            validation_errors,
        )
        .withColumn(
            "is_valid",
            F.size(F.col("validation_errors")) == 0,
        )
        .withColumn(
            "processed_at",
            F.current_timestamp(),
        )
        .select(
            "event_id",
            "schema_version",
            "event_type",
            "occurred_at",
            "source_system",
            "experiment_id",
            "program_id",
            "lab_id",
            "material_id",
            "material_family",
            "composition",
            "composition_mass_percent_total",
            "temperature_c",
            "pressure_mpa",
            "duration_minutes",
            "tensile_strength_mpa",
            "conductivity_s_per_m",
            "density_g_per_cm3",
            "validation_errors",
            "is_valid",
            "raw_payload",
            "payload_sha256",
            "source_file",
            "ingested_at",
            "processed_at",
        )
    )


def valid_silver_events(
    silver_events: DataFrame,
) -> DataFrame:
    return silver_events.filter(F.col("is_valid"))


def quarantined_silver_events(
    silver_events: DataFrame,
) -> DataFrame:
    return silver_events.filter(~F.col("is_valid"))
