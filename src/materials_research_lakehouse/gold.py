from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def _normalize_higher_is_better(
    value: Column,
    minimum: Column,
    maximum: Column,
) -> Column:
    return F.when(
        maximum == minimum,
        F.lit(1.0),
    ).otherwise(
        (value - minimum) / (maximum - minimum)
    )


def _normalize_lower_is_better(
    value: Column,
    minimum: Column,
    maximum: Column,
) -> Column:
    return F.when(
        maximum == minimum,
        F.lit(1.0),
    ).otherwise(
        (maximum - value) / (maximum - minimum)
    )


def build_material_candidate_scores(
    valid_events: DataFrame,
    strength_weight: float = 0.45,
    conductivity_weight: float = 0.35,
    density_weight: float = 0.20,
) -> DataFrame:
    total_weight = (
        strength_weight
        + conductivity_weight
        + density_weight
    )

    if abs(total_weight - 1.0) > 0.000001:
        raise ValueError("Scoring weights must add up to 1.0.")

    material_summary = (
        valid_events
        .dropDuplicates(["event_id"])
        .groupBy(
            "program_id",
            "material_id",
            "material_family",
        )
        .agg(
            F.countDistinct("experiment_id").alias(
                "experiment_count"
            ),
            F.avg("tensile_strength_mpa").alias(
                "average_tensile_strength_mpa"
            ),
            F.avg("conductivity_s_per_m").alias(
                "average_conductivity_s_per_m"
            ),
            F.avg("density_g_per_cm3").alias(
                "average_density_g_per_cm3"
            ),
            F.max("occurred_at").alias(
                "latest_experiment_at"
            ),
        )
    )

    program_window = Window.partitionBy("program_id")

    scored_candidates = (
        material_summary
        .withColumn(
            "minimum_strength",
            F.min("average_tensile_strength_mpa").over(
                program_window
            ),
        )
        .withColumn(
            "maximum_strength",
            F.max("average_tensile_strength_mpa").over(
                program_window
            ),
        )
        .withColumn(
            "minimum_conductivity",
            F.min("average_conductivity_s_per_m").over(
                program_window
            ),
        )
        .withColumn(
            "maximum_conductivity",
            F.max("average_conductivity_s_per_m").over(
                program_window
            ),
        )
        .withColumn(
            "minimum_density",
            F.min("average_density_g_per_cm3").over(
                program_window
            ),
        )
        .withColumn(
            "maximum_density",
            F.max("average_density_g_per_cm3").over(
                program_window
            ),
        )
        .withColumn(
            "strength_score",
            _normalize_higher_is_better(
                F.col("average_tensile_strength_mpa"),
                F.col("minimum_strength"),
                F.col("maximum_strength"),
            ),
        )
        .withColumn(
            "conductivity_score",
            _normalize_higher_is_better(
                F.col("average_conductivity_s_per_m"),
                F.col("minimum_conductivity"),
                F.col("maximum_conductivity"),
            ),
        )
        .withColumn(
            "density_score",
            _normalize_lower_is_better(
                F.col("average_density_g_per_cm3"),
                F.col("minimum_density"),
                F.col("maximum_density"),
            ),
        )
        .withColumn(
            "candidate_score",
            F.round(
                F.col("strength_score") * strength_weight
                + F.col("conductivity_score")
                * conductivity_weight
                + F.col("density_score") * density_weight,
                4,
            ),
        )
    )

    ranking_window = (
        Window
        .partitionBy("program_id")
        .orderBy(
            F.col("candidate_score").desc(),
            F.col(
                "average_tensile_strength_mpa"
            ).desc(),
            F.col(
                "average_conductivity_s_per_m"
            ).desc(),
            F.col("average_density_g_per_cm3").asc(),
            F.col("material_id").asc(),
        )
    )

    return (
        scored_candidates
        .withColumn(
            "candidate_rank",
            F.row_number().over(ranking_window),
        )
        .select(
            "program_id",
            "material_id",
            "material_family",
            "experiment_count",
            "average_tensile_strength_mpa",
            "average_conductivity_s_per_m",
            "average_density_g_per_cm3",
            "strength_score",
            "conductivity_score",
            "density_score",
            "candidate_score",
            "candidate_rank",
            "latest_experiment_at",
        )
    )
