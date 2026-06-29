from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


composition_schema = ArrayType(
    StructType(
        [
            StructField("component", StringType(), nullable=False),
            StructField("mass_percent", DoubleType(), nullable=False),
        ]
    ),
    containsNull=False,
)


experiment_event_schema = StructType(
    [
        StructField("schema_version", StringType(), nullable=False),
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("occurred_at", StringType(), nullable=False),
        StructField("source_system", StringType(), nullable=False),
        StructField(
            "experiment",
            StructType(
                [
                    StructField("experiment_id", StringType(), nullable=False),
                    StructField("program_id", StringType(), nullable=False),
                    StructField("lab_id", StringType(), nullable=False),
                    StructField(
                        "material",
                        StructType(
                            [
                                StructField(
                                    "material_id",
                                    StringType(),
                                    nullable=False,
                                ),
                                StructField(
                                    "family",
                                    StringType(),
                                    nullable=False,
                                ),
                                StructField(
                                    "composition",
                                    composition_schema,
                                    nullable=False,
                                ),
                            ]
                        ),
                        nullable=False,
                    ),
                    StructField(
                        "process_conditions",
                        StructType(
                            [
                                StructField(
                                    "temperature_c",
                                    DoubleType(),
                                    nullable=False,
                                ),
                                StructField(
                                    "pressure_mpa",
                                    DoubleType(),
                                    nullable=False,
                                ),
                                StructField(
                                    "duration_minutes",
                                    DoubleType(),
                                    nullable=False,
                                ),
                            ]
                        ),
                        nullable=False,
                    ),
                    StructField(
                        "measurements",
                        StructType(
                            [
                                StructField(
                                    "tensile_strength_mpa",
                                    DoubleType(),
                                    nullable=False,
                                ),
                                StructField(
                                    "conductivity_s_per_m",
                                    DoubleType(),
                                    nullable=False,
                                ),
                                StructField(
                                    "density_g_per_cm3",
                                    DoubleType(),
                                    nullable=False,
                                ),
                            ]
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=False,
        ),
    ]
)
