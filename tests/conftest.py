from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("materials-research-lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    session.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    yield session

    session.stop()
