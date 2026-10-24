import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Une seule SparkSession pour tous les tests (démarrage lent)."""
    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.sql.shuffle.partitions", "1")   # petites données : 1 partition suffit
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
