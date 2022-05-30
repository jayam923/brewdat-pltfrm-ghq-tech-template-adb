from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip

builder = (
        SparkSession.builder
        .appName("Spark job")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )


def get_spark(builder):
        return configure_spark_with_delta_pip(builder).getOrCreate()


spark = get_spark(builder)
