import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("Spark job")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", f"{os.getcwd()}/test/libs/spark-xml_2.12-0.15.0.jar")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
