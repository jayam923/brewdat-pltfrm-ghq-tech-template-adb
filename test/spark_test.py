import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

jars = ["spark-xml_2.12-0.15.0.jar", "spark-avro_2.12-3.3.0.jar"]
jars = map(lambda x: f"{os.getcwd()}/test/libs/{x}", jars)

builder = (
    SparkSession.builder
    .appName("Spark job")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", ",".join(jars))
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
