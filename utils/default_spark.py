from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip


def get_default_session():
    builder = SparkSession.builder \
        .appName('test_delta_utils') \
        .config('spark.sql.warehouse.dir', 'pyspark_tables') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('spark.databricks.delta.retentionDurationCheck.enabled', False) \
        .config('spark.databricks.delta.schema.autoMerge.enabled', True) \
        .config('spark.databricks.delta.checkLatestSchemaOnRead', True) \
        .config("spark.log.level", "ERROR") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config('delta.enableChangeDataFeed', True)

    spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    return spark

spark = get_default_session()