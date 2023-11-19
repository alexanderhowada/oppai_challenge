# Databricks notebook source
from pyspark.sql import functions as F
from utils.logs import print_args
from utils.data_quality import assert_no_null, assert_pk
from utils.spark_delta_transform import unnest_struct, transform_column_names
from utils.spark_delta import merge, table_exists, optimize_tb

# COMMAND ----------

class PostsCommentsRawETL:
    def __init__(self, spark, dt_start, dt_end, pk=None):
        self.spark = spark
        self.dt_start = dt_start
        self.dt_end = dt_end
        self.pk = self._get_default_pk(pk)
        self.default_date = '1901-01-01'
        
    @staticmethod
    def _get_default_pk(pk):
        if pk is None:
            return ['id_oid']
        return pk
        
    @print_args(print_kwargs=['file_name'])
    def extract(self, file_name: str):
        return spark.read.format('json').options(multiLine=True).load(file_name)
    
    def transform(self, df):
        df = unnest_struct(df)
        df = transform_column_names(df)

        for c in df.columns:
            if '_date' in c:
                df = df.withColumn(c, F.to_timestamp(df[c]))
        
        return df
    
    def assert_quality(self, df):
        assert_no_null(df, self.pk+['created_at_date', 'updated_at_date'])
        assert_pk(df, self.pk)

    @print_args(print_kwargs=['target_tb', 'drop'])
    def load(self, df, target_tb, drop=False):
        if drop and table_exists(target_tb, self.spark):
            print(f"Dropping table {target_tb}")
            self.spark.sql(f"DROP TABLE {target_tb}").show()

        print(f"df {df.count()} rows.")
        df.write.format('delta').mode('append').saveAsTable(target_tb)

# COMMAND ----------

# MAGIC %run ./etl_constants

# COMMAND ----------

etl = PostsCommentsRawETL(spark, DT_START, DT_END)
df = etl.extract(file_name=f"{BASE_PATH[5:]}/posts_comments.json")
df.persist()

df = etl.transform(df)
etl.assert_quality(df)

etl.load(df, target_tb=TARGET_POSTS_COMMENTS_RAW_TB, drop=False)
df.unpersist()

# COMMAND ----------

# etl = PostsCommentsRawETL(spark, DT_START, DT_END)
# optimize_tb(spark, TARGET_POSTS_COMMENTS_RAW_TB, ['updated_at_date']+etl.pk, replace=True)
