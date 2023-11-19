# Databricks notebook source
from pyspark.sql import functions as F
from utils.logs import print_args
from utils.spark_delta import merge, table_exists, optimize_tb
from utils.data_quality import assert_no_null, assert_pk
from utils.spark_delta_transform import unnest_struct, transform_column_names

# COMMAND ----------

class VotesRawETL:
    def __init__(self, spark, dt_start, dt_end, pk=None):
        self.spark = spark
        self.dt_start = dt_start  # dummy varibles to be used later in real ETL
        self.dt_end = dt_end  # dummy varibles to be used later in real ETL
        self.pk = self._set_pk(pk)
        
    @staticmethod
    def _set_pk(pk):
        if pk is None:
            pk = ["id_oid"]
        return pk
            
    @print_args(print_kwargs=['file_name'])
    def extract(self, file_name: str):
        df = self.spark.read.format('json').options(multiLine=True).load(file_name)
        return df
    
    def transform(self, df):
        df = unnest_struct(df)
        df = transform_column_names(df)
        df = self._transform_to_timestamp(df)   
        return df
    
    @staticmethod
    def _transform_to_timestamp(df):
        for c in df.columns:
            if '_date' in c:
                df = df.withColumn(c, F.to_timestamp(df[c]))
        return df
    
    def assert_quality(self, df):
        assert_no_null(df, self.pk+['updated_at_date'])
        assert_pk(df, self.pk)
    
    @print_args(print_kwargs=['target_tb'])
    def load(self, df, target_tb):
        print(f"{df.count()} rows.")
        merge(df, target_tb, self.pk, spark_session=self.spark)

# COMMAND ----------

# MAGIC %run ./etl_constants

# COMMAND ----------

etl = VotesRawETL(spark, DT_START, DT_END)
df = etl.extract(file_name=f"{BASE_PATH[5:]}/votes.json")
df.persist()

df = etl.transform(df)
etl.assert_quality(df)

etl.load(df, target_tb=TARGET_VOTES_RAW_TB)
df.unpersist()

# COMMAND ----------

# etl = VotesRawETL(spark, DT_START, DT_END)
# optimize_tb(spark, TARGET_VOTES_RAW_TB, ['updated_at_date']+etl.pk, replace=True)
