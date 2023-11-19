# Databricks notebook source
from pyspark.sql import functions as F
from utils.logs import print_args
from utils.spark_delta import merge, table_exists, optimize_tb
from utils.data_quality import assert_no_null, assert_pk
from utils.spark_delta_transform import unnest_struct, transform_column_names

# COMMAND ----------

class VotesBronzeETL:
    def __init__(self, spark, dt_start, dt_end, pk=None):
        self.spark = spark
        self.dt_start = dt_start  # dummy varibles to be used later in real ETL
        self.dt_end = dt_end  # dummy varibles to be used later in real ETL
        self.pk = self._set_pk(pk)
        
    @staticmethod
    def _set_pk(pk):
        if pk is None:
            pk = ["id_oid", "vote_index"]
        return pk
            
    @print_args(print_kwargs=['source_tb'])
    def extract(self, source_tb: str):
        df = self.spark.read.format('delta').table(source_tb)
        df = df.filter(f"updated_at_date BETWEEN '{self.dt_start}' AND '{self.dt_end}'").distinct()
        return df
    
    def transform(self, df):
        df = self._transform_to_timestamp(df)
        df = df.withColumn('votes', F.explode('votes')) \
            .withColumn('vote_index', F.col('votes.index')) \
            .withColumn('vote_weight', F.col('votes.weight')) \
            .drop('votes')
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

    @print_args(print_kwargs=['target_tb', 'drop'])
    def load(self, df, target_tb, drop=False):
        if drop and table_exists(target_tb, self.spark):
            print(f"Dropping table {target_tb}")
            self.spark.sql(f"DROP TABLE {target_tb}").display()

        print(f"{df.count()} rows.")
        merge(df, target_tb, self.pk, spark_session=self.spark)

# COMMAND ----------

# MAGIC %run ./etl_constants

# COMMAND ----------

etl = VotesBronzeETL(spark, DT_START, DT_END)
df = etl.extract(source_tb=TARGET_VOTES_RAW_TB)
df.persist()

df = etl.transform(df)
etl.assert_quality(df)

etl.load(df, target_tb=TARGET_VOTES_BRONZE_TB, drop=False)
df.unpersist()

# COMMAND ----------

# etl = VotesBronzeETL(spark, DT_START, DT_END)
# optimize_tb(spark, TARGET_VOTES_BRONZE_TB, ['updated_at_date']+etl.pk, replace=True)
