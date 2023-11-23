# Databricks notebook source
import json
import unittest
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../etl_constants

# COMMAND ----------

class TestPostCommentsSilver(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = spark.read.format('delta').table(TARGET_POST_COMMENTS_JOIN_SILVER).persist()
        with open(f'{BASE_PATH}/posts_comments.json', 'r') as f:
            cls.json = json.load(f)
    
    @classmethod
    def tearDownClass(cls):
        cls.df.unpersist()

    def test_n_posts(self):
        self.assertEqual(len(self.json), self.df.select('post_comments_id_oid').distinct().toPandas().index.size)

    def test_sum_body_size(self):
        n_json = sum([len(j['body']) for j in self.json])
        n_df = self.df.select(F.sum(F.length(self.df['body']))).toPandas().iloc[0, 0]
        self.assertEqual(n_json, n_df)
        

if __name__ == '__main__':
    r = unittest.main(argv=[''], verbosity=0, exit=False)
    assert r.result.wasSuccessful()
