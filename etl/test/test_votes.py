# Databricks notebook source
import json
import unittest
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../etl_constants

# COMMAND ----------

class TestPostsSilver(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = spark.read.format('delta').table(TARGET_POSTS_ENGLISH_SILVER).persist()
        with open(f'{BASE_PATH}/posts.json', 'r') as f:
            cls.json = json.load(f)
    
    @classmethod
    def tearDownClass(cls):
        cls.df.unpersist()

    def test_n_posts(self):
        self.assertEqual(len(self.json), self.df.select('id_oid').distinct().toPandas().index.size)

    def test_sum_multiple_votes(self):
        print(self.json[0].keys())
        s_json = sum([j['multiple_votes'] for j in self.json if 'multiple_votes' in j.keys()])
        s_df = self.df.select(
            'id_oid', 'multiple_votes'
        ).distinct().select(
            F.sum(self.df['multiple_votes'].cast('int'))
        ).toPandas().iloc[0, 0]
        
        self.assertEqual(s_json, s_df)

if __name__ == '__main__':
    r = unittest.main(argv=[''], verbosity=0, exit=False)
    assert r.result.wasSuccessful()
