# Databricks notebook source
import json
import unittest
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../etl_constants

# COMMAND ----------

class TestVotesSilver(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = spark.read.format('delta').table(TARGET_POST_VOTES_JOIN_SILVER).persist()
        with open(f'{BASE_PATH}/votes.json', 'r') as f:
            cls.json = json.load(f)
    
    @classmethod
    def tearDownClass(cls):
        cls.df.unpersist()

    def test_count_votes(self):
        n_votes = sum([len(j['votes']) for j in self.json])
        silver_n_votes = self.df.count()
        self.assertEqual(n_votes, silver_n_votes)

    def test_sum_weight(self):
        w_json = sum([sum([j['weight'] for j in jj['votes']]) for jj in self.json if len(jj['votes']) != 0])
        w_silver = self.df.select(F.sum(self.df['vote_weight'])).toPandas().iloc[0, 0]
        self.assertEqual(w_json, w_silver)

if __name__ == '__main__':
    r = unittest.main(argv=[''], verbosity=0, exit=False)
    assert r.result.wasSuccessful()

# COMMAND ----------


