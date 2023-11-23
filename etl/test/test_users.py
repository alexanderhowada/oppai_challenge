# Databricks notebook source
import json
import unittest
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../etl_constants

# COMMAND ----------

class TestUsersSilver(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = spark.read.format('delta').table(TARGET_USERS_BRONZE_TB).persist()
        with open(f'{BASE_PATH}/users.json', 'r') as f:
            cls.json = json.load(f)
    
    @classmethod
    def tearDownClass(cls):
        cls.df.unpersist()

    def test_n_posts(self):
        self.assertEqual(len(self.json), self.df.select('id_oid').distinct().toPandas().index.size)

    def test_sum_body_size(self):
        n_json = sum(
            [
                j['patreon_status']['lifetime_support_cents'] for j in self.json
                if 'patreon_status' in j.keys()
                    and 'lifetime_support_cents' in j['patreon_status'].keys()
            ]
        )
        n_df = self.df.select(F.sum(self.df['patreon_status_lifetime_support_cents'])).toPandas().iloc[0, 0]
        self.assertEqual(n_json, n_df)
        

if __name__ == '__main__':
    r = unittest.main(argv=[''], verbosity=0, exit=False)
    assert r.result.wasSuccessful()
