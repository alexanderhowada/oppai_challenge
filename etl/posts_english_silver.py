# Databricks notebook source
# MAGIC %run ./etl_constants

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {TARGET_POSTS_ENGLISH_SILVER} AS
SELECT 
    p.*
    , t.translations_title
    , t.translations_description
    , index
    , option
FROM {TARGET_POSTS_BRONZE_TB} p
JOIN {TARGET_POSTS_TRANSLATIONS_BRONZE_TB} t
    ON t.id_oid = p.id_oid
LATERAL VIEW POSEXPLODE(t.translations_options) options AS index, option
WHERE t.translations_language = 'en'
""").display()

# COMMAND ----------

spark.sql(f"SELECT * FROM {TARGET_POSTS_ENGLISH_SILVER} WHERE id_oid = '654ac3aac292fbc27462505b'").collect()
