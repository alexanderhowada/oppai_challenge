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
LEFT JOIN {TARGET_POSTS_TRANSLATIONS_BRONZE_TB} t
    ON t.id_oid = p.id_oid
LATERAL VIEW POSEXPLODE(
    CASE WHEN SIZE(t.translations_options) = 0
        THEN ARRAY("")
        ELSE t.translations_options
    END) options AS index, option
WHERE t.translations_language = 'en'
""").display()
