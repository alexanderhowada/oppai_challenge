# Databricks notebook source
# MAGIC %run ../etl/etl_constants

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Totals

# COMMAND ----------

spark.sql(f"""
SELECT
    COUNT(DISTINCT id_oid) AS `total users`
    , SUM(CAST(patreon_status_is_patron AS BIGINT)) AS is_patron
    , 100*ROUND(is_patron/`total users`, 3) AS is_patreon_percent
    , SUM(patreon_status_campaign_lifetime_support_cents)/100 AS campaign_support
    , SUM(patreon_status_lifetime_support_cents)/100 as lifetime_support
    , SUM(CAST(patreon_status_is_follower AS BIGINT)) AS is_followwer
    , SUM(CAST(patreon_status_is_free_trial AS BIGINT)) AS is_free_trial
FROM {TARGET_USERS_BRONZE_TB}""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Per country

# COMMAND ----------

# DBTITLE 0,Per country
spark.sql(f"""
SELECT
    COALESCE(country, 'null') AS country
    , SUM(CAST(COALESCE(patreon_status_is_patron, FALSE) AS BIGINT)) AS is_patron
    , COUNT(DISTINCT id_oid) AS n_users
    , SUM(patreon_status_campaign_lifetime_support_cents)/100 AS lifetime_support
    , SUM(CAST(patreon_status_is_follower AS BIGINT)) AS is_followwer
    , SUM(CAST(patreon_status_is_free_trial AS BIGINT)) AS is_free_trial
FROM {TARGET_USERS_BRONZE_TB}
GROUP BY 1
ORDER BY 3 DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Distribution

# COMMAND ----------

spark.sql(f"""
SELECT
    patreon_status_campaign_lifetime_support_cents/100 AS campaign_support
    , patreon_status_lifetime_support_cents/100 as lifetime_support
    , COALESCE(patreon_status_is_patron, False) AS is_patron
FROM {TARGET_USERS_BRONZE_TB}
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### per country and patron

# COMMAND ----------

spark.sql(f"""
SELECT
    country
    , COALESCE(patreon_status_is_patron, FALSE) as is_patron
    , SUM(patreon_status_campaign_lifetime_support_cents)/100 AS campaign_support
FROM {TARGET_USERS_BRONZE_TB}
GROUP BY 1, 2
ORDER BY campaign_support, is_patron, country DESC
""").display()
