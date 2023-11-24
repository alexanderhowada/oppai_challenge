# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Widgets

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from utils.databricks import Widgets
from machine_learning.linear_trend import LinearTrend
from machine_learning.openai import OpenAIRequests

# COMMAND ----------

# MAGIC %run ./../etl/etl_constants

# COMMAND ----------

w = Widgets(dbutils)
w.remove_all()

# COMMAND ----------

w.create_text('start_date', '2020-01-01')
w.create_text('end_date', str(datetime.now().date()+timedelta(days=1)))
w.create_text('n_countries', "10")
w.create_text('sort', 'lifetime_support')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Top countries.
# MAGIC
# MAGIC ## Widgets:
# MAGIC
# MAGIC 1. n_countries: the number of top countries to list (ex: 20 will list the top 20 countries).
# MAGIC 2. sort: column to sort the data of the top countries.
# MAGIC   - See table tab to list the available columns.
# MAGIC 3. start_date: the start date for comments and votes.
# MAGIC 4. end_date: the end date for comments and votes.

# COMMAND ----------

spark.sql(f"""
WITH users AS (
    SELECT
        COALESCE(country, 'None') AS country
        , SUM(CAST(patreon_status_is_patron AS BIGINT)) AS is_patron
        , SUM(patreon_status_campaign_lifetime_support_cents) AS lifetime_support
        , COUNT(1) AS n_users
        , lifetime_support/n_users AS lifetime_support_per_user
        , lifetime_support/is_patron AS lifetime_support_per_patron
    FROM {TARGET_USERS_BRONZE_TB}
    GROUP BY country
),
comments AS (
    SELECT
        COALESCE(country, 'None') AS country
        , count(post_comments_id_oid) AS n_comments
    FROM {TARGET_POST_COMMENTS_JOIN_SILVER}
    WHERE created_at_date BETWEEN "{w('start_date')}" AND "{w('end_date')}"
    GROUP BY country
),
votes AS (
    SELECT
        COALESCE(country, 'None') AS country
        , count(vote_id_oid) AS n_votes
    FROM {TARGET_POST_VOTES_JOIN_SILVER}
    WHERE created_at_date BETWEEN "{w('start_date')}" AND "{w('end_date')}"
    GROUP BY country
)
SELECT
    ROW_NUMBER() OVER(ORDER BY {w('sort')} DESC) AS rank
    , CASE WHEN rank <= {w('n_countries')} THEN users.country ELSE 'others' END AS country
    , users.* EXCEPT(country)
    , comments.* EXCEPT(country)
    , votes.* EXCEPT(country)
FROM users
JOIN comments ON users.country = comments.country
JOIN votes ON votes.country = users.country
ORDER BY rank ASC
""").display()

# COMMAND ----------

spark.sql(f"""
WITH users AS (
    SELECT
        COALESCE(country, 'None') AS country
        , SUM(CAST(patreon_status_is_patron AS BIGINT)) AS is_patron
        , SUM(patreon_status_campaign_lifetime_support_cents) AS lifetime_support
        , COUNT(1) AS n_users
        , lifetime_support/n_users AS lifetime_support_per_user
        , lifetime_support/is_patron AS lifetime_support_per_patron
    FROM {TARGET_USERS_BRONZE_TB}
    GROUP BY country
),
comments AS (
    SELECT
        COALESCE(country, 'None') AS country
        , count(post_comments_id_oid) AS n_comments
    FROM {TARGET_POST_COMMENTS_JOIN_SILVER}
    WHERE created_at_date BETWEEN "{w('start_date')}" AND "{w('end_date')}"
    GROUP BY country
),
votes AS (
    SELECT
        COALESCE(country, 'None') AS country
        , count(vote_id_oid) AS n_votes
        , SUM(vote_weight) AS w_votes
    FROM {TARGET_POST_VOTES_JOIN_SILVER}
    WHERE created_at_date BETWEEN "{w('start_date')}" AND "{w('end_date')}"
    GROUP BY country
)
SELECT
    ROW_NUMBER() OVER(ORDER BY {w('sort')} DESC) AS rank
    , CASE WHEN rank <= {w('n_countries')} THEN users.country ELSE 'others' END AS country
    , users.* EXCEPT(country)
    , comments.* EXCEPT(country)
    , votes.* EXCEPT(country)
FROM users
JOIN comments ON users.country = comments.country
JOIN votes ON votes.country = users.country
ORDER BY rank ASC
""").display()

# COMMAND ----------

spark.sql(f"""
WITH comments AS (
    SELECT
        COALESCE(country, 'None') AS country
        , count(post_comments_id_oid) AS n_comments
    FROM {TARGET_POST_COMMENTS_JOIN_SILVER}
    WHERE created_at_date BETWEEN "{w('start_date')}" AND "{w('end_date')}"
        AND COALESCE(country, 'None') IN {tuple(countries)}
    GROUP BY country
),
votes AS (
    SELECT
        COALESCE(country, 'None') AS country
        , count(vote_id_oid) AS n_votes
    FROM {TARGET_POST_VOTES_JOIN_SILVER}
    WHERE created_at_date BETWEEN "{w('start_date')}" AND "{w('end_date')}"
        AND COALESCE(country, 'None') IN {tuple(countries)}
    GROUP BY country
)
SELECT
    comments.country
    , n_comments
    , n_votes
FROM comments
JOIN votes ON comments.country = votes.country
""").display()

# COMMAND ----------

# top_countries_df = spark.sql(f"""
# WITH top AS (
#     SELECT
#         COALESCE(country, 'None') AS country
#         , SUM(CAST(patreon_status_is_patron AS BIGINT)) AS is_patron
#         , SUM(patreon_status_campaign_lifetime_support_cents) AS lifetime_support
#         , COUNT(1) AS n_users
#         , lifetime_support/n_users AS lifetime_support_per_user
#         , lifetime_support/is_patron AS lifetime_support_per_patron
#     FROM {TARGET_USERS_BRONZE_TB}
#     GROUP BY country
#     ORDER BY {w('sort')} DESC
#     LIMIT {w('n_countries')}
# ),
# others AS (
#     SELECT
#         'others' AS country
#         , SUM(CAST(patreon_status_is_patron AS BIGINT)) AS is_patron
#         , SUM(patreon_status_campaign_lifetime_support_cents) AS lifetime_support
#         , COUNT(1) AS n_users
#         , lifetime_support/n_users AS lifetime_support_per_user
#         , lifetime_support/is_patron AS lifetime_support_per_patron
#     FROM {TARGET_USERS_BRONZE_TB} source
#     WHERE COALESCE(country, 'None') NOT IN (SELECT COALESCE(t.country, 'None') FROM top t)
# )
# SELECT * FROM top
# UNION ALL
# SELECT * FROM others
# """)

# top_countries_df.display()
# countries = top_countries_df.select('country').toPandas()['country'].tolist()
# countries = [c for c in countries if c is not None]
