# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Widgets

# COMMAND ----------

from datetime import datetime, timedelta
from utils.databricks import Widgets
from machine_learning.linear_trend import LinearTrend

# COMMAND ----------

# MAGIC %run ./../etl/etl_constants

# COMMAND ----------

w = Widgets(dbutils)
w.create_text('start_date', str(datetime.now().date()-timedelta(days=30)))
w.create_text('end_date', str(datetime.now().date()+timedelta(days=1)))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Posts
# MAGIC
# MAGIC Plots:
# MAGIC - posts/comments: Shows the number of comments/posts per month. Each type (comments/poll/posts) comes with a linear regression (linear comments) that shows the overall trend. The linear trend could later be upgraded to a forecaster for other timeseries.
# MAGIC
# MAGIC Analysis:
# MAGIC - posts/comments
# MAGIC     1. Trend: decrease + small oscilation

# COMMAND ----------

df = spark.sql(f"""
-- posts
SELECT
    DATE_TRUNC('MONTH', created_at_date) AS `date`
    , type
    , COUNT(id_oid) AS n_posts
FROM {TARGET_POSTS_BRONZE_TB}
GROUP BY 1, 2

UNION ALL
-- post comments
SELECT
    DATE_TRUNC('MONTH', created_at_date) AS `date`
    , 'comments' AS `type`
    , COUNT(id_oid) AS n_comments
FROM {TARGET_POSTS_COMMENTS_BRONZE_TB}
GROUP BY 1, 2

ORDER BY `date`
""")

df = LinearTrend(spark).df_all_labels(df, 'date', 'n_posts', 'type', 'linear ')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # User engagement (change date at the top)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Time series with the number of posts comments
# MAGIC
# MAGIC Plots:
# MAGIC - n_comments: Shows the number of comments (per patron status).
# MAGIC
# MAGIC Analysis:
# MAGIC
# MAGIC - n_comments:
# MAGIC   1. Has the same trend as the number of posts (decrease + oscilation)

# COMMAND ----------

df = spark.sql(f"""
SELECT 
    DATE_TRUNC('month', created_at_date) AS `date`
    , CAST(COALESCE(patreon_status_is_patron, FALSE) AS STRING) AS is_patron
    , COUNT(post_comments_id_oid) AS n_comments
FROM {TARGET_POST_COMMENTS_JOIN_SILVER}
GROUP BY 1, 2
ORDER BY 1, n_comments
""")

# df = LinearTrend(spark).df_all_labels(df, 'date', 'n_comments', 'is_patron', 'linear ')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Number of comments
# MAGIC
# MAGIC Analysis with:
# MAGIC <code>start_date</code> = 2020-10-21
# MAGIC <code>end_date</code> = 2023-11-21
# MAGIC
# MAGIC Plots:
# MAGIC - n_per_country (top):
# MAGIC This is plot with the count of comments per country. The comments are aggregated by patron status.
# MAGIC - share_country (top):
# MAGIC Share of comments per country.
# MAGIC - share_patron (top):
# MAGIC Share of patron.
# MAGIC - share_country_patron (top):
# MAGIC Share of comments per country and patron status
# MAGIC - lifetime_support (bottom):
# MAGIC Share of total patreon contribution.
# MAGIC - share_users (bottom):
# MAGIC Share of users per country.
# MAGIC - share_country_patron (bottom).
# MAGIC Share of users per country and patron status.
# MAGIC
# MAGIC ## Analysis
# MAGIC
# MAGIC # Overview
# MAGIC
# MAGIC There are not a lot of comments per country and their distribution per patron status is very mixed.
# MAGIC Therefore, we may not have enough comments to draw good (statistical) decisions.
# MAGIC
# MAGIC # n_per_country
# MAGIC - US has the highest ammount of comments (US also has the greatest user base).
# MAGIC - Only the to 5 countries have over 100 comments.
# MAGIC - The patron status seems to not directly correlate with the number of comments since the highest number of comments per patron status swap across many countries.
# MAGIC
# MAGIC # share_country
# MAGIC
# MAGIC - The share of the number of comments seems to decay much slowly then share per monetary contributions.
# MAGIC This is a good sanity check, since comment is free and it is more likely that a user will write a comment
# MAGIC then to donate.
# MAGIC - The plot also reinforce that there are contries (like BR) that ranks very high on engagement (comments/votes),
# MAGIC but they fall back in monetary contribution.
# MAGIC
# MAGIC # share_country_patron
# MAGIC
# MAGIC - 43% of patrons are from US!
# MAGIC - TR seems to have a large user base (16.6% of non patrons), but very patrons (1.58%)
# MAGIC - The discrepancy of the share of users per patron status among countries shows that there is a lot of opportunities to increase income by creating lower price tiers.
# MAGIC
# MAGIC # bottom plots
# MAGIC - They also emphasizes the conclusions above.
# MAGIC
# MAGIC Overall, we can conclude that, while monetary contributions do correlate with higher engagement,
# MAGIC there are differences in culture that influences communication.
# MAGIC From the optimization standpoint, some countries, like BR, may be high maintenance and low profit due to the contrast between high comments and lower contribution.
# MAGIC
# MAGIC Furthermore, it would be interesting to understand how piracy and price localization influences these behaviors: is BR contribution lower due to bad USD pricing?
# MAGIC Can we engagement and contribution (and possibly reduce piracy) with better localized (or lower tier) prices?

# COMMAND ----------

df = spark.sql(f"""
SELECT 
    COALESCE(country, 'None') country
    , CAST(COALESCE(patreon_status_is_patron, FALSE) AS STRING) AS is_patron
    , COUNT(post_comments_id_oid) AS n_comments
FROM {TARGET_POST_COMMENTS_JOIN_SILVER}
WHERE created_at_date BETWEEN '{w('start_date')}' AND '{w('end_date')}'
GROUP BY 1, 2
ORDER BY n_comments DESC
""")

df_users = spark.sql(f"""
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
""")

# df = LinearTrend(spark).df_all_labels(df, 'date', 'n_comments', 'is_patron', 'linear ')
df.display()
df_users.display()
