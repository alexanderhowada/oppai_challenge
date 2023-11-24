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
# MAGIC 1. n_countries: the number of top countries to list (ex: 10 will list the top 10 countries). Set to 10 for this analysis.
# MAGIC 2. sort: column to sort the data of the top countries.
# MAGIC     - See table tab to list the available columns.
# MAGIC 3. start_date: the start date for comments and votes. Set to 2020-01-01 for this analysis.
# MAGIC 4. end_date: the end date for comments and votes. Set to today for this analysis.
# MAGIC
# MAGIC ## Plots
# MAGIC
# MAGIC These plots aims to analyze the data of to top countries by the sorted column.
# MAGIC The goal is to have an overview of monetary support and engagement (comments and votes).
# MAGIC
# MAGIC All plots have the number of users and number of patrons on the right axis.
# MAGIC The title of the tabs describe the content of the left axis.
# MAGIC The plots are:
# MAGIC - lifetime_support
# MAGIC - lifetime_support_per_user
# MAGIC - comments
# MAGIC - votes
# MAGIC - w_votes
# MAGIC
# MAGIC ## Analysis
# MAGIC
# MAGIC ### sort=lifetime_support
# MAGIC
# MAGIC we can understand what are the countries with the highest contribution.
# MAGIC
# MAGIC From the lifetime_support tab:
# MAGIC
# MAGIC 1. The highest number of users are in others, followed by None, than US.
# MAGIC 1. The highest contribution comes from users without a registered country.
# MAGIC 1. The second highest contribution comes from US.
# MAGIC 1. US has higher contribution that all other countries that are not in the top 10.
# MAGIC 1. Note that the 5th country, TW, has more users than the 3rd and 4th, DE and GB, despite its lower monetary contribution.
# MAGIC
# MAGIC From the lifetime_support_per_user tab:
# MAGIC
# MAGIC 1. US has the highest average contribution (at around 20 USD).
# MAGIC 1. Most of the top countries, sorted by lifetime_support, with the highest average contribution per user
# MAGIC are developed countries with strong currency.
# MAGIC 1. We can observe a major disparity in TW regarding the average contribution per user.
# MAGIC
# MAGIC From the comments tab:
# MAGIC
# MAGIC 1. The others category has the highest number of commentaries.
# MAGIC 1. Here we can see that the number of comments does not correlate with monetary contribution or number of users.
# MAGIC     - The number of commentaries may related to other variable such as local culture
# MAGIC
# MAGIC From the votes tab:
# MAGIC
# MAGIC 1. Here we see the same behavior as in the comments tab:
# MAGIC     - The number of votes does not relate with monetary contribution.
# MAGIC     - The number of votes seems to be strongly correlated with the number of users, except for None and UNK.
# MAGIC 1. Since there is no relation between monetary contribution and number of votes,
# MAGIC we can understand that (unweighted) votes can be used as a mesure of overall opinion of the Patreon community.
# MAGIC
# MAGIC From the w_votes tab:
# MAGIC
# MAGIC 1. It is interesting to realize that the weighted vote also do not correlate with monetary contribution.
# MAGIC     - While US still has the highest ammount of weighted votes, the ranking seems a little random as we follow the list of highest contribution per country.
# MAGIC     - Votes may be not only related to contribution (users that pay more vote more), but there may be other variables, such as local culture, that influences the number of votes.
# MAGIC
# MAGIC ### sort=n_users
# MAGIC
# MAGIC From the lifetime_support tab:
# MAGIC 1. It is interesting that, while most of the countries with the highest number of users are also the countries with highest contribution,
# MAGIC there are a few newcomers such as:
# MAGIC     - BR
# MAGIC     - TH
# MAGIC     - TR
# MAGIC 1. In my opinion, these newcomers are countries with weaker currency.
# MAGIC 1. Weaker currency and lower buy power may correlate with higher piracy
# MAGIC     - It would be interesting to know the number of users that actually have the game
# MAGIC         - Cross with steam data?
# MAGIC         - Integrate the game with Patreon directly?
# MAGIC     - Piracy may be reduced with localized prices.
# MAGIC     - Stream content instead of shipping it with the game?
# MAGIC         - May be regarded as "bad practice" and decrease the number of users in a country.
# MAGIC
# MAGIC From the comments tab:
# MAGIC 1. Looks like that the number of comments increase with the number of users, however it is not the only determinant factor.
# MAGIC     - The number of comments is in decreasing order, hence there are other varibles that influences it.
# MAGIC
# MAGIC From the votes tab:
# MAGIC 1. The data seems very ordered, therefore the number of comments do correlated with the number of votes!
# MAGIC
# MAGIC From the w_votes tab:
# MAGIC 1. As expected, the number of users is not related to the weighted votes.
# MAGIC
# MAGIC ### sort=n_comments
# MAGIC
# MAGIC Sorting by n_comments further emphasizes the previous conclusions.

# COMMAND ----------

spark.sql(f"""
WITH users AS (
    SELECT
        COALESCE(country, 'None') AS country
        , SUM(CAST(patreon_status_is_patron AS BIGINT)) AS is_patron
        , ROUND(SUM(patreon_status_campaign_lifetime_support_cents)/100, 0) AS lifetime_support
        , COUNT(1) AS n_users
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
),
join_others AS (
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
)
SELECT
    country
    , SUM(is_patron) AS is_patron
    , ROUND(SUM(lifetime_support), 0) AS lifetime_support
    , SUM(n_users) AS n_users
    , SUM(lifetime_support)/SUM(n_users) AS lifetime_support_per_user
    , SUM(n_comments) AS n_comments
    , SUM(n_votes) AS n_votes
    , SUM(w_votes) AS w_votes
FROM join_others
GROUP BY ALL
""").display()
