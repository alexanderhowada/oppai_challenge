# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Widgets

# COMMAND ----------

!pip3 install -U kaleido

# COMMAND ----------

import ipywidgets
import plotly.graph_objects as go

from utils.databricks import Widgets

# COMMAND ----------

w = Widgets(dbutils)
w.remove_all()

# COMMAND ----------

w.create_text('per_country_order', 'n_users')
w.create_text('n_bins', '50')

# COMMAND ----------

# MAGIC %run ../etl/etl_constants

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Totals
# MAGIC
# MAGIC List users totals
# MAGIC
# MAGIC 1. 55k total users
# MAGIC 1. 5060 patrons
# MAGIC 1. 9.1% of the users are patrons
# MAGIC 1. almost 600k raised for the campaign
# MAGIC 1. 300k raised
# MAGIC     - I am not sure about these results, since there is more money raised in the campaign than in the lifetime.
# MAGIC 1. Only 19 followers (strange)
# MAGIC 1. no free trial users

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
# MAGIC # Support distribution per user
# MAGIC
# MAGIC ## Widgets:
# MAGIC
# MAGIC 1. per_country_order: the column to sort. See availables in the table tab.
# MAGIC
# MAGIC ## Plots
# MAGIC
# MAGIC These plots aims to analyze the distribution os monetary contribution per user and patron.
# MAGIC The plots are made using Plotly due to limitations of the plots in Databricks notebooks.
# MAGIC
# MAGIC - share of user supporters: share of users that have monetary contribution > 0.
# MAGIC - Share of campaign support: Share of campaign support contribution per user patron.
# MAGIC - campaign share: distribution of campaign lifetime support (excludes non contributors).
# MAGIC - top supporters: table with the top supporters
# MAGIC
# MAGIC ## Analysis
# MAGIC
# MAGIC ### share of user supporters
# MAGIC
# MAGIC 1. Almost 70% of the userbase have contributed in the past.
# MAGIC
# MAGIC ### share of campaign support
# MAGIC
# MAGIC 1. Almost 60% of the total monetary contribution are from patrons.
# MAGIC 2. However, a lot (40%) is from non patrons.
# MAGIC
# MAGIC ### Support distribution (excludes non contributors)
# MAGIC
# MAGIC 1. About 11k users have donated less than 20 USD.
# MAGIC 1. The outliers have donated less than 1k USD, therefore they are not extreme outliers and cannot be considered "angel investors".
# MAGIC 1. The linear decline (on a log scale), seems to indicate that the probabilty of monetary donation decays exponentially fast.
# MAGIC     > This may indicate that extreme (rare events) statistcs may be ideal to describe users monetary contribution.
# MAGIC
# MAGIC     > This kind of statiscs should consider that the tail of the probability distribution is equally important as its bulk if we are considering monetary contribution.
# MAGIC
# MAGIC ### Top 10 supporters
# MAGIC
# MAGIC 1. None of the supporters have outstanding donations.
# MAGIC 1. All of them are from developed countries and speak english.

# COMMAND ----------

def share_contributors(df):
    fig = go.Figure()
    df['supporter'] = df['campaign_support'].apply(lambda x: True if x > 0 else False)
    fig.add_trace(
        go.Pie(labels=df['supporter'])
    )
    fig.update_layout(
        title="Share of user supporters",
        legend={"title": "User has monetary contribution"},
    )
    fig.show()

def monetary_support(df, exclude_0=True):
    if exclude_0:
        df = df[df['campaign_support'] > 0]
    fig = go.Figure()
    fig.add_trace(
        go.Histogram(x=df['campaign_support'], nbinsx=int(w('n_bins')), name='campaign_support')
    )
    fig.add_trace(
        go.Histogram(x=df['lifetime_support'], nbinsx=int(w('n_bins')), name='lifetime_support')
    )
    fig.update_layout(title=f"support distribution (exclude 0 support)")
    fig.update_yaxes(type='log', title="Freq")
    fig.update_xaxes(type='linear', title="USD")
    
    fig.show()

def share_of_patron_support(df):
    fig = go.Figure()
    df = df.groupby('is_patron').sum(['campaign_support', 'lifetime_support']).reset_index()
    fig.add_trace(
        go.Pie(labels=df['is_patron'], values=df['campaign_support'])
    )
    fig.update_layout(
        title="Share of campaign_support",
        legend={"title": "is_patron"}
    )
    fig.show()

df = spark.sql(f"""
SELECT
    COALESCE(patreon_status_campaign_lifetime_support_cents, 0)/100 AS campaign_support
    , COALESCE(patreon_status_lifetime_support_cents, 0)/100 as lifetime_support
    , COALESCE(patreon_status_is_patron, False) AS is_patron
FROM {TARGET_USERS_BRONZE_TB}
""").toPandas()

share_contributors(df)
share_of_patron_support(df)
monetary_support(df)

print("top 10 supporters")
df = spark.sql(f"""
SELECT
    id_oid
    , country
    , language
    , COALESCE(patreon_status_campaign_lifetime_support_cents, 0)/100 AS campaign_support
    , COALESCE(patreon_status_lifetime_support_cents, 0)/100 as lifetime_support
FROM {TARGET_USERS_BRONZE_TB}
ORDER BY patreon_status_campaign_lifetime_support_cents DESC
LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Per country distribution
# MAGIC
# MAGIC ## Plots
# MAGIC
# MAGIC - n_users: distribution of users per country
# MAGIC - user_share: share of users per country
# MAGIC - lifetime_support: contribution distribution
# MAGIC - share_lifetime_support: share of support per country
# MAGIC
# MAGIC ## n_users
# MAGIC
# MAGIC - If we set the widget per_country_order to n_users,
# MAGIC   the distribution shows that there are two classes of countries:
# MAGIC   > The first few (up to FR), have a unnusual high number of users if compared with the remaining.
# MAGIC
# MAGIC   > The other countries seems to follow an exponential distribution (linear decay on a log scale).
# MAGIC
# MAGIC - The number of patrons is not proportiornal to the number of users.
# MAGIC
# MAGIC ## user_share
# MAGIC
# MAGIC - The user share plot seems to illustrate very well that the top countries have the largest share (abount 70% up to FR)
# MAGIC
# MAGIC ## lifetime_support
# MAGIC
# MAGIC - sorting by n_users shows that number of users does not directly correlate with monetary contribution.
# MAGIC - sorting by lifetime_support we can see that the ranking of countries changes, further emphasizing that number of users does not directly correlate with money.
# MAGIC - It is interesting to note that a few countries (BR, for example) have small monetary contribution (0.94%) despite having a large (4.19%) user base.
# MAGIC - It is also interesting to notice that the distribution of monetary distribution decays much faster then the user base. This may be an opportunity to localize Patreon contributions in order to reach a larger user base.

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
ORDER BY {w('per_country_order')} DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Per country and patron

# COMMAND ----------

spark.sql(f"""
SELECT
    COALESCE(country, 'None') AS country
    , COALESCE(patreon_status_is_patron, FALSE) as is_patron
    , SUM(patreon_status_campaign_lifetime_support_cents)/100 AS campaign_support
FROM {TARGET_USERS_BRONZE_TB}
GROUP BY ALL
ORDER BY campaign_support DESC
""").display()

# COMMAND ----------


