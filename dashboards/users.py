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
# MAGIC ## Totals
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
# MAGIC ## Support distribution per user
# MAGIC
# MAGIC ## Widgets:
# MAGIC
# MAGIC 1. per_country_order: the column to sort. See availables in the table tab.
# MAGIC
# MAGIC ## Plots
# MAGIC
# MAGIC These plots aims to analyze the distribution os monetary contribution per user and patron.
# MAGIC
# MAGIC - campaign_support: distribution of campaign lifetime support.
# MAGIC - lifetime_support: distribution of lifetime support.
# MAGIC - is_patron: share os patrons
# MAGIC - patron support: share os patron suport vs non patrons.
# MAGIC
# MAGIC ## Analysis
# MAGIC
# MAGIC From the campaign and lifetime support tabs:
# MAGIC
# MAGIC 1. Almost 1800k of the users have contributed with 0-12 USD.
# MAGIC 1. Contributions higher than 12 USD seems to decay exponentially fast.
# MAGIC
# MAGIC

# COMMAND ----------

help(go.Histogram)

# COMMAND ----------

def update_histogram(df):
    fig = go.Figure()
    fig.add_trace(
        go.Histogram(x=df['campaign_support'], nbinsx=int(w('n_bins')), name='campaign_support')
    )
    fig.add_trace(
        go.Histogram(x=df['lifetime_support'], nbinsx=int(w('n_bins')), name='lifetime_support')
    )
    fig.update_layout(title=f"Freq")
    fig.update_yaxes(type='log', title="Freq")
    fig.update_xaxes(type='linear', title="USD")
    
    fig.show()

def share_per_user(df):
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
    patreon_status_campaign_lifetime_support_cents/100 AS campaign_support
    , patreon_status_lifetime_support_cents/100 as lifetime_support
    , COALESCE(patreon_status_is_patron, False) AS is_patron
FROM {TARGET_USERS_BRONZE_TB}
""").toPandas()

update_histogram(df)
share_per_user(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Per country distribution
# MAGIC
# MAGIC Users totals per country

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
    country
    , COALESCE(patreon_status_is_patron, FALSE) as is_patron
    , SUM(patreon_status_campaign_lifetime_support_cents)/100 AS campaign_support
FROM {TARGET_USERS_BRONZE_TB}
GROUP BY ALL
ORDER BY campaign_support DESC
""").display()

# COMMAND ----------


