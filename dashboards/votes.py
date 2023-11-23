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

# COMMAND ----------

post_id_df = spark.sql(f"""
SELECT DISTINCT
    id_oid, created_at_date
    , CASE WHEN translations_description IS NOT NULL AND translations_description != ''
        THEN translations_description
        ELSE translations_title
    END translation_en
FROM {TARGET_POSTS_ENGLISH_SILVER}
WHERE created_at_date BETWEEN '{w("start_date")}' AND '{w("end_date")}'
ORDER BY created_at_date DESC
""").persist()

post_ids = post_id_df.select('id_oid').toPandas()['id_oid'].tolist()
w.create_text('post_id', post_ids[0])

# COMMAND ----------

def get_comments_from_post():
    return spark.sql(f"""
    SELECT DISTINCT
        CASE WHEN translations_description IS NOT NULL AND translations_description != ''
            THEN translations_description
            ELSE translations_title
        END body
    FROM {TARGET_POSTS_ENGLISH_SILVER}
    WHERE id_oid = '{w("post_id")}'

    UNION ALL

    SELECT DISTINCT body FROM {TARGET_POST_COMMENTS_JOIN_SILVER}
    WHERE post_id_oid = '{w("post_id")}'
    """).toPandas()['body'].tolist()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # User engagement (timeseries)
# MAGIC
# MAGIC Plots:
# MAGIC - n_votes: shows the number of votes (weighted and not weighted) at every month.
# MAGIC - votes per index: shows the total number of votes per index (see below for the interpretation).
# MAGIC - votes per country: shows the share of votes per country (without weight).
# MAGIC - votes per country weight: shows the share of votes per country (with weight).
# MAGIC - share votes: share of votes per is_patron status (without weight).
# MAGIC - share weight votes: share of votes per is_patron status (with weight).
# MAGIC
# MAGIC Analysis:
# MAGIC - n_votes
# MAGIC   1. The number shows the same trend as the number of posts: it decreases and has a small oscilation.
# MAGIC   This may indicate that there are less votes due to the lower amount of posts.
# MAGIC - votes per index
# MAGIC   1. Normally, the information regarding which index was voted is irrelevant, however people usually
# MAGIC   have a bias to vote in the first few options.
# MAGIC   The existence of a bias can be confirmed by noticing that the option 0 has more votes than the second.
# MAGIC   However, notice that this bias may not be relevant, but may due to pool with options yes/no with the yes always at option 0.
# MAGIC - votes per country:
# MAGIC   1. The ranking of most votes do correlate with the highest monetary contribution.
# MAGIC   1. However, by looking at the 4th country with the most votes (BR), we can see that the ammount of votes does always necessarily correlated with monetary contribution.
# MAGIC   This behavior is similar to the behavior observed in the number of comments.
# MAGIC - votes per country weight:
# MAGIC   1. Here the weight seems to fix the proportion of monetary contribution and votes.
# MAGIC   2. It is important to balance the importance of monetary contribution and individual votes:
# MAGIC     - attending to users with highest contribution only accounts for a small part of the community but maximizes money.
# MAGIC     - attending to users equally ensures better coverage which may result in higher satisfaction.
# MAGIC - share votes:
# MAGIC   1. As expected most of the votes are from users without the patron status.
# MAGIC - share weight votes:
# MAGIC   1. Adding weight invert the results.
# MAGIC
# MAGIC Here it is interesting to discuss the duality between people with and without money.
# MAGIC From the business standpoint, it is interesting to benefit those with higher monetary contribution,
# MAGIC however these people are a very selected and small part of the community.
# MAGIC Responding more frequently to those with lower monetary contribution may result in less money in the short term,
# MAGIC but it may result in a game better suited for everyone and, as a consequence, in better organic growth.

# COMMAND ----------

spark.sql(f"""
SELECT 
    DATE_TRUNC('month', created_at_date) as `date`
    , country
    , COALESCE(patreon_status_is_patron, FALSE) AS is_patron
    , vote_index
    , COUNT(vote_index) AS n_votes
    , SUM(vote_weight) AS sum_vote_weight
FROM {TARGET_POST_VOTES_JOIN_SILVER}
WHERE created_at_date BETWEEN '{w("start_date")}' AND '{w("end_date")}'
GROUP BY ALL
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Votes per pool
# MAGIC
# MAGIC Select an post id (paste in the post_id widgets) and run the cells.
# MAGIC This will result in a plot of the pool with and without weights

# COMMAND ----------

post_id_df.display()

# COMMAND ----------

df = spark.sql(f"""
SELECT 
    vote_index
    , option
    , country
    , COALESCE(patreon_status_is_patron, FALSE) AS is_patron
    , COUNT(vote_index) AS n_votes
    , SUM(vote_weight) AS vote_weight
FROM {TARGET_POST_VOTES_JOIN_SILVER}
WHERE post_id_oid = '{w("post_id")}'
GROUP BY ALL
ORDER BY vote_index ASC
""")

df.display()

# COMMAND ----------

comments = get_comments_from_post()

message = """Summarize these responses the message and write the main keywords.""" \
    """For context, these messages were written in a Patreon for adult games\n""" \
    """This is the message\n""" \
    """------------------------------\n""" \
    f"""{comments[0]}\n""" \
    """------------------------------\n""" \
    """And these are the responses\n""" \
    """------------------------------\n""" \
    f"""{str(comments[1:])}\n""" \
    """------------------------------\n""" \
    """Return a single JSON in the format: {"summary": "summary here", "keywords": ["keyword 1", "keyword 2", ...]}.""" \
    """The JSON must contain only information regarding the responses."""

print("------------post------------")
print(comments[0])
print("----------------------------")

if len(comments) >= 5:
    api = OpenAIRequests(dbutils.secrets.get(scope='openai', key='openai_key'))
    r = api.chat_completion(message)
    j = r.json()
    df = api.get_content_df()
    print(f"Total cost (USD): {api.calculate_cost()}")
    df.display()
else:
    print("Less than 5 comments. Printing them.")
    for c in comments:
        print('--------------------')
        print(c)
        print('--------------------')


# COMMAND ----------

# print(comments)

print("Comments with the most ammount of votes.")
spark.sql(f"""
SELECT post_id_oid, count(vote_id_oid) FROM {TARGET_POST_VOTES_JOIN_SILVER}
GROUP BY 1
ORDER BY 2 DESC
""").display()
