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
# MAGIC # Summary of post comments.
# MAGIC
# MAGIC This script uses ChatGPT to summarize post comments without the need of reading them.
# MAGIC
# MAGIC To use this script:
# MAGIC
# MAGIC 1. choose a post by its id (id_oid).
# MAGIC     - For demonstration purpose, I recommend choosing a post with a large ammount of post comments (see table with count of votes).
# MAGIC     - Otherwise, choose any other (see table with posts sorted by date)
# MAGIC 2. Paste the post_id in the widget post_id.
# MAGIC 3. Run the script.
# MAGIC 4. Read the summary.
# MAGIC
# MAGIC Observations:
# MAGIC 1. ChatGPT can return different responses to the same statement, hence it may be interesting to run the script many times for the same post_id.
# MAGIC 2. This uses the paid version of ChatGPT, therefore, please, be carefull with its use.
# MAGIC     - The sum of total cost is capped at 5 USD.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Posts ordered by creation date.

# COMMAND ----------

post_id_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Posts ordered by comment count.

# COMMAND ----------

# print(comments)

print("Comments with the most ammount of votes.")
spark.sql(f"""
SELECT post_id_oid, count(vote_id_oid) AS n_comments FROM {TARGET_POST_VOTES_JOIN_SILVER}
GROUP BY 1
ORDER BY 2 DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ChatGPT summary.

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

