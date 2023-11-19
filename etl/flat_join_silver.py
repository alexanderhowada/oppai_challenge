# Databricks notebook source
# MAGIC %run ./etl_constants

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {TARGET_FLAT_JOIN_SILVER} AS
SELECT 
    votes.* EXCEPT(id_oid)
    , votes.id_oid AS vote_id_oid
    , posts.* EXCEPT(id_oid, updated_at_date, created_at_date)
    , post_comments.id_oid AS post_comments_id_oid
    , post_comments.* EXCEPT(id_oid, post_id_oid, user_id_oid, updated_at_date, created_at_date)
    , users.* EXCEPT(id_oid, updated_at_date, created_at_date)
FROM {TARGET_VOTES_BRONZE_TB} votes
JOIN {TARGET_POSTS_BRONZE_TB} posts
    ON votes.post_id_oid = posts.id_oid
JOIN {TARGET_POSTS_COMMENTS_BRONZE_TB} post_comments
    ON votes.post_id_oid = post_comments.post_id_oid
JOIN {TARGET_USERS_BRONZE_TB} users
    ON votes.user_id_oid = users.id_oid
""")

# COMMAND ----------

try:
    df.unpersist()
except:
    pass
df = spark.read.format('delta').table(TARGET_FLAT_JOIN_SILVER)
df = df.filter("updated_at_date BETWEEN '2023-06-01' AND '2023-07-01'")
df.persist()

# COMMAND ----------

display(df.limit(100))

# COMMAND ----------

display(df)
