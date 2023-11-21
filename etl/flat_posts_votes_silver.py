# Databricks notebook source
# MAGIC %run ./etl_constants

# COMMAND ----------

spark.sql(f"""
-- Note that there are votes without posts.
CREATE OR REPLACE VIEW {TARGET_POST_VOTES_JOIN_SILVER} AS
SELECT 
    votes.* EXCEPT(id_oid)
    , votes.id_oid AS vote_id_oid
    , posts.* EXCEPT(id_oid, updated_at_date, created_at_date, index)
    , users.* EXCEPT(id_oid, updated_at_date, created_at_date)
FROM {TARGET_VOTES_BRONZE_TB} votes
JOIN {TARGET_POSTS_ENGLISH_SILVER} posts
    ON votes.post_id_oid = posts.id_oid
    AND votes.vote_index = posts.index
JOIN {TARGET_USERS_BRONZE_TB} users
    ON votes.user_id_oid = users.id_oid
""")
