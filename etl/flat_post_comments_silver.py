# Databricks notebook source
# MAGIC %run ./etl_constants

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {TARGET_POST_COMMENTS_JOIN_SILVER} AS
SELECT
    posts_comments.id_oid AS post_comments_id_oid
    , posts_comments.* EXCEPT(id_oid)
    , posts.* EXCEPT(id_oid, updated_at_date, created_at_date)
    , users.* EXCEPT(id_oid, updated_at_date, created_at_date)
FROM {TARGET_POSTS_COMMENTS_BRONZE_TB} posts_comments
LEFT JOIN {TARGET_POSTS_BRONZE_TB} posts
    ON posts.id_oid = posts_comments.post_id_oid
LEFT JOIN {TARGET_USERS_BRONZE_TB} users
    ON users.id_oid = posts_comments.user_id_oid
""")

# COMMAND ----------


