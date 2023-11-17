# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Prepare_songs_data
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   artist_id,
# MAGIC   artist_name,
# MAGIC   duration,
# MAGIC   release,
# MAGIC   tempo,
# MAGIC   time_signature,
# MAGIC   title,
# MAGIC   year,
# MAGIC   current_timestamp() AS processed_time
# MAGIC FROM
# MAGIC   raw_song_data;
# MAGIC
# MAGIC INSERT INTO
# MAGIC   Prepare_songs_data
# MAGIC SELECT
# MAGIC   artist_id,
# MAGIC   artist_name,
# MAGIC   duration,
# MAGIC   release,
# MAGIC   tempo,
# MAGIC   time_signature,
# MAGIC   title,
# MAGIC   year,
# MAGIC   current_timestamp()
# MAGIC FROM
# MAGIC   raw_song_data
