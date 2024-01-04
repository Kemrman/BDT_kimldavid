# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC ## PID STREAMING
# MAGIC ---
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 20px;">
# MAGIC   <img src="https://raw.githubusercontent.com/animrichter/BDT_2023/master/data/assets/streaming.png" style="width: 1200">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, CONCAT(DATE_FORMAT(from_timestamp, 'dd.MM.yyyy HH:mm'), ' - ', DATE_FORMAT(to_timestamp, 'HH:mm')) AS interval FROM gold_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, CONCAT(DATE_FORMAT(from_timestamp, 'dd.MM.yyyy HH:mm'), ' - ', DATE_FORMAT(to_timestamp, 'HH:mm')) AS interval  FROM gold_routes
