# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC ## PID STREAMING
# MAGIC ---
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 20px;">
# MAGIC   <img src="https://raw.githubusercontent.com/animrichter/BDT_2023/master/data/assets/streaming.png" style="width: 1200">
# MAGIC </div>

# COMMAND ----------

bootstrap = "b-2-public.felpidkafka.vi270o.c2.kafka.eu-central-1.amazonaws.com:9196,b-3-public.felpidkafka.vi270o.c2.kafka.eu-central-1.amazonaws.com:9196,b-1-public.felpidkafka.vi270o.c2.kafka.eu-central-1.amazonaws.com:9196"
topic = "FelPidTopic"

# Get data from Kafka stream
raw = (
    spark
        .readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', bootstrap)
        .option('subscribe', topic) 
        .option('startingOffsets', 'latest')
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="felpiduser" password="LVuI#Qq&1XjX2JrY";')
        .load()
)

file_path = '/mnt/test/test4.json'
checkpoint = '/mnt/test/check4.txt'

# Save data to table
(raw.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint)
   .toTable("bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze AS SELECT string(value), timestamp from events
