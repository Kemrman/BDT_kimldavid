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

# MAGIC %sql
# MAGIC SELECT * FROM timestamps

# COMMAND ----------

from pyspark.sql.functions import col, max, current_timestamp, date_format, expr

df = spark.read.format('delta').table('timestamps')
last_timestamp = df.select(max(col("timestamp"))).collect()[0][0]

curr_timestamp_df = spark.sql('SELECT CURRENT_TIMESTAMP() AS timestamp')
curr_timestamp = curr_timestamp_df.collect()[0][0]
curr_timestamp_df.write.mode('append').saveAsTable("timestamps")

# COMMAND ----------

from pyspark.sql.functions import explode, from_json, col, udf,lit
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, FloatType, IntegerType, BooleanType

# Define schema
schema = StructType([
    StructField("properties", StructType([
        StructField("last_position", StructType([
            StructField("last_stop", StructType([
                StructField("arrival_time", StringType(), True),
                StructField("id", StringType(), True),
            ]), True),
        ]), True),
        StructField("trip", StructType([
            StructField("gtfs", StructType([
                StructField("route_id", StringType(), True),
                StructField("route_short_name", StringType(), True),
                StructField("trip_id", StringType(), True),
                StructField("trip_short_name", StringType(), True)
            ]), True),
            StructField("vehicle_type", StructType([
                StructField("description_en", StringType(), True)
            ]), True)
        ]), True)
    ]), False)
])

# Load Delta table
df = spark.read.format('delta').table('bronze')
df = df.filter((col("timestamp") <= curr_timestamp) & (col("timestamp") > last_timestamp)) \
       .withColumn("value", col("value").cast(StringType()))

# Parse data and get relevant data as route/trip info and stop info
# Kafka returns only id's of routes/trips. Another API call should be called if real names required
df_exploded = (
    df.withColumn("json_struct", explode(from_json("value", ArrayType(schema))))
    .select(
        col("json_struct.properties.last_position.last_stop.id").alias("last_stop_id"),
        col("json_struct.properties.last_position.last_stop.arrival_time").alias("last_stop_time"),
        col("json_struct.properties.trip.vehicle_type.description_en").alias("vehicle_type"),
        col("json_struct.properties.trip.gtfs.trip_id").alias("trip_id"),
        col("json_struct.properties.trip.gtfs.trip_short_name").alias("trip_name"),
        col("json_struct.properties.trip.gtfs.route_short_name").alias("route_name"),
        col("json_struct.properties.trip.gtfs.route_id").alias("route_id"),
    )
    .withColumn("from_timestamp", lit(last_timestamp))
    .withColumn("to_timestamp", lit(curr_timestamp))
)

# Save dataframe to table
df_exploded.write.mode('append').saveAsTable("silver")

# COMMAND ----------

from pyspark.sql.functions import explode, from_json, col, udf, countDistinct, count
import requests, time

#Function to get route name from Golemio API
def get_route_name(route_id):
    #in case of large data, Too many requests is returned
    url = f'https://api.golemio.cz/v2/gtfs/routes/{route_id}'
    headers = {
        'accept': 'application/json',
        'X-Access-Token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MjM0NywiaWF0IjoxNzAzODY1MjIzLCJleHAiOjExNzAzODY1MjIzLCJpc3MiOiJnb2xlbWlvIiwianRpIjoiMGEzNzQwMGMtYWIxNi00NmU0LTgwNTEtY2UwZjBjNTc1YjI4In0.e8nMLQ247THfwSX2tp2KYy5MJ5U09PWgkjUOEL6R-RM' 
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        time.sleep(1)
        return get_route_name(route_id)
    return response.json()['route_long_name']

df = spark.read.format('delta').table('silver')
df = df.filter((col("from_timestamp") == last_timestamp) & (col("to_timestamp") == curr_timestamp))

#Get count of vehicles which use the route (count vehicle/trip only once per trip)
get_route_name_udf = udf(get_route_name)
df_routes_agg = (
    df.select(col("trip_id"), col("route_id")) 
    .groupBy("route_id") 
    .agg(countDistinct("trip_id").alias("route_count")) 
    .orderBy("route_count", ascending=False)
    .limit(100)
    .withColumn("route_name", get_route_name_udf(col("route_id")))
    .withColumn("from_timestamp", lit(last_timestamp))
    .withColumn("to_timestamp", lit(curr_timestamp))
)

# Read the JSON file with stop names into a DataFrame
json_file_path = "file:/Workspace/Repos/kimldavi_bdt/BDT_kimldavid/data/stops.json"
df_stops = spark.read.json(json_file_path, multiLine=True)
df_stops = (
     df_stops.select(col("fullname").alias("stop_name"), explode(col("stops.gtfsIds")).alias("stop_id"))
     .select(col("stop_name"), explode("stop_id").alias("stop_id"))
)

# Get count of vehicles which passed the stop (count vehicle/trip only once per trip)
df_stops_agg = (
    df.join(df_stops, df_stops["stop_id"] == df["last_stop_id"], "inner")
    .select(col("stop_name"), col("trip_id")) 
    .groupBy("stop_name") 
    .agg(countDistinct("trip_id").alias("stop_count")) 
    .orderBy("stop_count", ascending=False)
    .limit(100)
    .withColumn("from_timestamp", lit(last_timestamp))
    .withColumn("to_timestamp", lit(curr_timestamp))
)

#df_routes_agg.display()
df_routes_agg.write.mode('append').saveAsTable("gold_routes")
df_stops_agg.write.mode('append').saveAsTable("gold_stops")
