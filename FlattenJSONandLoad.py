from pyspark.sql.functions import col, explode

# ----------------------------
# 1️⃣ Read ALL JSON files
# ----------------------------

# This will automatically read every file in your raw_weather_json folder
Json_data = (
    spark.read
    .option("multiline", "true")
    .json("Files/raw_weather_json/*.json")
)

print("✅ Loaded JSON files")

# ----------------------------
# 2️⃣ Explode the forecastday array
# ----------------------------

df = Json_data.withColumn(
    "forecastday",
    explode("forecast.forecastday")
).drop("forecast")

df.printSchema()

# ----------------------------
# 3️⃣ Select needed columns
# ----------------------------

new_df = df.select(
    col("location.*"),               # include location details
    col("forecastday.date").alias("date"),
    col("forecastday.day.*")         # unpack all day-level fields
)

# ----------------------------
# 4️⃣ Save as Delta table
# ----------------------------

new_df.write.mode("overwrite").saveAsTable("weather_forecast_table")

print("✅ Final weather_forecast_table saved")
