import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType

# Drivers necesarios: Kafka + Postgres JDBC
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 pyspark-shell'

def main():
    spark = SparkSession.builder \
        .appName("CryptoAnomalyDetector") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("source", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("timestamp", DoubleType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "crypto_prices") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    processed_df = parsed_df.withColumn("timestamp", 
                                      from_unixtime(col("timestamp")).cast("timestamp"))

    # sliding window: 2min con slide de 30s, watermark 10s
    windowed_avg = processed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "2 minutes", "30 seconds"),
            col("symbol")
        ) \
        .agg(avg("price").alias("avg_price"))

    def save_to_postgres(df, epoch_id):
        df_clean = df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("avg_price")
        )
        
        df_clean.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/crypto_db") \
            .option("dbtable", "crypto_alerts") \
            .option("user", "admin") \
            .option("password", "adminpassword") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(f"Batch {epoch_id} guardado en Postgres!")

    query = windowed_avg.writeStream \
        .outputMode("update") \
        .foreachBatch(save_to_postgres) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()