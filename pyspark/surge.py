import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    when,
    sum as _sum,
    greatest,
    least,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


def main():
    # ----------------------------
    # Argument parsing
    # ----------------------------
    parser = argparse.ArgumentParser(description="Surge Pricing Pipeline")
    parser.add_argument(
        "--bootstrap-servers",
        default="YOUR_KAFKA_IP:9092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--topic",
        default="topic1",
        help="Kafka topic name"
    )
    parser.add_argument(
        "--checkpoint-location",
        default="/tmp/surge_pricing_checkpoint",
        help="Spark checkpoint location"
    )
    args = parser.parse_args()

    # ----------------------------
    # Spark session
    # ----------------------------
    spark = SparkSession.builder \
        .appName("SurgePricingDetection") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ----------------------------
    # Input schema
    # ----------------------------
    schema = StructType([
        StructField("request_id", StringType(), True),
        StructField("rider_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("event_type", StringType(), True),  # ride_request | driver_available
        StructField("pickup_lat", DoubleType(), True),
        StructField("pickup_lon", DoubleType(), True),
        StructField("dropoff_lat", DoubleType(), True),
        StructField("dropoff_lon", DoubleType(), True),
        StructField("zone_id", StringType(), True),
        StructField("event_time", StringType(), True)   # ISO timestamp string
    ])

    # ----------------------------
    # 1. Read from Kafka (FROM BEGINNING)
    # ----------------------------
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.bootstrap_servers) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # ----------------------------
    # 2. Parse JSON and cast timestamp
    # ----------------------------
    parsed_stream = raw_stream \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", col("event_time").cast("timestamp"))

    # ----------------------------
    # 3. Windowed aggregation (5‑minute tumbling window + watermark)
    # ----------------------------
    windowed_aggs = parsed_stream \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("zone_id")
        ) \
        .agg(
            _sum(
                when(col("event_type") == "ride_request", 1).otherwise(0)
            ).alias("demand_count"),
            _sum(
                when(col("event_type") == "driver_available", 1).otherwise(0)
            ).alias("supply_count")
        )

    # ----------------------------
    # 4. Surge multiplier calculation
    # ----------------------------
    raw_multiplier = (
        col("demand_count").cast("double") /
        greatest(col("supply_count"), lit(1))
    )

    surge_multiplier = when(
        col("demand_count") > col("supply_count"),
        least(greatest(lit(1.0), raw_multiplier), lit(5.0))
    ).otherwise(lit(1.0))

    enriched_stream = windowed_aggs.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("zone_id"),
        col("demand_count").cast("long"),
        col("supply_count").cast("long"),
        surge_multiplier.alias("surge_multiplier")
    )

    # ----------------------------
    # 5. Hotspot flag
    # ----------------------------
    final_stream = enriched_stream.withColumn(
        "is_hotspot",
        col("surge_multiplier") > 2.0
    )

    # ----------------------------
    # 6. Output to Console & GCS Only
    # ----------------------------
    from google.cloud import storage
    
    def process_batch(df, epoch_id):
        # Show in console like before
        df.show(truncate=False)
        
        records = df.toJSON().collect()
        if records:
            try:
                print(f"Appending batch {epoch_id} directly to GCS bucket {os.environ.get('GCS_BUCKET', 'YOUR_BUCKET_NAME')}...")
                storage_client = storage.Client()
                bucket = storage_client.bucket(os.environ.get("GCS_BUCKET", "YOUR_BUCKET_NAME"))
                blob = bucket.blob("surge-results/surge_results.json")
                
                new_data = "\n".join(records) + "\n"
                
                # Fetch existing content to append, avoiding local files
                if blob.exists():
                    existing_content = blob.download_as_text()
                    updated_content = existing_content + new_data
                else:
                    updated_content = new_data
                
                blob.upload_from_string(updated_content)
                print(f"Success! Batch {epoch_id} appended in GCS.")
            except Exception as e:
                print(f"Error uploading to GCS: {e}")

    query = final_stream.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", args.checkpoint_location) \
        .start()

    # ----------------------------
    # Graceful shutdown handling
    # ----------------------------
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        print("\nStopping streaming query gracefully...")
        try:
            query.stop()
        except Exception:
            pass
        try:
            spark.stop()
        except Exception:
            pass
        print("Shutdown complete.")


if __name__ == "__main__":
    main()