import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, sum as spark_sum, count, avg, window,
    to_timestamp, current_timestamp, expr, lit, round as spark_round,
    desc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, ArrayType, TimestampType
)
from cassandra.cluster import Cluster

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---------- Spark ----------
def create_spark_connection():
    try:
        spark = (
            SparkSession.builder
            .appName("IntradayEcommercePipeline")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
            )
            .config("spark.cassandra.connection.host", "localhost")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logging.info("✅ Spark connection created successfully.")
        return spark
    except Exception as e:
        logging.error(f"❌ Couldn't create the Spark session: {e}")
        return None

# ---------- Cassandra Setup ----------
def ensure_keyspace_and_tables():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS ecommerce_analytics
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS ecommerce_analytics.order_summary (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                total_orders INT,
                total_sales DOUBLE,
                avg_order_size DOUBLE,
                PRIMARY KEY (window_start, window_end)
            );
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS ecommerce_analytics.sales_by_category (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                category TEXT,
                total_sales DOUBLE,
                order_count INT,
                PRIMARY KEY ((window_start, window_end), category)
            );
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS ecommerce_analytics.sales_by_region (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                region TEXT,
                total_sales DOUBLE,
                order_count INT,
                PRIMARY KEY ((window_start, window_end), region)
            );
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS ecommerce_analytics.recent_orders (
                order_id TEXT PRIMARY KEY,
                timestamp TIMESTAMP,
                user_id TEXT,
                product_name TEXT,
                category TEXT,
                quantity INT,
                total_price DOUBLE,
                status TEXT,
                created_at TIMESTAMP
            );
        """)

        logging.info("✅ Keyspace and tables ready in Cassandra.")
        session.shutdown()
        cluster.shutdown()
    except Exception as e:
        logging.error(f"❌ Failed to create keyspace/tables: {e}")
        raise

# ---------- Schemas ----------
def get_schemas():
    order_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("items", ArrayType(
            StructType([
                StructField("product_id", StringType(), True),
                StructField("qty", IntegerType(), True)
            ])
        ), True),
        StructField("total", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("discount_code", StringType(), True),
        StructField("region", StringType(), True),
        StructField("created_at", StringType(), True)
    ])

    payment_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("method", StringType(), True),
        StructField("paid_at", StringType(), True)
    ])

    shipment_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("carrier", StringType(), True),
        StructField("tracking_id", StringType(), True),
        StructField("shipped_at", StringType(), True)
    ])

    return order_schema, payment_schema, shipment_schema

# ---------- Product catalog mapping ----------
def get_product_mapping(spark):
    product_map = [
        ("P001", "Smartphone", "Electronics", 799.99),
        ("P002", "Laptop", "Electronics", 1199.99),
        ("P003", "T-Shirt", "Apparel", 19.99),
        ("P004", "Jeans", "Apparel", 39.99),
        ("P005", "Blender", "Home & Garden", 89.99),
        ("P006", "Yoga Mat", "Sports", 29.99),
    ]
    return spark.createDataFrame(product_map, ["product_id", "product_name", "category", "unit_price"])

# ---------- Processing Orders ----------
def process_orders(spark, order_schema):
    orders_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false") 
        .load()
    )

    parsed_orders = (
        orders_df
        .selectExpr("CAST(value AS STRING) AS json_data")
        .select(from_json(col("json_data"), order_schema).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp(col("created_at")))
    )

    flattened_orders = (
        parsed_orders
        .select(
            col("order_id"),
            col("user_id"),
            col("total"),
            col("region"),
            col("timestamp"),
            expr("explode(items)").alias("item")
        )
        .select(
            col("order_id"),
            col("user_id"),
            col("total"),
            col("region"),
            col("timestamp"),
            col("item.product_id"),
            col("item.qty").alias("quantity")
        )
    )

    # Load mapping as DataFrame and JOIN
    mapping_df = get_product_mapping(spark)

    enriched_orders = flattened_orders.join(mapping_df, on="product_id", how="left")

    return enriched_orders

# ---------- Write Functions ----------
def write_order_summary(df, epoch_id):
    try:
        windowed_summary = (
            df
            .withWatermark("timestamp", "1 minute")
            .groupBy(window(col("timestamp"), "1 minute"))
            .agg(
                count("order_id").alias("total_orders"),
                spark_sum("total").alias("total_sales"),
                avg("total").alias("avg_order_size")
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("total_orders"),
                spark_round(col("total_sales"), 2).alias("total_sales"),
                spark_round(col("avg_order_size"), 2).alias("avg_order_size")
            )
        )

        windowed_summary.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace", "ecommerce_analytics") \
            .option("table", "order_summary") \
            .save()
            
        logging.info(f"✅ Order summary batch {epoch_id} written to Cassandra.")
    except Exception as e:
        logging.error(f"❌ Error writing order summary batch {epoch_id}: {e}")

def write_sales_by_category(df, epoch_id):
    try:
        windowed_category = (
            df
            .withWatermark("timestamp", "1 minute")
            .groupBy(
                window(col("timestamp"), "1 minute"),
                col("category")
            )
            .agg(
                spark_sum("total").alias("total_sales"),
                count("order_id").alias("order_count")
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("category"),
                spark_round(col("total_sales"), 2).alias("total_sales"),
                col("order_count")
            )
        )

        windowed_category.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace", "ecommerce_analytics") \
            .option("table", "sales_by_category") \
            .save()
            
        logging.info(f"✅ Sales by category batch {epoch_id} written to Cassandra.")
    except Exception as e:
        logging.error(f"❌ Error writing sales by category batch {epoch_id}: {e}")

def write_sales_by_region(df, epoch_id):
    try:
        windowed_region = (
            df
            .withWatermark("timestamp", "1 minute")
            .groupBy(
                window(col("timestamp"), "1 minute"),
                col("region")
            )
            .agg(
                spark_sum("total").alias("total_sales"),
                count("order_id").alias("order_count")
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("region"),
                spark_round(col("total_sales"), 2).alias("total_sales"),
                col("order_count")
            )
        )

        windowed_region.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace", "ecommerce_analytics") \
            .option("table", "sales_by_region") \
            .save()
            
        logging.info(f"✅ Sales by region batch {epoch_id} written to Cassandra.")
    except Exception as e:
        logging.error(f"❌ Error writing sales by region batch {epoch_id}: {e}")

def write_recent_orders(df, epoch_id):
    try:
        recent_orders = (
            df
            .select(
                col("order_id"),
                col("timestamp"),
                col("user_id"),
                col("product_name"),
                col("category"),
                col("quantity"),
                (col("unit_price") * col("quantity")).alias("total_price"),
                lit("Delivered").alias("status"),
                current_timestamp().alias("created_at")
            )
            .orderBy(desc("timestamp"))
            .limit(100)
        )

        recent_orders.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace", "ecommerce_analytics") \
            .option("table", "recent_orders") \
            .save()
            
        logging.info(f"✅ Recent orders batch {epoch_id} written to Cassandra.")
    except Exception as e:
        logging.error(f"❌ Error writing recent orders batch {epoch_id}: {e}")

# ---------- Main ----------
if __name__ == "__main__":
    spark = create_spark_connection()
    if spark is None:
        raise SystemExit(1)

    ensure_keyspace_and_tables()
    order_schema, payment_schema, shipment_schema = get_schemas()

    enriched_orders = process_orders(spark, order_schema)

    summary_query = enriched_orders \
        .writeStream \
        .foreachBatch(write_order_summary) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints/order_summary") \
        .start()

    category_query = enriched_orders \
        .writeStream \
        .foreachBatch(write_sales_by_category) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints/sales_by_category") \
        .start()

    region_query = enriched_orders \
        .writeStream \
        .foreachBatch(write_sales_by_region) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints/sales_by_region") \
        .start()

    orders_query = enriched_orders \
        .writeStream \
        .foreachBatch(write_recent_orders) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/recent_orders") \
        .start()

    logging.info("🚀 Intraday E-commerce Analytics Pipeline Started")
    spark.streams.awaitAnyTermination()
