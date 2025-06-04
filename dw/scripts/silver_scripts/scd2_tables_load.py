# final first script

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, date
from delta import *
from delta.tables import DeltaTable
import json
import os

min_available_date = datetime(2025, 5, 20).date()  # '2025-05-20'


def create_spark_session():
    builder = (
        SparkSession.builder.appName("Pipeline_Sales")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def load_bookmark(spark, bookmark_path):
    """Load processing bookmark if exists, otherwise return None"""
    try:
        if DeltaTable.isDeltaTable(spark, bookmark_path):
            bookmark_df = spark.read.format("delta").load(bookmark_path)
            latest_bookmark = bookmark_df.orderBy(col("processed_date").desc()).first()
            return latest_bookmark["processed_date"] if latest_bookmark else None
    except Exception as e:
        print(f"Bookmark not found or error loading: {str(e)}")
    return None


def save_bookmark(spark, bookmark_path, current_date):
    """Save processing bookmark"""
    bookmark_data = [(current_date,)]
    bookmark_df = spark.createDataFrame(bookmark_data, ["processed_date"])

    if DeltaTable.isDeltaTable(spark, bookmark_path):
        delta_table = DeltaTable.forPath(spark, bookmark_path)
        delta_table.alias("target").merge(
            bookmark_df.alias("source"), "target.processed_date = source.processed_date"
        ).whenNotMatchedInsertAll().execute()
    else:
        bookmark_df.write.format("delta").mode("overwrite").save(bookmark_path)


def get_available_dates(spark, base_path):
    """Get all available date partitions in the source data"""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        dates = []

        # List all directories in the base path
        date_dirs = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(base_path))

        for dir in date_dirs:
            if dir.isDirectory():
                dir_name = dir.getPath().getName()
                if dir_name.startswith(("date=", "ingestion_date=")):
                    date_str = dir_name.split("=")[1]
                    try:
                        dates.append(date.fromisoformat(date_str))
                    except ValueError:
                        print(f"Skipping invalid date format: {date_str}")

        return sorted(list(set(dates)))  # Remove duplicates and sort
    except Exception as e:
        print(f"Error reading available dates: {str(e)}")
        return []


def load_product_dimension(spark, processing_date):
    """Load product dimension for a specific date"""
    print(f"Loading product dimension for date: {processing_date}")
    date_str = processing_date.strftime("%Y-%m-%d")
    product_df = spark.read.parquet(
        f"dw/destination_bucket/bronze/products/ingestion_date={date_str}/"
    )
    product_df = product_df.withColumn("updated_at", lit(processing_date))

    # Get the latest version of each product for this date
    windowSpec = Window.partitionBy("product_id").orderBy(col("updated_at").desc())
    latest_products = product_df.withColumn(
        "row_num", row_number().over(windowSpec)
    ).filter(col("row_num") == 1)

    dim_product = (
        latest_products.withColumn("product_sk", monotonically_increasing_id())
        .withColumn("active_from", lit(processing_date))
        .withColumn("active_to", lit(None).cast("date"))
        .withColumn("is_active", lit(True))
        .select(
            "product_sk",
            col("product_id").alias("product_id_nk"),
            "name",
            "category_id",
            "brand",
            "price",
            "stock_quantity",
            "active_from",
            "active_to",
            "is_active",
        )
    )

    return dim_product


def load_customer_dimension(spark, processing_date):
    """Load customer dimension for a specific date"""
    print(f"Loading customer dimension for date: {processing_date}")
    date_str = processing_date.strftime("%Y-%m-%d")
    customer_df = spark.read.parquet(
        f"dw/destination_bucket/bronze/customers/ingestion_date={date_str}/"
    )
    customer_df = customer_df.withColumn("updated_at", lit(processing_date))
    orders_df = spark.read.parquet(
        f"dw/destination_bucket/bronze/orders/ingestion_date={date_str}/"
    )
    orders_df = orders_df.withColumn("updated_at", lit(processing_date))

    # Get latest version of each customer
    windowSpec = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
    latest_customers = customer_df.withColumn(
        "row_num", row_number().over(windowSpec)
    ).filter(col("row_num") == 1)

    # Calculate loyalty tier
    order_counts = orders_df.groupBy("customer_id").agg(count("*").alias("order_count"))

    dim_customer = (
        latest_customers.join(order_counts, "customer_id", "left")
        .withColumn(
            "loyalty_tier",
            when(col("order_count") >= 10, "Gold")
            .when(col("order_count") >= 5, "Silver")
            .otherwise("Bronze"),
        )
        .withColumn("customer_sk", monotonically_increasing_id())
        .withColumn("active_from", lit(processing_date))
        .withColumn("active_to", lit(None).cast("date"))
        .withColumn("is_active", lit(True))
        .select(
            "customer_sk",
            col("customer_id").alias("customer_id_nk"),
            "name",
            "email",
            "address",
            "loyalty_tier",
            "active_from",
            "active_to",
            "is_active",
        )
    )

    return dim_customer


def merge_product_dimension(spark, new_dim_product, processing_date):
    """Merge new product data into existing dimension with SCD2 logic"""
    product_path = "dw/destination_bucket/silver/dim_product/"

    if DeltaTable.isDeltaTable(spark, product_path):
        delta_table = DeltaTable.forPath(spark, product_path)

        # Update existing records that have changed (set active_to and is_active)
        delta_table.alias("target").merge(
            new_dim_product.alias("source"),
            """
            target.product_id_nk = source.product_id_nk 
            AND target.is_active = true 
            AND (
                target.name != source.name OR
                target.category_id != source.category_id OR
                target.brand != source.brand OR
                target.price != source.price OR
                target.stock_quantity != source.stock_quantity
            )
            """,
        ).whenMatchedUpdate(
            set={"active_to": lit(processing_date), "is_active": lit(False)}
        ).execute()

        # Insert new records (both new products and new versions of existing products)
        delta_table.alias("target").merge(
            new_dim_product.alias("source"),
            "target.product_id_nk = source.product_id_nk AND target.is_active = true",
        ).whenNotMatchedInsertAll().execute()
    else:
        # First time load
        new_dim_product.write.format("delta").mode("overwrite").save(product_path)


def merge_customer_dimension(spark, new_dim_customer, processing_date):
    """Merge new customer data into existing dimension with SCD2 logic"""
    customer_path = "dw/destination_bucket/silver/dim_customer/"

    if DeltaTable.isDeltaTable(spark, customer_path):
        delta_table = DeltaTable.forPath(spark, customer_path)

        # Update existing records that have changed
        delta_table.alias("target").merge(
            new_dim_customer.alias("source"),
            """
            target.customer_id_nk = source.customer_id_nk 
            AND target.is_active = true 
            AND (
                target.name != source.name OR
                target.email != source.email OR
                target.address != source.address OR
                target.loyalty_tier != source.loyalty_tier
            )
            """,
        ).whenMatchedUpdate(
            set={"active_to": lit(processing_date), "is_active": lit(False)}
        ).execute()

        # Insert new records
        delta_table.alias("target").merge(
            new_dim_customer.alias("source"),
            "target.customer_id_nk = source.customer_id_nk AND target.is_active = true",
        ).whenNotMatchedInsertAll().execute()
    else:
        # First time load
        new_dim_customer.write.format("delta").mode("overwrite").save(customer_path)


def process_date(spark, processing_date, bookmark_path, initial_load=False):
    """Process data for a specific date"""
    print(f"\nProcessing date: {processing_date}")

    dim_product = load_product_dimension(spark, processing_date)
    dim_customer = load_customer_dimension(spark, processing_date)

    if initial_load:
        # For initial load, just overwrite
        dim_product.write.format("delta").mode("overwrite").save(
            "dw/destination_bucket/silver/dim_product/"
        )
        dim_customer.write.format("delta").mode("overwrite").save(
            "dw/destination_bucket/silver/dim_customer/"
        )
    else:
        # For incremental load, use SCD2 merge logic
        merge_product_dimension(spark, dim_product, processing_date)
        merge_customer_dimension(spark, dim_customer, processing_date)

    # Save bookmark
    save_bookmark(spark, bookmark_path, processing_date)
    print(f"Saved bookmark for date: {processing_date}")


def main():
    spark = create_spark_session()
    bookmark_path = "dw/destination_bucket/silver/dwh_processing_bookmark"

    # Get all available dates in source data
    product_dates = get_available_dates(spark, "dw/destination_bucket/bronze/products")
    customer_dates = get_available_dates(
        spark, "dw/destination_bucket/bronze/customers"
    )

    # Find common dates available in both sources
    common_dates = sorted(list(set(product_dates) & set(customer_dates)))
    if not common_dates:
        raise ValueError("No common dates found between product and customer sources")

    # Get last processed date from bookmark
    last_processed_date = load_bookmark(spark, bookmark_path)

    if last_processed_date:
        # Incremental load - process only new dates
        dates_to_process = [d for d in common_dates if d > last_processed_date]
        print(f"Resuming from bookmark. Last processed date: {last_processed_date}")
        initial_load = False
    else:
        # Initial load - process all dates starting from min_available_date
        dates_to_process = [d for d in common_dates if d >= min_available_date]
        print("No bookmark found. Performing initial load.")
        initial_load = True

    if not dates_to_process:
        print("No new dates to process. Exiting.")
        return

    # Process each date in order
    for processing_date in dates_to_process:
        try:
            process_date(spark, processing_date, bookmark_path, initial_load)
        except Exception as e:
            print(f"Error processing date {processing_date}: {str(e)}")
            raise

    spark.stop()


if __name__ == "__main__":
    main()
