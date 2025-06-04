from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, date
from delta import *
from delta.tables import DeltaTable

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
    """Get all available ingestion dates from the source data"""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        dates = []

        date_dirs = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(base_path))
        for dir in date_dirs:
            dir_name = dir.getPath().getName()
            if dir_name.startswith("ingestion_date="):
                date_str = dir_name.split("=")[1]
                try:
                    dates.append(date.fromisoformat(date_str))
                except ValueError:
                    print(f"Skipping invalid date format: {date_str}")

        return sorted(dates)
    except Exception as e:
        print(f"Error reading available dates: {str(e)}")
        return []


def load_fact_sales(spark, processing_date):
    """Load fact data for a specific date"""
    print(f"Loading fact data for date: {processing_date}")
    date_str = processing_date.strftime("%Y-%m-%d")

    # Read source data with explicit column selection to avoid ambiguity
    orders_df = spark.read.parquet(
        f"dw/destination_bucket/bronze/orders/ingestion_date={date_str}/"
    ).select("order_id", "customer_id", "order_date", "payment_method")

    order_items_df = spark.read.parquet(
        f"dw/destination_bucket/bronze/order_items/ingestion_date={date_str}/"
    ).select("order_id", "product_id", "quantity", col("price").alias("item_price"))

    # Get current versions of dimensions
    dim_product = (
        DeltaTable.forPath(spark, "dw/destination_bucket/silver/dim_product/")
        .toDF()
        .filter("is_active = true")
    )
    dim_customer = (
        DeltaTable.forPath(spark, "dw/destination_bucket/silver/dim_customer/")
        .toDF()
        .filter("is_active = true")
    )
    dim_date = DeltaTable.forPath(
        spark, "dw/destination_bucket/silver/dim_date/"
    ).toDF()

    # Join fact data with dimensions
    fact_sales = (
        orders_df.join(order_items_df, "order_id")
        .join(
            dim_product,
            (order_items_df["product_id"] == dim_product["product_id_nk"]),
            "left",
        )
        .join(
            dim_customer,
            (orders_df["customer_id"] == dim_customer["customer_id_nk"]),
            "left",
        )
        .join(
            dim_date,
            (date_format(col("order_date"), "yyyy-MM-dd") == dim_date["full_date"]),
            "left",
        )
        .select(
            col("order_id").alias("sale_id"),
            "date_sk",
            "customer_sk",
            "product_sk",
            "quantity",
            "item_price",
            (col("quantity") * col("item_price")).alias("total_amount"),
            "payment_method",
            lit(processing_date).alias("ingestion_date"),
        )
    )

    return fact_sales


def merge_fact_sales(spark, new_fact_sales, processing_date):
    """Merge new fact data into existing fact table"""
    fact_sales_path = "dw/destination_bucket/silver/fact_sales"

    if DeltaTable.isDeltaTable(spark, fact_sales_path):
        # For incremental load - append only (assuming immutable facts)
        new_fact_sales.write.format("delta").mode("append").save(fact_sales_path)
    else:
        # Initial load
        new_fact_sales.write.format("delta").mode("overwrite").save(fact_sales_path)


def process_date(spark, processing_date, bookmark_path, initial_load=False):
    """Process data for a specific date"""
    print(f"\nProcessing date: {processing_date}")

    # Load fact data for the date
    fact_sales = load_fact_sales(spark, processing_date)

    # Write or merge the fact data
    merge_fact_sales(spark, fact_sales, processing_date)

    # Save bookmark
    save_bookmark(spark, bookmark_path, processing_date)
    print(f"Saved bookmark for date: {processing_date}")


def main():
    spark = create_spark_session()
    bookmark_path = "dw/destination_bucket/silver/fact_sales_bookmark"

    # Get available dates in source data
    order_dates = get_available_dates(spark, "dw/destination_bucket/bronze/orders")
    order_item_dates = get_available_dates(
        spark, "dw/destination_bucket/bronze/order_items"
    )

    # Find common dates available in both sources
    common_dates = sorted(list(set(order_dates) & set(order_item_dates)))
    if not common_dates:
        raise ValueError("No common dates found between orders and order_items sources")

    # Get last processed date from bookmark
    last_processed_date = load_bookmark(spark, bookmark_path)

    if last_processed_date:
        # Incremental load - process only new dates
        dates_to_process = [d for d in common_dates if d > last_processed_date]
        print(f"Resuming from bookmark. Last processed date: {last_processed_date}")
        initial_load = False
    else:
        # Initial load - process all dates
        dates_to_process = common_dates
        print("No bookmark found. Performing initial load.")
        initial_load = True

    if not dates_to_process:
        print("No new dates to process. Exiting.")
        return

    # Process each date in order
    for processing_date in dates_to_process:
        try:
            process_date(spark, processing_date, bookmark_path, initial_load)
            # After first date, we're no longer in initial load mode
            initial_load = False
        except Exception as e:
            print(f"Error processing date {processing_date}: {str(e)}")
            raise

    # Optimize the fact table after loading - using correct path format
    if DeltaTable.isDeltaTable(spark, "fact_sales/"):
        print("Optimizing fact_sales table...")
        # Method 1: Using DeltaTable API
        delta_table = DeltaTable.forPath(
            spark, "dw/destination_bucket/silver/fact_sales/"
        )
        delta_table.optimize().executeZOrderBy("date_sk")

    spark.stop()


if __name__ == "__main__":
    main()
