from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, date, timedelta
from delta import *
from delta.tables import DeltaTable

min_available_date = datetime(2025, 5, 20).date()  # '2025-05-20'

def create_spark_session():
    builder = SparkSession.builder.appName("Pipeline_Sales")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")\
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    
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
            bookmark_df.alias("source"),
            "target.processed_date = source.processed_date"
        ).whenNotMatchedInsertAll().execute()
    else:
        bookmark_df.write.format("delta").mode("overwrite").save(bookmark_path)

def get_available_dates(spark, base_path):
    """Get all available ingestion dates from the source data"""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
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

def generate_date_dimension(spark, start_date, end_date, processing_date):
    """Generate or update date dimension"""
    print("Generating/Updating date dimension...")
    date_dim_path = "../dw/destination_bucket/silver/dim_date/"

    # Check if we need to extend the date dimension
    try:
        if DeltaTable.isDeltaTable(spark, date_dim_path):
            current_max_date = spark.read.format("delta").load(date_dim_path)\
                                       .agg(max("full_date")).collect()[0][0]
            if current_max_date >= end_date:
                print("Date dimension already covers the required range")
                return None
            else:
                start_date = current_max_date + timedelta(days=1)
    except Exception as e:
        print(f"Error checking existing date dimension: {str(e)}")
        # Proceed with full load if there's an error reading existing data

    date_range = spark.range(
        start=start_date.toordinal(),
        end=end_date.toordinal()
    ).select(col("id").cast("integer").alias("day_ordinal"))

    dim_date = date_range.withColumn("full_date", to_date(col("day_ordinal") - lit(start_date.toordinal()), "yyyy-MM-dd")) \
        .withColumn("date_sk", date_format(col("full_date"), "yyyyMMdd").cast("integer")) \
        .withColumn("day", dayofmonth(col("full_date"))) \
        .withColumn("month", month(col("full_date"))) \
        .withColumn("year", year(col("full_date"))) \
        .withColumn("quarter", quarter(col("full_date"))) \
        .withColumn("ingestion_date", lit(processing_date)) \
        .drop("day_ordinal")
    
    return dim_date

def load_category_dimension(spark, processing_date):
    """Load category dimension with incremental support"""
    print("Loading category dimension...")
    date_str = processing_date.strftime("%Y-%m-%d")
    category_df = spark.read.parquet(f"destination_bucket/bronze/categories/ingestion_date={date_str}/*")
    category_df = category_df.withColumn("updated_at", lit(processing_date))
    
    # Get the latest version of each category for this date
    windowSpec = Window.partitionBy("category_id").orderBy(col("updated_at").desc())
    latest_categories = category_df.withColumn("row_num", row_number().over(windowSpec)) \
                                .filter(col("row_num") == 1)
    
    dim_category = latest_categories \
        .withColumn("category_sk", monotonically_increasing_id()) \
        .withColumn("active_from", lit(processing_date)) \
        .withColumn("active_to", lit(None).cast("date")) \
        .withColumn("is_active", lit(True)) \
        .select(
            "category_sk",
            "category_id",
            "category_name",
            "parent_category_id",
            "active_from",
            "active_to",
            "is_active"
        )
    
    return dim_category

def merge_category_dimension(spark, new_dim_category, processing_date):
    """Merge new category data into existing dimension with SCD2 logic"""
    category_path = "../dw/destination_bucket/silver/dim_category/"
    
    if DeltaTable.isDeltaTable(spark, category_path):
        delta_table = DeltaTable.forPath(spark, category_path)
        
        # Update existing records that have changed - use lit() to convert date to column
        delta_table.alias("target").merge(
            new_dim_category.alias("source"),
            """
            target.category_id = source.category_id 
            AND target.is_active = true 
            AND (
                target.category_name != source.category_name OR
                target.parent_category_id != source.parent_category_id
            )
            """
        ).whenMatchedUpdate(
            set={
                "active_to": lit(processing_date),  # Fixed: Use lit() to create column
                "is_active": lit(False)
            }
        ).execute()
        
        # Insert new records
        delta_table.alias("target").merge(
            new_dim_category.alias("source"),
            "target.category_id = source.category_id AND target.is_active = true"
        ).whenNotMatchedInsertAll().execute()
    else:
        # First time load
        new_dim_category.write.format("delta").mode("overwrite").save(category_path)

def process_date(spark, processing_date, bookmark_path, initial_load=False):
    """Process data for a specific date"""
    print(f"\nProcessing date: {processing_date}")
    
    # Date dimension (special handling as it's not SCD2)
    dim_date = generate_date_dimension(
        spark,
        start_date=datetime(2024, 1, 1).date(),
        end_date=datetime(2025, 12, 31).date(),
        processing_date=processing_date
    )
    if dim_date:
        dim_date.write.format("delta").mode("append").save("../dw/destination_bucket/silver/dim_date/")
    
    # Category dimension (SCD2)
    dim_category = load_category_dimension(spark, processing_date)
    if initial_load:
        dim_category.write.format("delta").mode("overwrite").save("../dw/destination_bucket/silver/dim_category/")
    else:
        merge_category_dimension(spark, dim_category, processing_date)
    
    # Save bookmark
    save_bookmark(spark, bookmark_path, processing_date)
    print(f"Saved bookmark for date: {processing_date}")

def main():
    spark = create_spark_session()
    bookmark_path = "../dw/destination_bucket/silver/static_dimensions_bookmark/"
    
    # Get available dates in source data
    category_dates = get_available_dates(spark, "../dw/destination_bucket/bronze/categories")
    if not category_dates:
        raise ValueError("No valid dates found in category source data")
    
    # Get last processed date from bookmark
    last_processed_date = load_bookmark(spark, bookmark_path)
    
    if last_processed_date:
        # Incremental load - process only new dates
        dates_to_process = [d for d in category_dates if d > last_processed_date]
        print(f"Resuming from bookmark. Last processed date: {last_processed_date}")
        initial_load = False
    else:
        # Initial load - process all dates
        dates_to_process = category_dates
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
    
    spark.stop()

if __name__ == "__main__":
    main()