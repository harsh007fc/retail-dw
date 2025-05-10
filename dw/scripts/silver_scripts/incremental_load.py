from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder \
    .appName("SCD2 Incremental Load") \
    .getOrCreate()
    
    
    
    

def scd2_incremental_load(bronze_df, silver_df, business_keys, scd_columns, current_date):
    """
    Performs SCD2 incremental load in a simple and readable way.
    
    Parameters:
    - bronze_df: New snapshot DataFrame
    - silver_df: Existing SCD2 table DataFrame
    - business_keys: List of columns used as primary keys
    - scd_columns: List of columns to track for changes
    - current_date: The date of the load (string format 'YYYY-MM-DD')
    
    Returns:
    - final_df: DataFrame with updated SCD2 records
    """
    
    # Step 1: Get only current records from silver
    current_silver_df = silver_df.filter(col("is_current") == True)

    # Step 2: Join bronze and silver on business keys
    join_condition = [bronze_df[k] == current_silver_df[k] for k in business_keys]
    joined_df = bronze_df.alias("bronze").join(current_silver_df.alias("silver"), join_condition, "left")

    # Step 3: Add a column to mark if any SCD column has changed
    def scd_change_expr(col_name):
        return col(f"bronze.{col_name}") != col(f"silver.{col_name}")

    # Build a readable "has_changed" column
    has_changed = scd_change_expr(scd_columns[0])
    for c in scd_columns[1:]:
        has_changed = has_changed | scd_change_expr(c)

    joined_df = joined_df.withColumn("has_changed", has_changed)

    # Step 4: Split into changed and unchanged
    changed_df = joined_df.filter(col("has_changed") == True)
    unchanged_df = joined_df.filter(col("has_changed") == False)

    # Step 5: Expire old versions in silver (set is_current = False and end_date)
    expired_df = changed_df.select("silver.*").withColumn("is_current", lit(False)).withColumn("end_date", lit(current_date))

    # Step 6: Prepare new current rows from bronze
    new_records_df = changed_df.select("bronze.*") \
        .withColumn("is_current", lit(True)) \
        .withColumn("effective_date", lit(current_date)) \
        .withColumn("end_date", lit(None).cast("date"))

    # Step 7: Keep the unchanged rows as-is
    unchanged_df = unchanged_df.select("silver.*")

    # Step 8: Union all and return
    final_df = unchanged_df.unionByName(expired_df).unionByName(new_records_df)

    return final_df

# # Helper function for SCD2 logic
# def scd2_incremental_load(bronze_df, silver_df, business_keys, scd_columns, current_date):
#     """
#     Performs SCD2 incremental load.
    
#     bronze_df: New daily snapshot (raw_*).
#     silver_df: Existing SCD2 table (stg_*).
#     business_keys: List of keys to uniquely identify a row.
#     scd_columns: List of columns to track for changes.
#     current_date: Load date (usually today's date).
#     """

#     # Filter current records from silver
#     current_silver_df = silver_df.filter(col("is_current") == True)

#     # Join on business keys
#     join_cond = [bronze_df[k] == current_silver_df[k] for k in business_keys]
#     joined_df = bronze_df.alias("bronze").join(
#         current_silver_df.alias("silver"),
#         on=join_cond,
#         how="left"
#     )

#     # Check if any of the SCD columns have changed
#     condition = reduce(lambda x, y: x | y, [
#         col(f"bronze.{c}") != col(f"silver.{c}") for c in scd_columns
#     ])
#     changed_df = joined_df.filter(condition)

#     # Expire old versions
#     expired_df = changed_df.select("silver.*").withColumn(
#         "end_date", lit(current_date)
#     ).withColumn("is_current", lit(False))

#     # New current versions from bronze
#     new_current_df = changed_df.select("bronze.*").withColumn(
#         "effective_date", lit(current_date)
#     ).withColumn("end_date", lit(None).cast("date")).withColumn("is_current", lit(True))

#     # Unchanged records
#     unchanged_df = joined_df.filter(~condition).select("silver.*")

#     # Union all together
#     final_df = unchanged_df.unionByName(expired_df).unionByName(new_current_df)
    
#     return final_df

# Load bronze and silver tables (example for customers)
bronze_customers = spark.read.parquet("/bronze/raw_customers/")
silver_customers = spark.read.parquet("/silver/stg_customers/")

# Apply incremental SCD2
current_date = current_date = datetime.today().strftime('%Y-%m-%d')
final_customers = scd2_incremental_load(
    bronze_df=bronze_customers,
    silver_df=silver_customers,
    business_keys=["customer_id"],
    scd_columns=["email", "location", "customer_segment"],
    current_date=current_date
)

# Write to silver path
final_customers.write.mode("overwrite").parquet("/silver/stg_customers/")

# Repeat similarly for products
bronze_products = spark.read.parquet("/bronze/raw_products/")
silver_products = spark.read.parquet("/silver/stg_products/")
final_products = scd2_incremental_load(
    bronze_df=bronze_products,
    silver_df=silver_products,
    business_keys=["product_id"],
    scd_columns=["name", "category", "brand", "price"],
    current_date=current_date
)
final_products.write.mode("overwrite").parquet("/silver/stg_products/")

# You can wrap each load inside a function and call within an Airflow DAG task if needed.
