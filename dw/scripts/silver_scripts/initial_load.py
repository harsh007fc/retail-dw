from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def init_spark(app_name="SCD2 Initial Load"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_bronze_data(spark):
    return {
        "orders": spark.read.parquet("bronze/raw_orders"),
        "products": spark.read.parquet("bronze/raw_products"),
        "customers": spark.read.parquet("bronze/raw_customers"),
        "order_items": spark.read.parquet("bronze/raw_order_items"),
        "browsing_logs": spark.read.parquet("bronze/raw_browsing_logs")
    }


def apply_scd2_initial_load(df, key_columns):
    df = df.dropDuplicates(key_columns)
    df = df.withColumn("is_current", lit(True)) \
           .withColumn("effective_date", current_timestamp()) \
           .withColumn("end_date", lit(None).cast("timestamp")) \
           .withColumn("updated_at", current_timestamp())
    return df


def process_products(bronze_df):
    return apply_scd2_initial_load(bronze_df, ["product_id"])


def process_customers(bronze_df):
    return apply_scd2_initial_load(bronze_df, ["customer_id"])


def process_orders(bronze_df):
    return bronze_df.dropDuplicates(["order_id"]) \
        .filter(col("order_status").isNotNull()) \
        .withColumn("updated_at", current_timestamp())


def process_order_items(bronze_df, product_df):
    product_current = product_df.filter(col("is_current") == True).select(
        "product_id", "price", "category", "brand")
    return bronze_df.join(product_current, on="product_id", how="left") \
        .withColumn("updated_at", current_timestamp())


def process_browsing_behavior(bronze_df):
    from pyspark.sql.functions import to_date, count, avg

    return bronze_df.withColumn("date", to_date("timestamp")) \
        .groupBy("customer_id", "product_id", "date") \
        .agg(
            count("*").alias("view_count"),
            avg("duration_seconds").alias("avg_time_spent")
        ).withColumn("updated_at", current_timestamp())


def write_to_silver(df, path):
    df.write.mode("overwrite").parquet(path)


def main():
    spark = init_spark()
    bronze = read_bronze_data(spark)

    stg_products = process_products(bronze["products"])
    stg_customers = process_customers(bronze["customers"])
    stg_orders = process_orders(bronze["orders"])
    stg_order_items = process_order_items(bronze["order_items"], stg_products)
    stg_browsing_behavior = process_browsing_behavior(bronze["browsing_logs"])

    write_to_silver(stg_products, "silver/stg_products")
    write_to_silver(stg_customers, "silver/stg_customers")
    write_to_silver(stg_orders, "silver/stg_orders")
    write_to_silver(stg_order_items, "silver/stg_order_items")
    write_to_silver(stg_browsing_behavior, "silver/stg_browsing_behavior")


if __name__ == "__main__":
    main()
