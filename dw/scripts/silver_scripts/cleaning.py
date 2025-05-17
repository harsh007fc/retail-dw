from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp

spark = SparkSession.builder \
    .appName("Retail Silver Layer") \
    .getOrCreate()

bronze_path = "../dw/destination_bucket/bronze"
silver_path = "../dw/destination_bucket/silver"

def process_customers():
    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}/customers")
    df_clean = df.dropna(subset=["customer_id", "name", "email"]).dropDuplicates(["customer_id"])
    df_clean = df_clean.withColumn("registration_date", to_date("registration_date"))
    df_clean.write.mode("overwrite").parquet(f"{silver_path}/customers")

def process_products():
    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}/products")
    df_clean = df.dropna(subset=["product_id", "name"]).dropDuplicates(["product_id"])
    df_clean.write.mode("overwrite").parquet(f"{silver_path}/products")

def process_categories():
    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}/categories")
    df_clean = df.dropna(subset=["category_id", "name"]).dropDuplicates(["category_id"])
    df_clean.write.mode("overwrite").parquet(f"{silver_path}/categories")

def process_orders():
    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}/orders")
    df_clean = df.dropna(subset=["order_id", "customer_id"]).dropDuplicates(["order_id"])
    df_clean = df_clean.withColumn("order_date", to_date("order_date"))
    df_clean.write.mode("overwrite").parquet(f"{silver_path}/orders")

def process_order_items():
    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}/order_items")
    df_clean = df.dropna(subset=["order_item_id", "order_id", "product_id"]).dropDuplicates(["order_item_id"])
    df_clean.write.mode("overwrite").parquet(f"{silver_path}/order_items")

def process_reviews():
    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}/reviews")
    df_clean = df.dropna(subset=["review_id", "customer_id", "product_id"]).dropDuplicates(["review_id"])
    df_clean = df_clean.withColumn("review_date", to_date("review_date"))
    df_clean.write.mode("overwrite").parquet(f"{silver_path}/reviews")

def process_cart():
    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}/cart")
    df_clean = df.dropna(subset=["cart_id", "customer_id"]).dropDuplicates(["cart_id"])
    df_clean.write.mode("overwrite").parquet(f"{silver_path}/cart")

def process_cart_items():
    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}/cart_items")
    df_clean = df.dropna(subset=["cart_item_id", "cart_id", "product_id"]).dropDuplicates(["cart_item_id"])
    df_clean.write.mode("overwrite").parquet(f"{silver_path}/cart_items")

def process_browsing_history():
    df = spark.read.option("basePath", bronze_path).parquet(f"{bronze_path}/browsing_history")
    df_clean = df.dropna(subset=["customer_id", "product_id", "timestamp"]).dropDuplicates()
    df_clean = df_clean.withColumn("timestamp", to_date("timestamp"))
    df_clean.write.mode("overwrite").parquet(f"{silver_path}/browsing_history")

# Call all processing functions
process_customers()
process_products()
process_categories()
process_orders()
process_order_items()
process_reviews()
process_cart()
process_cart_items()
process_browsing_history()

spark.stop()
