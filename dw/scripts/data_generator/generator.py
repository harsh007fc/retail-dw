import os
import json
import pandas as pd
from faker import Faker
from datetime import datetime
from pathlib import Path
import random

fake = Faker()
TODAY = datetime.now().strftime("%Y-%m-%d")

# Directory setup
RAW_BASE = Path("dw/source_bucket/").resolve()
DATA_DIRS = {"csv": RAW_BASE / "csv", "json": RAW_BASE / "json"}
BOOKMARK_DIR = RAW_BASE / "bookmark"
BOOKMARK_DIR.mkdir(parents=True, exist_ok=True)

COUNTER_FILE = BOOKMARK_DIR / "pk_counters.json"
LATEST_FILE = BOOKMARK_DIR / "latest.json"

TABLES = [
    "customers",
    "products",
    "categories",
    "orders",
    "order_items",
    "cart",
    "cart_items",
    "reviews",
    "browsing_history",
]

# Persistent counters
if COUNTER_FILE.exists():
    with open(COUNTER_FILE) as f:
        pk_counters = json.load(f)
else:
    pk_counters = {t: 1 for t in TABLES}


def get_pk(table):
    val = pk_counters[table]
    pk_counters[table] += 1
    return val


def save_pk_counters():
    with open(COUNTER_FILE, "w") as f:
        json.dump(pk_counters, f, indent=2)


def load_latest_entities():
    if LATEST_FILE.exists():
        with open(LATEST_FILE) as f:
            data = json.load(f)
            return (
                data.get("customers", []),
                data.get("products", []),
                data.get("categories", []),
            )
    return [], [], []


def save_latest_entities(customers, products, categories):
    with open(LATEST_FILE, "w") as f:
        json.dump(
            {"customers": customers, "products": products, "categories": categories},
            f,
            indent=2,
        )


def get_run_number(table, date=TODAY):
    for fmt in DATA_DIRS:
        folder = DATA_DIRS[fmt] / table
        if not folder.exists():
            return 1
        existing = list(folder.glob(f"{date}_*.{fmt}"))
        return len(existing) + 1 if existing else 1


def write_data(table, data):
    run_number = get_run_number(table)
    for fmt, base_path in DATA_DIRS.items():
        dir_path = base_path / table
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"{TODAY}_{run_number}.{fmt}"
        df = pd.DataFrame(data)
        if fmt == "csv":
            df.to_csv(file_path, index=False)
        else:
            df.to_json(file_path, orient="records", indent=2)


# Global data containers
customer_data, product_data, category_data = load_latest_entities()
order_history = []


def generate_categories(n=2):
    global category_data
    new = []
    for _ in range(n):
        new.append(
            {
                "category_id": get_pk("categories"),
                "category_name": fake.word().capitalize(),
                "parent_category_id": None,
            }
        )
    category_data.extend(new)
    write_data("categories", new)


def generate_products(n=5):
    global product_data
    new = []
    for _ in range(n):
        new.append(
            {
                "product_id": get_pk("products"),
                "name": fake.word().capitalize(),
                "category_id": random.choice(category_data)["category_id"],
                "brand": fake.company(),
                "price": round(random.uniform(10, 1000), 2),
                "stock_quantity": random.randint(10, 100),
            }
        )
    product_data.extend(new)
    write_data("products", new)


def generate_customers(n=5):
    global customer_data
    new = []
    for _ in range(n):
        new.append(
            {
                "customer_id": get_pk("customers"),
                "name": fake.name(),
                "email": fake.unique.email(),
                "phone_number": fake.phone_number(),
                "address": fake.address().replace("\n", ", "),
                "registration_date": fake.date_time_between(
                    start_date="-2y", end_date="now"
                ).isoformat(),
            }
        )
    customer_data.extend(new)
    write_data("customers", new)


def generate_orders(n=15):
    global order_history
    orders = []
    for _ in range(n):
        cust = random.choice(customer_data)
        order = {
            "order_id": get_pk("orders"),
            "customer_id": cust["customer_id"],
            "order_date": datetime.now().isoformat(),
            "total_amount": 0.0,  # updated later
            "payment_method": random.choice(["Credit Card", "PayPal", "UPI", "COD"]),
            "shipping_address": cust["address"],
            "status": random.choice(["pending", "shipped", "delivered", "cancelled"]),
        }
        orders.append(order)
        order_history.append(order)
    write_data("orders", orders)
    return orders


def generate_order_items(orders):
    items = []
    for order in orders:
        total = 0
        for _ in range(random.randint(1, 4)):
            product = random.choice(product_data)
            qty = random.randint(1, 3)
            total += qty * product["price"]
            items.append(
                {
                    "order_item_id": get_pk("order_items"),
                    "order_id": order["order_id"],
                    "product_id": product["product_id"],
                    "quantity": qty,
                    "price": product["price"],
                }
            )
        order["total_amount"] = round(total, 2)
    write_data("order_items", items)
    write_data("orders", orders)  # update with totals


def generate_carts(n=8):
    carts = []
    for _ in range(n):
        cust = random.choice(customer_data)
        carts.append(
            {
                "cart_id": get_pk("cart"),
                "customer_id": cust["customer_id"],
                "created_date": datetime.now().isoformat(),
            }
        )
    write_data("cart", carts)
    return carts


def generate_cart_items(carts):
    items = []
    for cart in carts:
        for _ in range(random.randint(1, 3)):
            prod = random.choice(product_data)
            items.append(
                {
                    "cart_item_id": get_pk("cart_items"),
                    "cart_id": cart["cart_id"],
                    "product_id": prod["product_id"],
                    "quantity": random.randint(1, 4),
                }
            )
    write_data("cart_items", items)


def generate_reviews(n=10):
    reviews = []
    for _ in range(n):
        if not order_history:
            continue
        order = random.choice(order_history)
        reviews.append(
            {
                "review_id": get_pk("reviews"),
                "customer_id": order["customer_id"],
                "product_id": random.choice(product_data)["product_id"],
                "rating": random.randint(1, 5),
                "comment": fake.sentence(),
                "review_date": datetime.now().isoformat(),
            }
        )
    write_data("reviews", reviews)


def generate_browsing_history(n=20):
    history = []
    for _ in range(n):
        history.append(
            {
                "history_id": get_pk("browsing_history"),
                "customer_id": random.choice(customer_data)["customer_id"],
                "product_id": random.choice(product_data)["product_id"],
                "viewed_at": fake.date_time_this_month().isoformat(),
            }
        )
    write_data("browsing_history", history)


def main():
    generate_categories()
    generate_products()
    generate_customers()
    orders = generate_orders()
    generate_order_items(orders)
    carts = generate_carts()
    generate_cart_items(carts)
    generate_reviews()
    generate_browsing_history()
    save_pk_counters()
    save_latest_entities(customer_data, product_data, category_data)


if __name__ == "__main__":
    main()
    print("data generated successfully ✅ ")
