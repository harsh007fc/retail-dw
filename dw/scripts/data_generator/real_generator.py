import os
import json
import pandas as pd
from faker import Faker
from datetime import datetime
from pathlib import Path
import random

fake = Faker()
TODAY = datetime.now().strftime("%Y-%m-%d")

# Directory structure
# BASE_DIR = Path(__file__).resolve().parent
RAW_BASE = Path("../dw/source_bucket/raw").resolve()
DATA_DIRS = {
    "csv": RAW_BASE / "csv",
    "json": RAW_BASE / "json"
}
BOOKMARK_DIR = RAW_BASE / "bookmark"
BOOKMARK_DIR.mkdir(parents=True, exist_ok=True)
COUNTER_FILE = BOOKMARK_DIR / "pk_counters.json"

TABLES = ['customers', 'products', 'categories', 'orders', 'order_items', 'cart', 'cart_items', 'reviews', 'browsing_history']

# Persistent counters for primary keys
if COUNTER_FILE.exists():
    with open(COUNTER_FILE) as f:
        pk_counters = json.load(f)
else:
    pk_counters = {t: 1 for t in TABLES}

def get_pk(table_name):
    val = pk_counters[table_name]
    pk_counters[table_name] += 1
    return val

def save_pk_counters():
    with open(COUNTER_FILE, 'w') as f:
        json.dump(pk_counters, f)

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
        if fmt == 'csv':
            df.to_csv(file_path, index=False)
        else:
            df.to_json(file_path, orient='records', indent=2)

# Reference data
category_data = []
product_data = []
customer_data = []

def generate_categories(n=5):
    global category_data
    category_data = [{
        'category_id': get_pk('categories'),
        'category_name': fake.word().capitalize(),
        'parent_category_id': None
    } for _ in range(n)]
    write_data('categories', category_data)

def generate_products(n=20):
    global product_data
    product_data = [{
        'product_id': get_pk('products'),
        'name': fake.word().capitalize(),
        'category_id': random.choice(category_data)['category_id'],
        'brand': fake.company(),
        'price': round(random.uniform(10, 500), 2),
        'stock_quantity': random.randint(1, 100)
    } for _ in range(n)]
    write_data('products', product_data)

def generate_customers(n=10):
    global customer_data
    customer_data = [{
        'customer_id': get_pk('customers'),
        'name': fake.name(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'address': fake.address().replace('\n', ', '),
        'registration_date': fake.date_time_this_year().isoformat()
    } for _ in range(n)]
    write_data('customers', customer_data)

def generate_orders(n=15):
    orders = [{
        'order_id': get_pk('orders'),
        'customer_id': random.choice(customer_data)['customer_id'],
        'order_date': datetime.now().isoformat(),
        'total_amount': 0.0,
        'payment_method': random.choice(['Credit Card', 'PayPal', 'COD']),
        'shipping_address': fake.address().replace('\n', ', '),
        'status': random.choice(['pending', 'shipped', 'delivered'])
    } for _ in range(n)]
    write_data('orders', orders)
    return orders

def generate_order_items(orders):
    order_items = []
    for order in orders:
        num_items = random.randint(1, 5)
        total = 0
        for _ in range(num_items):
            product = random.choice(product_data)
            qty = random.randint(1, 3)
            price = product['price']
            total += price * qty
            order_items.append({
                'order_item_id': get_pk('order_items'),
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': qty,
                'price': price
            })
        order['total_amount'] = round(total, 2)
    write_data('order_items', order_items)
    write_data('orders', orders)

def generate_cart(n=8):
    carts = [{
        'cart_id': get_pk('cart'),
        'customer_id': random.choice(customer_data)['customer_id'],
        'created_date': datetime.now().isoformat()
    } for _ in range(n)]
    write_data('cart', carts)
    return carts

def generate_cart_items(carts):
    cart_items = []
    for cart in carts:
        for _ in range(random.randint(1, 4)):
            product = random.choice(product_data)
            cart_items.append({
                'cart_item_id': get_pk('cart_items'),
                'cart_id': cart['cart_id'],
                'product_id': product['product_id'],
                'quantity': random.randint(1, 5)
            })
    write_data('cart_items', cart_items)

def generate_reviews(n=20):
    reviews = [{
        'review_id': get_pk('reviews'),
        'customer_id': random.choice(customer_data)['customer_id'],
        'product_id': random.choice(product_data)['product_id'],
        'rating': random.randint(1, 5),
        'comment': fake.sentence(),
        'review_date': datetime.now().isoformat()
    } for _ in range(n)]
    write_data('reviews', reviews)

def generate_browsing_history(n=30):
    history = [{
        'history_id': get_pk('browsing_history'),
        'customer_id': random.choice(customer_data)['customer_id'],
        'product_id': random.choice(product_data)['product_id'],
        'viewed_at': datetime.now().isoformat()
    } for _ in range(n)]
    write_data('browsing_history', history)

def main():
    generate_categories()
    generate_products()
    generate_customers()
    orders = generate_orders()
    generate_order_items(orders)
    carts = generate_cart()
    generate_cart_items(carts)
    generate_reviews()
    generate_browsing_history()
    save_pk_counters()

if __name__ == '__main__':
    main()
