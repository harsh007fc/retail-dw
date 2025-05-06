import csv
import json
import random
from datetime import datetime, timedelta
from faker import Faker
from faker.providers import BaseProvider
import math
import os

# Custom providers
class EcommerceProvider(BaseProvider):
    def order_status(self):
        statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled', 'Returned']
        weights = [0.1, 0.2, 0.3, 0.3, 0.05, 0.05]
        return random.choices(statuses, weights=weights, k=1)[0]
        # return self.random_element(elements=statuses, weights=weights)
    
    def payment_method(self):
        methods = ['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Google Pay', 'Bank Transfer']
        return self.random_element(methods)
    
    def product_category(self):
        main_categories = [
            ('Electronics', None),
            ('Clothing', None),
            ('Home & Kitchen', None),
            ('Books', None),
            ('Sports & Outdoors', None),
            ('Beauty', None),
            ('Toys & Games', None),
            ('Grocery', None)
        ]
        return self.random_element(main_categories)
    
    def review_comment(self, rating):
        if rating >= 4:
            return self.random_element([
                "Absolutely love it! Exceeded my expectations.",
                "Perfect in every way. Worth every penny!",
                "Highly recommend! Excellent quality."
            ])
        elif rating == 3:
            return self.random_element([
                "Good product overall, but has some minor issues.",
                "Works as expected, nothing special though."
            ])
        else:
            return self.random_element([
                "Very disappointed with this purchase.",
                "Poor quality and doesn't work as described."
            ])

# Initialize Faker
fake = Faker()
fake.add_provider(EcommerceProvider)

def generate_data(start_date, end_date):
    # Convert dates
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    date_range = (end - start).days
    
    # ======================
    # 1. Generate Customers
    # ======================
    print("Generating customers...")
    customers = []
    num_customers = random.randint(500, 1000)
    
    for customer_id in range(1, num_customers + 1):
        reg_days = random.randint(0, date_range)
        customers.append({
            'customer_id': customer_id,
            'name': fake.name(),
            'email': fake.email(),
            'phone_number': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'registration_date': (start + timedelta(days=reg_days)).strftime('%Y-%m-%d')
        })
    
    # =====================
    # 2. Generate Categories
    # =====================
    print("Generating categories...")
    categories = []
    main_categories = []
    num_main_categories = 8
    num_subcategories = 20
    
    # Main categories
    for cat_id in range(1, num_main_categories + 1):
        cat_name, _ = fake.product_category()
        categories.append({
            'category_id': cat_id,
            'category_name': cat_name,
            'parent_category_id': None
        })
        main_categories.append(cat_id)
    
    # Subcategories
    for cat_id in range(num_main_categories + 1, num_main_categories + num_subcategories + 1):
        parent_id = random.choice(main_categories)
        parent_name = next(c['category_name'] for c in categories if c['category_id'] == parent_id)
        
        if parent_name == 'Electronics':
            sub_name = fake.random_element(['Smartphones', 'Laptops', 'TVs', 'Headphones'])
        elif parent_name == 'Clothing':
            sub_name = fake.random_element(['Men', 'Women', 'Kids', 'Accessories'])
        else:
            sub_name = fake.word().capitalize() + " " + fake.word().capitalize()
        
        categories.append({
            'category_id': cat_id,
            'category_name': f"{parent_name} - {sub_name}",
            'parent_category_id': parent_id
        })
    
    # ==================
    # 3. Generate Products
    # ==================
    print("Generating products...")
    products = []
    num_products = random.randint(1000, 2000)
    
    for product_id in range(1, num_products + 1):
        category = random.choice(categories)
        category_id = category['category_id']
        
        # Determine brand and name based on category
        if category['parent_category_id'] == 1 or category_id == 1:  # Electronics
            brand = fake.random_element(['Apple', 'Samsung', 'Sony', 'LG'])
            name = fake.random_element(['Smartphone', 'Laptop', 'TV', 'Headphones']) + " " + fake.random_element(['Pro', 'Max', 'Ultra', ''])
        elif category['parent_category_id'] == 2 or category_id == 2:  # Clothing
            brand = fake.random_element(['Nike', 'Adidas', 'Levi\'s', 'H&M'])
            name = fake.random_element(['T-Shirt', 'Jeans', 'Jacket', 'Dress'])
        else:
            brand = fake.company()
            name = fake.catch_phrase()
        
        products.append({
            'product_id': product_id,
            'name': name.strip(),
            'category_id': category_id,
            'brand': brand,
            'price': round(random.uniform(5, 500), 2),
            'stock_quantity': random.randint(0, 1000)
        })
    
    # ==================
    # 4. Generate Orders
    # ==================
    print("Generating orders...")
    orders = []
    num_orders = random.randint(3000, 5000)
    
    for order_id in range(1, num_orders + 1):
        customer = random.choice(customers)
        order_days = random.randint(0, date_range)
        
        orders.append({
            'order_id': order_id,
            'customer_id': customer['customer_id'],
            'order_date': (start + timedelta(days=order_days)).strftime('%Y-%m-%d %H:%M:%S'),
            'total_amount': 0,  # Will be calculated after order items
            'payment_method': fake.payment_method(),
            'shipping_address': customer['address'],
            'status': fake.order_status()
        })
    
    # ======================
    # 5. Generate Order Items
    # ======================
    print("Generating order items...")
    order_items = []
    order_item_id = 1
    
    for order in orders:
        num_items = random.randint(1, 10)
        order_total = 0
        
        for _ in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 5)
            price = product['price'] * (0.9 if random.random() > 0.8 else 1.0)  # 20% chance of discount
            order_total += price * quantity
            
            order_items.append({
                'order_item_id': order_item_id,
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'price': round(price, 2)
            })
            order_item_id += 1
        
        # Update order total
        order['total_amount'] = round(order_total, 2)
    
    # =================
    # 6. Generate Reviews
    # =================
    print("Generating reviews...")
    reviews = []
    num_reviews = random.randint(4000, 8000)
    
    for review_id in range(1, num_reviews + 1):
        customer = random.choice(customers)
        product = random.choice(products)
        review_days = random.randint(0, date_range)
        # Normal distribution centered at 4.2
        rating = min(5, max(1, int(random.gauss(4.2, 1.2))))
        

        
        reviews.append({
            'review_id': review_id,
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'rating': rating,
            'comment': fake.review_comment(rating),
            'review_date': (start + timedelta(days=review_days)).strftime('%Y-%m-%d')
        })
    
    # ==============
    # 7. Generate Carts
    # ==============
    print("Generating carts...")
    carts = []
    num_carts = random.randint(2000, 4000)
    
    for cart_id in range(1, num_carts + 1):
        customer = random.choice(customers)
        cart_days = random.randint(0, date_range)
        
        carts.append({
            'cart_id': cart_id,
            'customer_id': customer['customer_id'],
            'created_date': (start + timedelta(days=cart_days)).strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # ==================
    # 8. Generate Cart Items
    # ==================
    print("Generating cart items...")
    cart_items = []
    cart_item_id = 1
    
    for cart in carts:
        num_items = random.randint(1, 8)
        
        for _ in range(num_items):
            product = random.choice(products)
            
            cart_items.append({
                'cart_item_id': cart_item_id,
                'cart_id': cart['cart_id'],
                'product_id': product['product_id'],
                'quantity': random.randint(1, 3)
            })
            cart_item_id += 1
    
    # =========================
    # 9. Generate Browsing History
    # =========================
    print("Generating browsing history...")
    browsing_history = []
    num_history = random.randint(15000, 30000)
    
    for history_id in range(1, num_history + 1):
        customer = random.choice(customers)
        product = random.choice(products)
        view_days = min(date_range, max(0, int(random.expovariate(1/(date_range/3)))))  # More recent views
        
        browsing_history.append({
            'history_id': history_id,
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'viewed_at': (end - timedelta(days=view_days)).strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return {
        'customers': customers,
        'categories': categories,
        'products': products,
        'orders': orders,
        'order_items': order_items,
        'reviews': reviews,
        'carts': carts,
        'cart_items': cart_items,
        'browsing_history': browsing_history
    }


def save_data(data, format='both'):
    """Save data in CSV and/or JSON format with the desired folder structure"""
    
    # Loop through each table and save the data
    for table_name, records in data.items():
        # Create the table-specific subdirectories
        json_folder = os.path.join('dw/source_bucket/raw/json', table_name)
        csv_folder = os.path.join('dw/source_bucket/raw/csv', table_name)
        
        # Create directories if they don't exist
        if not os.path.exists(json_folder):
            os.makedirs(json_folder)
        if not os.path.exists(csv_folder):
            os.makedirs(csv_folder)
        
        # CSV File saving
        if format in ('csv', 'both'):
            # Check for existing files and increment file names
            file_name = f'{table_name}.csv'
            file_path = os.path.join(csv_folder, file_name)
            counter = 1
            
            # Find the next available file name
            while os.path.exists(file_path):
                file_path = os.path.join(csv_folder, f'{table_name}_{str(counter).zfill(2)}.csv')
                counter += 1
            
            # Save the CSV file
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=records[0].keys())
                writer.writeheader()
                writer.writerows(records)
            print(f"CSV file saved as: {file_path}")
        
        # JSON File saving
        if format in ('json', 'both'):
            # Check for existing files and increment file names
            file_name = f'{table_name}.json'
            file_path = os.path.join(json_folder, file_name)
            counter = 1
            
            # Find the next available file name
            while os.path.exists(file_path):
                file_path = os.path.join(json_folder, f'{table_name}_{str(counter).zfill(2)}.json')
                counter += 1
            
            # Save the JSON file
            with open(file_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(records, jsonfile, indent=2)
            print(f"JSON file saved as: {file_path}")
    
    print(f"Data saved in {format.upper()} format")

def main():
        # Configuration
    start_date = '2023-01-01'
    end_date = '2023-12-31'
    
    # Generate data
    data = generate_data(start_date, end_date)
    
    # Save data in both formats
    save_data(data, format='both')


if __name__ == '__main__':
    main()
