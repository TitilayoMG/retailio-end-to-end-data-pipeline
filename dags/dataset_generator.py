from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta


fake = Faker()

# Generate Customers Data
regions = {
    "North": ["Manchester", "Leeds", "Liverpool"],
    "South": ["London", "Brighton", "Bristol"],
    "East": ["Norwich", "Cambridge", "Ipswich"],
    "West": ["Birmingham", "Bristol", "Cardiff"]
}

store_types = ["mall", "standalone", "online"]

def generate_customers(n=10000):
    customers = []

    for i in range(n):
        region = random.choice(list(regions.keys()))
        city = random.choice(regions[region])

        customers.append({
            "customer_id": f"CUST_{i}",
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "signup_date": fake.date_between(start_date='-2y'),
            "branch_id": random.randint(1, 50),
            "branch_city": city,
            "region": region,
            "store_type": random.choice(store_types)
        })

    return pd.DataFrame(customers)


# Generate Products Data
categories = {
    "Electronics": ["Phones", "Laptops"],
    "Fashion": ["Men", "Women"],
    "Groceries": ["Beverages", "Snacks"]
}

brands = ["Samsung", "Apple", "Nike", "Adidas", "Nestle"]

def generate_products(n=2000):
    products = []

    for i in range(n):
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])

        price = round(random.uniform(5, 500), 2)
        discount = random.choice([0, 5, 10, 20])

        products.append({
            "product_id": f"PROD_{i}",
            "name": fake.word().capitalize(),
            "category": category,
            "subcategory": subcategory,
            "brand": random.choice(brands),
            "price": price,
            "discount_percent": discount
        })

    return pd.DataFrame(products)


# Generate Sales Data
def generate_sales(customers_df, products_df, n=500_000):
    sales = []

    customer_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()

    for i in range(n):
        customer_id = random.choice(customer_ids)

        # Repeat purchases bias
        if random.random() < 0.6:
            customer_id = random.choice(customer_ids[:2000])

        # Date with weekend spike
        base_date = datetime.now() - timedelta(days=random.randint(0, 365))
        if base_date.weekday() >= 5:  # weekend
            quantity = random.randint(2, 5)
        else:
            quantity = random.randint(1, 3)

        # Multi-product purchase
        num_items = random.randint(1, 5)

        for _ in range(num_items):
            product_id = random.choice(product_ids)

            sales.append({
                "sale_id": f"SALE_{i}",
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": quantity,
                "channel": random.choice(["online", "in-store"]),
                "purchase_date": base_date
            })

    return pd.DataFrame(sales)


#
# customers_df = generate_customers(10000)
# products_df = generate_products(2000)
# sales_df = generate_sales(customers_df, products_df, 50000)

# upload_to_minio(customers_df, "landing/customers/customers.csv")
# upload_to_minio(products_df, "landing/products/products.csv")
# upload_to_minio(sales_df, "landing/sales/sales.csv")


