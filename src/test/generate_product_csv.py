import csv
import os
import random
from datetime import datetime, timedelta

# -----------------------------
# Path resolution (STANDARD)
# -----------------------------
current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
project_root = os.path.dirname(os.path.dirname(current_dir))

data_dir = os.path.join(project_root, "resources", "data")
os.makedirs(data_dir, exist_ok=True)

csv_file_path = os.path.join(data_dir, "product.csv")

# -----------------------------
# Reference data
# -----------------------------
categories = ["Food", "Personal Care", "Household", "Beverages"]
brands = ["Quaker", "Dabur", "Patanjali", "Nestle", "Generic"]

product_names = [
    "Quaker Oats",
    "Sugar",
    "Maida",
    "Besan",
    "Refined Oil",
    "Clinic Plus",
    "Dantkanti",
    "Nutrella",
    "Green Tea",
    "Coffee Powder",
    "Toothpaste",
    "Dish Soap",
    "Floor Cleaner",
    "Shampoo",
    "Cooking Salt",
    "Spices Mix",
    "Energy Drink",
    "Fruit Juice",
    "Milk Powder",
    "Butter"
]

# -----------------------------
# Helper functions
# -----------------------------
def random_date(start_days_ago=1000):
    return datetime.today().date() - timedelta(days=random.randint(0, start_days_ago))


def maybe_invalid(value, probability=0.1):
    """Injects invalid values with given probability"""
    return None if random.random() < probability else value


# -----------------------------
# CSV generation
# -----------------------------
with open(csv_file_path, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)

    writer.writerow([
        "product_id",
        "product_name",
        "category",
        "brand",
        "current_price",
        "old_price",
        "created_date",
        "updated_date",
        "expiry_date",
        "is_active"
    ])

    product_id_start = 1001

    for i, product in enumerate(product_names):
        product_id = maybe_invalid(product_id_start + i, 0.05)
        product_name = maybe_invalid(product, 0.05)
        category = random.choice(categories)
        brand = random.choice(brands)

        current_price = maybe_invalid(round(random.uniform(20, 300), 2), 0.05)
        old_price = (
            current_price - round(random.uniform(1, 20), 2)
            if current_price and random.random() > 0.1
            else None
        )

        created_date = random_date(1500)
        updated_date = created_date + timedelta(days=random.randint(0, 300))
        expiry_date = (
            created_date - timedelta(days=random.randint(1, 100))
            if random.random() < 0.05  # invalid case
            else created_date + timedelta(days=random.randint(200, 1500))
        )

        is_active = maybe_invalid(random.choice([True, False]), 0.05)

        writer.writerow([
            product_id,
            product_name,
            category,
            brand,
            current_price,
            old_price,
            created_date,
            updated_date,
            expiry_date,
            is_active
        ])

print(f"✅ product.csv generated at: {csv_file_path}")
