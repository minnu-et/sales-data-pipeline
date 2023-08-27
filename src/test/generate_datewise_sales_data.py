import os
import csv
import random
from datetime import datetime

# -----------------------------
# Project paths (Linux / WSL)
# -----------------------------
current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
project_root = os.path.dirname(os.path.dirname(current_dir))

data_dir = os.path.join(project_root, "resources", "data")

# -----------------------------
# Helper function
# -----------------------------
def load_column_from_csv(csv_path, column_name):
    values = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row[column_name]:
                values.append(row[column_name])
    return values

# -----------------------------
# Load dimension data
# -----------------------------
customer_csv = os.path.join(data_dir, "customer.csv")
product_csv = os.path.join(data_dir, "product.csv")
store_csv = os.path.join(data_dir, "store.csv")

customer_ids = load_column_from_csv(customer_csv, "customer_id")
store_ids = load_column_from_csv(store_csv, "store_id")
product_names = load_column_from_csv(product_csv, "product_name")

# Cast numeric IDs
customer_ids = [int(x) for x in customer_ids]
store_ids = [int(x) for x in store_ids]

# -----------------------------
# Sales persons per store
# -----------------------------
sales_persons = {
    store_id: list(range(store_id * 10, store_id * 10 + 5))
    for store_id in store_ids
}

# -----------------------------
# Output path
# -----------------------------
output_dir = data_dir
os.makedirs(output_dir, exist_ok=True)

input_date_str = input("Enter the date for which you want to generate (YYYY-MM-DD): ")
sales_date = datetime.strptime(input_date_str, "%Y-%m-%d").date()

csv_file_path = os.path.join(output_dir, "sales_data.csv")

# -----------------------------
# CSV generation
# -----------------------------
with open(csv_file_path, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)

    writer.writerow([
        "customer_id",
        "store_id",
        "product_name",
        "sales_date",
        "sales_person_id",
        "price",
        "quantity",
        "total_cost"
    ])

    for _ in range(400_000):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(product_names)

        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        price = round(random.uniform(20, 300), 2)
        total_cost = round(price * quantity, 2)

        writer.writerow([
            customer_id,
            store_id,
            product_name.lower().strip(),
            sales_date.strftime("%Y-%m-%d"),
            sales_person_id,
            price,
            quantity,
            total_cost
        ])

print("✅ sales_data.csv generated at:", csv_file_path)
