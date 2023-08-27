import csv
import os
import random
from datetime import datetime, timedelta

# -----------------------------
# Path resolution (UNCHANGED)
# -----------------------------
current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
project_root = os.path.dirname(os.path.dirname(current_dir))
data_dir = os.path.join(project_root, "resources", "data")
os.makedirs(data_dir, exist_ok=True)

csv_file_path = os.path.join(data_dir, "sales_data.csv")

# -----------------------------
# Reference data
# -----------------------------
customer_ids = list(range(1, 21))
store_ids = list(range(121, 124))

product_data = {
    "quaker oats": 212,
    "sugar": 50,
    "maida": 20,
    "besan": 52,
    "refined oil": 110,
    "clinic plus": 1.5,
    "dantkanti": 100,
    "nutrella": 40
}

sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9]
}

start_date = datetime(2023, 3, 3)
end_date = datetime(2023, 8, 20)

# -----------------------------
# CSV generation
# -----------------------------
TOTAL_ROWS = 500
BAD_DATA_RATIO = 0.15   # 15% bad rows

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

    for i in range(TOTAL_ROWS):

        is_bad_row = random.random() < BAD_DATA_RATIO

        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days)
        )
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        price = product_data[product_name]

        # -----------------------------
        # Introduce BAD DATA intentionally
        # -----------------------------
        if is_bad_row:
            bad_type = random.choice([
                "null_customer",
                "null_store",
                "null_date",
                "negative_price",
                "zero_quantity"
            ])

            if bad_type == "null_customer":
                customer_id = None

            elif bad_type == "null_store":
                store_id = None
                sales_person_id = None

            elif bad_type == "null_date":
                sales_date = None

            elif bad_type == "negative_price":
                price = -price

            elif bad_type == "zero_quantity":
                quantity = 0

        total_cost = (
            price * quantity
            if price is not None and quantity is not None
            else None
        )

        writer.writerow([
            customer_id,
            store_id,
            product_name,
            sales_date.strftime("%Y-%m-%d") if sales_date else None,
            sales_person_id,
            price,
            quantity,
            total_cost
        ])

print(f"CSV file generated successfully at: {csv_file_path}")

