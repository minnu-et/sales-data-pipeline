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

csv_file_path = os.path.join(data_dir, "store.csv")

# -----------------------------
# Reference data
# -----------------------------
cities = [
    ("Bangalore", "KA"),
    ("Chennai", "TN"),
    ("Hyderabad", "TS"),
    ("Mumbai", "MH"),
    ("Pune", "MH"),
    ("Delhi", "DL"),
    ("Kochi", "KL"),
    ("Trivandrum", "KL")
]

store_names = [
    "Central Store",
    "City Mart",
    "Daily Needs",
    "Fresh Hub",
    "Value Bazaar",
    "Smart Shop",
    "Mega Store",
    "Urban Mart"
]

manager_names = [
    "Ramesh Kumar",
    "Suresh Nair",
    "Anita Sharma",
    "Priya Menon",
    "Rahul Verma",
    "Sunita Das"
]

# -----------------------------
# Helper functions
# -----------------------------
def random_date(start_days_ago=3000):
    return datetime.today().date() - timedelta(days=random.randint(0, start_days_ago))


def maybe_invalid(value, probability=0.1):
    """Injects invalid values with given probability"""
    return None if random.random() < probability else value


def random_pincode():
    # Indian-style pincodes, sometimes invalid
    if random.random() < 0.1:
        return random.choice(["ABCDE", "123", None])
    return random.randint(100000, 999999)

# -----------------------------
# CSV generation
# -----------------------------
with open(csv_file_path, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)

    writer.writerow([
        "store_id",
        "store_name",
        "address",
        "city",
        "state",
        "pincode",
        "store_manager_name",
        "store_opening_date",
        "store_closing_date",
        "reviews"
    ])

    store_id_start = 201

    for i in range(60):  # ~60 stores (dimension-sized)
        store_id = maybe_invalid(store_id_start + (i % 40), 0.05)  # duplicates on purpose
        store_name = maybe_invalid(random.choice(store_names), 0.05)

        city, state = random.choice(cities)
        address = f"{random.randint(10, 999)}, Main Road"

        pincode = random_pincode()
        manager = maybe_invalid(random.choice(manager_names), 0.1)

        opening_date = random_date(4000)

        # Introduce invalid closing dates occasionally
        if random.random() < 0.1:
            closing_date = opening_date - timedelta(days=random.randint(1, 500))  # invalid
        elif random.random() < 0.2:
            closing_date = None
        else:
            closing_date = opening_date + timedelta(days=random.randint(500, 3000))

        reviews = maybe_invalid(
            random.choice(["Good", "Average", "Excellent", "Needs Improvement"]),
            0.2
        )

        writer.writerow([
            store_id,
            store_name,
            address,
            city,
            state,
            pincode,
            manager,
            opening_date,
            closing_date,
            reviews
        ])

print(f"✅ store.csv generated at: {csv_file_path}")
