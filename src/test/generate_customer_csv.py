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

csv_file_path = os.path.join(data_dir, "customer.csv")

# -----------------------------
# Reference data
# -----------------------------
first_names = ["John", "Anita", "Rahul", "Priya", "Sunita", "Amit", "Neha", "Kiran"]
last_names = ["Sharma", "Verma", "Nair", "Menon", "Das", "Iyer", "Patel"]

cities = [
    ("Bangalore", "KA"),
    ("Chennai", "TN"),
    ("Hyderabad", "TS"),
    ("Mumbai", "MH"),
    ("Delhi", "DL"),
    ("Kochi", "KL")
]

genders = ["M", "F", "O"]

# -----------------------------
# Helper functions
# -----------------------------
def random_date(start_year=1950, end_year=2015):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def maybe_invalid(value, probability=0.1):
    return None if random.random() < probability else value


def random_email(first, last):
    if random.random() < 0.15:
        return "invalid_email"  # bad email
    return f"{first.lower()}.{last.lower()}@example.com"


def random_phone():
    if random.random() < 0.15:
        return "12345"  # invalid phone
    return str(random.randint(6000000000, 9999999999))


# -----------------------------
# CSV generation
# -----------------------------
with open(csv_file_path, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)

    writer.writerow([
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone_number",
        "date_of_birth",
        "gender",
        "address",
        "city",
        "state",
        "pincode",
        "created_date",
        "updated_date"
    ])

    customer_id_start = 5001

    for i in range(60):
        first = random.choice(first_names)
        last = random.choice(last_names)
        city, state = random.choice(cities)

        created_date = random_date(2015, 2022)
        updated_date = created_date + timedelta(days=random.randint(0, 500))

        # Introduce bad updated_date cases
        if random.random() < 0.1:
            updated_date = created_date - timedelta(days=random.randint(1, 100))

        dob = random_date(1950, 2015)
        if random.random() < 0.05:
            dob = datetime.today() + timedelta(days=10)  # future DOB (invalid)

        writer.writerow([
            maybe_invalid(customer_id_start + (i % 45), 0.05),  # duplicates + nulls
            maybe_invalid(first, 0.05),
            maybe_invalid(last, 0.05),
            random_email(first, last),
            random_phone(),
            dob.date(),
            random.choice(genders),
            f"{random.randint(10, 999)} Main Road",
            city,
            state,
            random.randint(100000, 999999),
            created_date.date(),
            updated_date.date()
        ])

print(f"✅ customer.csv generated at: {csv_file_path}")
