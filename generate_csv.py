from faker import Faker
import datetime
import csv
import random
from random import randint, choice

fake = Faker()

transactions_per_day = 60
current_date = datetime.date.today()

transaction_ids = [f'T{str(i).zfill(5)}' for i in range(1, 101)]  # Generate T00001 to T00100
used_transaction_ids = set()  # To track used transaction IDs
product_ids = [fake.unique.bothify("P####") for _ in range(30)]
customer_ids = [fake.unique.bothify("C####") for _ in range(50)]
product_with_prices = {product: round(random.uniform(20, 100), 2) for product in product_ids}
store_location = [fake.unique.state() for _ in range (20)]

def get_product_price(product_id):
    # retrieve the price for a given product ID from the pre-defined dictionary"""
    if product_id in product_with_prices:
        return product_with_prices[product_id]
    else:
        return 0.00

transaction_information = {}

def generate_one_transaction(customer_id, current_date):
    transaction_data = {}
    transaction_data["CustomerID"] = customer_id
    # Ensure unique TransactionID
    while True:
        transaction_id = random.choice(transaction_ids)
        if transaction_id not in used_transaction_ids:
            used_transaction_ids.add(transaction_id)
            transaction_data["TransactionID"] = transaction_id
            break
    transaction_data["Date"] = str(current_date)
    transaction_data["ProductID"] = random.choice(product_ids)
    transaction_data["Quantity"] = random.randint(1, 5)
    price = get_product_price(transaction_data["ProductID"])
    transaction_data["Price"] = round(price * transaction_data["Quantity"], 2)
    transaction_data["StoreLocation"] = random.choice(store_location)

    return transaction_data

def generate_transactions(num_transactions, current_date):
    transactions = []
    for _ in range(num_transactions):
        transactions.append(generate_one_transaction(choice(customer_ids), current_date))
    return transactions

# testing
# data = generate_transactions(num_transactions, current_date)

def write_to_csv(data, filename):
    with open(filename, "w", newline="") as file:
        fieldnames = ["CustomerID","TransactionID","Date", "ProductID", "Quantity", "Price", "StoreLocation"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def generate_data(timestamp):
    transactions = generate_transactions(transactions_per_day, current_date)
    write_to_csv(transactions, f"/tmp/sales_{timestamp}.csv")
    print(f"Generated mock sales data sales_{timestamp}.csv and saved in s3 bucket")
    return

