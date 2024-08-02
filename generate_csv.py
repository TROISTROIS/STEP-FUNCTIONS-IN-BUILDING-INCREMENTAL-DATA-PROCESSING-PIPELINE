from faker import Faker
import datetime
import csv
import random
from random import randint, choice

fake = Faker()

transactions_per_day = 76
current_date = datetime.date.today()

transaction_ids = [fake.unique.bothify("T#####") for _ in range(transactions_per_day)]
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
    if customer_id not in transaction_information:
        transaction_information[customer_id] = {
        }
    transaction_data = transaction_information[customer_id].copy()
    transaction_data["CustomerID"] = customer_id
    transaction_data["TransactionID"] = random.choice(transaction_ids)
    transaction_data["Date"] = str(current_date)
    transaction_data["ProductID"] = random.choice(product_ids)
    transaction_data["Quantity"] = random.randint(1, 5)
    price = get_product_price(transaction_data["ProductID"])
    transaction_data["Price"] = round(price * transaction_data["Quantity"],2)
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

def generate_data():
    transactions = generate_transactions(transactions_per_day, current_date)
    write_to_csv(transactions, f"/tmp/sales.csv")
    print(f"Generated mock sales data sales.csv and saved in s3 bucket")
    return