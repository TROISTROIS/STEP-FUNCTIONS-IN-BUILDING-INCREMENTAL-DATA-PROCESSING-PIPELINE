from faker import Faker
import datetime
import csv
import random
from random import randint, choice

fake = Faker()

transactions_per_day = 60
current_date = datetime.date.today()

transaction_ids = ['T90951', 'T46381', 'T42830', 'T69066', 'T17678', 'T08368', 'T60539', 'T15900', 'T62731', 'T03704', 'T03502', 'T57962', 'T55864', 'T67196', 'T24415', 'T10994', 'T59403', 'T13845', 'T67422', 'T17258', 'T12959', 'T79885', 'T44885', 'T46911', 'T41866', 'T62019', 'T83877', 'T27727', 'T81469', 'T03862', 'T27745', 'T65551', 'T48998', 'T49725',
                   'T72177', 'T49215', 'T83094', 'T35782', 'T25122', 'T29701', 'T50556', 'T92483', 'T52305', 'T17195', 'T41284', 'T13488', 'T07806', 'T81555', 'T43515', 'T68107', 'T17300', 'T76272', 'T15047', 'T48828', 'T32657', 'T14822', 'T40711', 'T25146', 'T32152', 'T72392', 'T06113', 'T94476', 'T06648', 'T52904', 'T72038', 'T17146', 'T57551', 'T73906', 'T17447', 'T61636', 'T13688', 'T45516', 'T10572', 'T59879', 'T93091', 'T54660', 'T47603', 'T60851', 'T89853', 'T40171', 'T27233', 'T05374', 'T49192', 'T27324', 'T97603', 'T62358', 'T71462', 'T77679', 'T78989', 'T33292', 'T28198', 'T29068', 'T28416', 'T59348', 'T86443', 'T58542', 'T56610', 'T92734', 'T68371', 'T98150']
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

def generate_data(timestamp):
    transactions = generate_transactions(transactions_per_day, current_date)
    write_to_csv(transactions, f"/tmp/sales_{timestamp}.csv")
    print(f"Generated mock sales data sales_{timestamp}.csv and saved in s3 bucket")
    return

