import json
import boto3
import psycopg2
import csv
from botocore.exceptions import ClientError
import logging


def get_secret():
    try:
        secret_name = "redshift!step-functions-redshift-cluster-assgmnt-awsuser"
        region_name = "us-east-1"

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )

            secret = get_secret_value_response['SecretString']
            return secret

        except ClientError as e:
            logging.error(f"Error retrieving secret: {e}")
            raise

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise


def lambda_handler(event, context):
    print("***** EVENT ****", event)

    try:
        bucket = event['detail']['bucket']['name']
        key = event['detail']['object']['key']

        print(f"Bucket name: ", bucket)
        print(f"File name: ", key)

        # s3 boto3 client
        s3 = boto3.client('s3')
        # get the CSV file
        data = s3.get_object(Bucket=bucket, Key=key)
        data = data['Body'].read().decode("utf-8")
        # get the secrete values
        secret = json.loads(get_secret())

    except Exception as e:
        print("Error getting secret:", e)
        raise e

    # redshift connectivity
    try:
        conn = psycopg2.connect(
            dbname='transactions_db',
            host='step-functions-redshift-cluster-assgmnt.cyy1gmfmb9hv.us-east-1.redshift.amazonaws.com',
            port=5439,
            user=secret['username'],
            password=secret['password']
        )


    except Exception as e:
        print("Connection error:", e)

    try:
        cursor = conn.cursor()
        # Create staging table
        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS transactions_db.public.staging_fact_transactions (
            CustomerID VARCHAR(10) ENCODE lzo,
            TransactionID VARCHAR(10) ENCODE lzo,
            Date DATE ENCODE bytedict,
            ProductID VARCHAR(10) ENCODE lzo,
            Quantity INT ENCODE delta,
            Price DECIMAL(10,2) ENCODE delta,
            StoreLocation VARCHAR(30) ENCODE lzo
            )
            DISTSTYLE KEY
            DISTKEY (TransactionID)
            SORTKEY (TransactionID , ProductID);""")

        # Create target table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions_db.public.target_fact_transactions (
            CustomerID VARCHAR(10) ENCODE lzo,
            TransactionID VARCHAR(10) ENCODE lzo,
            Date DATE ENCODE bytedict,
            ProductID VARCHAR(10) ENCODE lzo,
            Quantity INT ENCODE delta,
            Price DECIMAL(10,2) ENCODE delta,
            StoreLocation VARCHAR(30) ENCODE lzo
            )
            DISTSTYLE KEY
            DISTKEY (TransactionID)
            SORTKEY (TransactionID , ProductID);""")

        # show tables
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
        print("TABLES AVAILABLE:", cursor.fetchall())
        print("Connection Successful!")

    except Exception as e:
        print("Error creating tables:", e)

    try:
        # load the data
        file = csv.reader(data.splitlines())
        headers = next(file)

        #  truncate staging table
        cursor.execute("TRUNCATE TABLE transactions_db.public.staging_fact_transactions;")

        for row in file:
            # print(f"Rows available:", row)
            if row[0] != 'ProductID':
                cursor.execute("""INSERT INTO transactions_db.public.staging_fact_transactions
                VALUES( %s, %s, %s, %s, %s, %s, %s)""",
                               (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))

        # Commit changes
        conn.commit()

        # sample data
        cursor.execute("SELECT * FROM transactions_db.public.staging_fact_transactions LIMIT 5;")
        print("SHOWING ONLY 5 ROWS:", cursor.fetchall())

    except Exception as e:
        print("Error fetching data from staging database:", e)
        raise e

    try:
        # upsert operation update based on transactionid and insert new values
        cursor.execute("""MERGE INTO 
        transactions_db.public.target_fact_transactions 
        USING 
        transactions_db.public.staging_fact_transactions stg ON transactions_db.public.target_fact_transactions.TransactionID = stg.TransactionID 
        WHEN MATCHED 
        THEN 
        UPDATE SET CustomerID = stg.CustomerID , Date= stg.Date, ProductID = stg.ProductID, Quantity = stg.Quantity, Price = stg.Price, StoreLocation = stg.StoreLocation 
        WHEN NOT MATCHED 
        THEN 
        INSERT (CustomerID,TransactionID,Date,ProductID,Quantity,Price,StoreLocation) VALUES (stg.CustomerID,stg.TransactionID,stg.Date,stg.ProductID,stg.Quantity,stg.Price,stg.StoreLocation);""")

        conn.commit()
        print("Upsert operation completed successfully.")

    except psycopg2.Error as e:
        print(f"Error during upsert operation: {e}")
        conn.rollback()
        cursor.execute("""MERGE INTO 
            transactions_db.public.target_fact_transactions 
            USING 
            transactions_db.public.staging_fact_transactions stg ON transactions_db.public.target_fact_transactions.TransactionID = stg.TransactionID 
            WHEN MATCHED 
            THEN DELETE
            WHEN NOT MATCHED 
            THEN 
            INSERT (CustomerID,TransactionID,Date,ProductID,Quantity,Price,StoreLocation) VALUES (stg.CustomerID,stg.TransactionID,stg.Date,stg.ProductID,stg.Quantity,stg.Price,stg.StoreLocation);""")

        conn.commit()
        print("Fallback operation (delete and insert) completed successfully.")


    return {
        'statusCode': 200,
        'body': json.dumps('Complete Upsert in Redshift!')
    }

