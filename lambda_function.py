import json
from generate_csv import generate_data
from upload_to_s3 import upload_to_s3
from datetime import date, timedelta, datetime


def lambda_handler(event, context):
    timestamp = datetime.now().strftime("%X")
    generate_data(timestamp)
    # Upload the generated CSV to S3
    upload_to_s3(f"sales_{timestamp}.csv", timestamp)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Sales Data Generated Successfully!!')
    }