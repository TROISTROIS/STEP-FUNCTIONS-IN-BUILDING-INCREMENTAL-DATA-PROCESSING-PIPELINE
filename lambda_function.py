import json
from generate_csv import generate_data
from upload_to_s3 import upload_to_s3
from datetime import date, timedelta

# Define date range (modify as needed)
start_date = date(2024, 7, 30)  # Adjust start date
end_date = date.today()

def lambda_handler(event, context):
    for current_date in range((end_date - start_date).days + 1):
        # Generate Date
        current_date = start_date + timedelta(days=current_date)
        date_str = str(current_date)
        generate_data(current_date, date_str)
        # Upload the generated CSV to S3
        upload_to_s3(f"sales_{date_str}.csv", date_str)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Sales Data Generated Successfully!!')
    }