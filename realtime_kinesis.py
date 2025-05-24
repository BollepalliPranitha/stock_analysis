import boto3
import json
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
s3_client = boto3.client('s3')
kinesis_client = boto3.client('kinesis')

def lambda_handler(event, context):
    # Extract bucket and object key from the event
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        logger.info(f"Bucket: {bucket_name}, Key: {object_key}")
    except KeyError as e:
        logger.error("Invalid S3 event structure.")
        raise e

    # Fetch the S3 object
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        data = response['Body'].read().decode('utf-8')
        records = data.splitlines()  # Assuming the file is line-delimited
        records = records[1:]  # Skip the header
    except Exception as e:
        logger.error(f"Error reading S3 object: {e}")
        raise e

    # Send data to Kinesis
    try:
        for record in records:
            kinesis_client.put_record(
                StreamName='stock_realtime',
                Data=json.dumps({"stock_data": record}),
                PartitionKey="default"  # Required: Add a partition key
            )
        logger.info(f"Successfully sent {len(records)} records to Kinesis.")
    except Exception as e:
        logger.error(f"Error sending data to Kinesis: {e}")
        raise e

    return {
        "statusCode": 200,
        "body": json.dumps({"message": f"Processed {len(records)} records."})
    }
