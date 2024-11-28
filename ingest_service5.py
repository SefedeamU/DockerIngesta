import boto3
import pandas as pd
import json
import os

def scan_dynamodb_table(table_name):
    dynamodb = boto3.client('dynamodb')
    paginator = dynamodb.get_paginator('scan')
    response_iterator = paginator.paginate(TableName=table_name)
    
    items = []
    for page in response_iterator:
        items.extend(page['Items'])
    
    return items

def save_to_s3(data, bucket_name, file_name):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=data)

def main():
    table_name = os.getenv('DYNAMODB_TABLE')
    bucket_name = os.getenv('S3_BUCKET')
    file_format = os.getenv('FILE_FORMAT', 'csv')
    
    items = scan_dynamodb_table(table_name)
    
    if file_format == 'csv':
        df = pd.DataFrame(items)
        data = df.to_csv(index=False)
        file_name = f'{table_name}.csv'
    else:
        data = json.dumps(items)
        file_name = f'{table_name}.json'
    
    save_to_s3(data, bucket_name, file_name)

if __name__ == "__main__":
    main()