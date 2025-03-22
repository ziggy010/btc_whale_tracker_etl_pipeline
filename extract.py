import json;
import requests;
import boto3;
from datetime import datetime;

def lambda_handler(event, context):
    url = 'https://blockchain.info/latestblock';
    latest_hash = requests.get(url).json()['hash'];
    block_url = f'https://blockchain.info/rawblock/{latest_hash}';
    block_data = requests.get(block_url).json();

    client = boto3.client('s3');

    client.put_object(
        Body=json.dumps(block_data), 
        Bucket='crypto-whale-tracker-etl', 
        Key=f'raw_data/to_process/whale_data_raw_' + str(datetime.now()) + '.json',
        );