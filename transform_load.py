import json
import pandas as pd;
import boto3;
from datetime import datetime;
from io import StringIO;

def get_whale_data(block_data):
    whale_txn = [];

    for txn in block_data['tx']:
        btc_amount = sum(out['value'] for out in txn['out']) / 1e8;
        if btc_amount > 10:
            whale_txn.append({
                'txn_hash' : txn['hash'],
                'from_wallet' : txn['inputs'][0]['prev_out']['addr'] if txn['inputs'] else 'unknown',
                'to_wallet' : txn['out'][0]['addr'] if txn['out'] else 'unknown',
                'amount' : btc_amount,
                'datetime' : datetime.now().strftime('%H:%M:%S'),
            });
    return whale_txn;


def lambda_handler(event, context):
    s3 = boto3.client('s3');
    bucket = 'crypto-whale-tracker-etl';
    key = 'raw_data/to_process/';

    whale_data = [];
    whale_key = [];

    for file in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']:
        file_key = file['Key'];
        if file_key.endswith('.json'):
            obj = s3.get_object(Bucket=bucket, Key=file_key);
            data = json.loads(obj['Body'].read().decode('utf-8'));
            whale_data.append(data);
            whale_key.append(file_key);
    
    for data in whale_data:
        whale_txn = get_whale_data(data);

        whale_df = pd.DataFrame(whale_txn);
        whale_df['datetime'] = pd.to_datetime(whale_df['datetime']);

        whale_file_key = f'transformed_data/whale_data/whale_processed_data_{str(datetime.now())}.csv';

        whale_buffer = StringIO();
        whale_df.to_csv(whale_buffer, index=False);
        whale_content = whale_buffer.getvalue();
        s3.put_object(Bucket=bucket, Key=whale_file_key, Body=whale_content);

    s3_resource = boto3.resource('s3');
    for key in whale_key:
        copy_source = {'Bucket': bucket, 'Key': key};
        s3_resource.meta.client.copy(copy_source, bucket, 'raw_data/processed/' + key.split('/')[-1]);
        s3_resource.Object(bucket, key).delete();


