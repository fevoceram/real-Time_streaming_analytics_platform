import boto3
import json

def consume_kinesis_stream(stream_name, region_name="us-east-1"):
    client = boto3.client('kinesis', region_name=region_name)
    response = client.describe_stream(StreamName=stream_name)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='LATEST'
    )['ShardIterator']

    while True:
        out = client.get_records(ShardIterator=shard_iterator, Limit=100)
        shard_iterator = out['NextShardIterator']
        for record in out['Records']:
            yield json.loads(record['Data'])
