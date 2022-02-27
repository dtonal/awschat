import json
import boto3
import os

s3_client = boto3.client("s3")
S3_BUCKET_NAME = os.environ.get('BUCKETNAME')
S3_ORIGIN = os.environ.get('S3ORIGIN')


def read_proxy(event):
    path_params = event["pathParameters"]
    proxy = path_params["proxy"]
    return proxy


def lambda_handler(event, context):
    proxy = read_proxy(event)

    if proxy == 'conversations':
        content_object = get_conversations()
    elif proxy.startswith('conversations/'):
        content_object = get_conversation(extract_id(proxy))
    else:
        return create_error('No matching route')

    return create_response(prepare_content(content_object))


def extract_id(proxy):
    return proxy[len('conversations/'):]


def get_conversations():
    return get_s3_object('data/conversations.json')


def get_conversation(id):
    return get_s3_object('data/conversations/'+id+'.json')


def prepare_content(content_object):
    file_content = content_object["Body"].read().decode()
    json_content = json.loads(file_content)
    return json_content


def get_s3_object(key):
    return s3_client.get_object(
        Bucket=S3_BUCKET_NAME, Key=key)


def create_response(json_content):
    return {
        'statusCode': 200,
        "headers": {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': S3_ORIGIN
        },
        'body': json.dumps(json_content)
    }


def create_error(msg):
    return {
        'statusCode': 400,
        "headers": {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': S3_ORIGIN
        },
        'body': msg
    }
