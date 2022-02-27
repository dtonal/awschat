import json
import os

import boto3
import ConversationDao

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
        return create_response(prepare_content(content_object['Body']))
    elif proxy.startswith('conversations/'):
        return create_response(read_conversation(extract_id(proxy)))

    return create_error('No matching route')


def extract_id(proxy):
    return proxy[len('conversations/'):]


def get_conversations():
    return get_s3_object('data/conversations.json')


def read_conversation(id):
    data = ConversationDao.query_chat_messages(id)
    items = data['Items']
    print(items)
    return load_messages(data, id, [])


def load_messages(data, id, messages):
    for msg in data['Items']:
        messages.append(to_dict(msg))

    if data.get('LastEvaluatedKey'):
        data = ConversationDao.query_chat_messages(
            id, data['LastEvaluatedKey'])
        load_messages(data, id, messages)

    return load_details(id, messages)


def to_dict(msg):
    return {
        'sender': msg['Sender'],
        'time':   str(msg['Timestamp']),
        'message': msg['Message']
    }


def load_details(id, messages):
    return {
        'id': id,
        'participants': read_participants(id),
        'last': get_last_message_time(messages),
        'messages': messages
    }


def get_last_message_time(messages):
    return 0 if len(messages) < 1 else messages[-1]['time']


def read_participants(id):
    data = ConversationDao.query_conversations(id)
    participants = []
    for participant in data['Items']:
        participants.append(participant['Username'])
    return participants


def get_conversation(id):
    return get_s3_object('data/conversations/'+id+'.json')


def prepare_content(body):
    file_content = body.read().decode()
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
