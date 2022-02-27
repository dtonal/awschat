import json
import boto3
import os
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
chat_messages = dynamodb.Table('Chat-Messages')
chat_conversations = dynamodb.Table('Chat-Conversations')

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
        content_object = read_conversation(extract_id(proxy))
        #content_object = get_conversation(extract_id(proxy))
        return create_response(content_object)
    else:
        return create_error('No matching route')


def extract_id(proxy):
    return proxy[len('conversations/'):]


def get_conversations():
    return get_s3_object('data/conversations.json')


def read_conversation(id):
    data = chat_messages.query(
        ProjectionExpression='#T, Sender, Message',
        ExpressionAttributeNames={'#T': 'Timestamp'},
        KeyConditionExpression=Key('ConversationId').eq(id))
    items = data['Items']
    print(items)
    return load_messages(data, id, [])


def load_messages(data, id, messages):
    for msg in data['Items']:
        print(msg)
        messages.append({
            'sender': msg['Sender'],
            'time':   str(msg['Timestamp']),
            'message': msg['Message']
        })
        print(messages)

    if data.get('LastEvaluatedKey'):
        data = chat_messages.query(
            ProjectionExpression='#T, Sender, Message',
            ExpressionAttributeNames={'#T': 'Timestamp'},
            KeyConditionExpression=Key('ConversationId').eq(id),
            ExclusiveStartKey=data['LastEvaluatedKey']
        )
        load_messages(data, id, messages)
    else:
        details = load_details(id, messages)
        print('details: ', details)
        return details


def load_details(id, messages):
    data = chat_conversations.query(
        Select='ALL_ATTRIBUTES',
        KeyConditionExpression=Key('ConversationId').eq(id)
    )
    participants = []
    for participant in data['Items']:
        participants.append(participant['Username'])

    return {
        'id': id,
        'participants': participants,
        'last': 0 if len(messages) < 1 else messages[-1]['time'],
        'messages': messages
    }


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
