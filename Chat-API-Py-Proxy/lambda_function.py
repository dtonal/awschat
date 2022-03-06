import json
import os
import boto3
import ConversationDao
import asyncio
from datetime import datetime

from itertools import groupby

S3_ORIGIN = os.environ.get('S3ORIGIN')


def read_proxy(event):
    path_params = event["pathParameters"]
    proxy = path_params["proxy"]
    return proxy


def lambda_handler(event, context):
    proxy = read_proxy(event)
    if proxy == 'conversations':
        return create_response(asyncio.run(get_conversations()))
    elif proxy.startswith('conversations/'):
        return create_response(asyncio.run(read_conversation(extract_id(proxy))))

    return create_error('No matching route')


def extract_id(proxy):
    return proxy[len('conversations/'):]


async def get_conversations():
    ids = ConversationDao.get_conv_ids_for('Student')
    return [await read_conv_data(id) for id in ids]


async def read_conv_data(id):
    return {
        'id': id,
        'participants': await read_participants(id),
        'last_msg_time': await last_msg_time(id)
    }


async def read_participants(id):
    return ConversationDao.query_participants(id)


async def last_msg_time(id):
    last_msg_time = ConversationDao.query_last_msg_time(id)
    return str(last_msg_time[0]['Timestamp'])


def key_func(k):
    return k['ConversationId']


async def read_conversation(id):
    data = ConversationDao.query_chat_messages(id)
    print('read_conversation', data)
    return await load_messages(data, id, [])


async def load_messages(data, id, messages):
    for msg in data['Items']:
        messages.append(to_dict(msg))

    print('load_messages mesages:', messages)
    if data.get('LastEvaluatedKey'):
        data = ConversationDao.query_chat_messages(
            id, data['LastEvaluatedKey'])
        load_messages(data, id, messages)

    return await load_details(id, messages)


def to_dict(msg):
    return {
        'sender': msg['Sender'],
        'time':   str(msg['Timestamp']),
        'message': msg['Message']
    }


async def load_details(id, messages):
    details = {
        'id': id,
        'participants': await read_participants(id),
        'last': get_last_message_time(messages),
        'messages': messages
    }
    print('load_details:', details)
    return details


def get_last_message_time(messages):
    return 0 if len(messages) < 1 else messages[-1]['time']


def prepare_content(body):
    file_content = body.read().decode()
    json_content = json.loads(file_content)
    return json_content


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
