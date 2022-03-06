import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')
chat_messages = dynamodb.Table('Chat-Messages')
chat_conversations = dynamodb.Table('Chat-Conversations')


def query_chat_messages(id, last_evaluated_key=None):
    query_params = {
        'ProjectionExpression': '#T, Sender, Message',
        'ExpressionAttributeNames': {'#T': 'Timestamp'},
        'KeyConditionExpression': Key('ConversationId').eq(id)
    }
    if last_evaluated_key:
        query_params = query_params['ExclusiveStartKey'] = last_evaluated_key

    return chat_messages.query(**query_params)


def query_conversations(id):
    return chat_conversations.query(
        Select='ALL_ATTRIBUTES',
        KeyConditionExpression=Key('ConversationId').eq(id)
    )


def query_all_conversations():
    return chat_conversations.scan()


def get_conv_ids_for(username):
    ids = chat_conversations.query(
        IndexName='Username-ConversationId-index',
        Select='SPECIFIC_ATTRIBUTES',
        ProjectionExpression="ConversationId",
        KeyConditionExpression=Key('Username').eq(username)
    )['Items']
    return [id['ConversationId'] for id in ids]


def get_convs(ids):
    return chat_conversations.scan(
        Select='ALL_ATTRIBUTES',
        FilterExpression=Attr('ConversationId').is_in(ids))['Items']


def query_participants(id):
    participants = chat_conversations.query(
        Select='SPECIFIC_ATTRIBUTES',
        ProjectionExpression="Username",
        KeyConditionExpression=Key('ConversationId').eq(id)
    )['Items']
    return [p['Username'] for p in participants]


def query_last_msg_time(id):
    return chat_messages.query(
        ProjectionExpression='#T',
        Limit=1,
        ScanIndexForward=False,
        ExpressionAttributeNames={'#T': 'Timestamp'},
        KeyConditionExpression=Key('ConversationId').eq(id)
    )['Items']
