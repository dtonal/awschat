import boto3
from boto3.dynamodb.conditions import Key

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
