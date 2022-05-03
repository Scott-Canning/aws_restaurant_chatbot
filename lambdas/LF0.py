import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('lex-runtime')
    
    # post text to lex
    response = client.post_text(
                                botName='<bot-name>',
                                botAlias='<bot-alias>',
                                userId="12",
                                inputText=event["messages"][0]["unstructured"]["text"]
                                )


    # if 200, respond from lex to chat bot
    if response['ResponseMetadata']['HTTPStatusCode'] == 200 :
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Headers" : "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
                "Access-Control-Allow-Origin": "*"
            },
            "messages": [
                {
                "type": "unstructured",
                "unstructured": {
                    "id": "12",
                    "text": response['message'],
                    "timestamp": "string"
                    }
                }
            ]
        }
        
    return {
        'statusCode': 200,
        'body': json.dumps('Error')
    }
