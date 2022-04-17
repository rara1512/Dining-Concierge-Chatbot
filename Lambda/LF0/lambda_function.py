import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#obtain the lex client using boto3
client = boto3.client('lex-runtime')
#post the bot response to the front end 
def lambda_handler(event, context):
    response = client.post_text(
        botName='DiningSuggestions',
        botAlias='$LATEST',
        userId='User0',
        inputText=event['messages'][0]['unstructured']['text']
        )
    botResponse =  [{
        'type': 'unstructured',
        'unstructured': {
          'text': response["message"]
        }}]
    return {
        'statusCode': 200,
        'messages': botResponse
        }
        