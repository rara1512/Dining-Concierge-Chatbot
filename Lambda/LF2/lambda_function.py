import json
import random
import boto3
import logging
from botocore.exceptions import ClientError
import requests 

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#this is the trigger function for this program, it calls the callSQS function 
def lambda_handler(event, context):
    callSQS(event)

#as soon as a message gets in the SQSqueue, lf2 gets triggered and information is stored as a event
def callSQS(event):
    message_attributes = event['Records'][0]['messageAttributes']
    cuisine = message_attributes['cuisine']['stringValue']
    location = message_attributes['location']['stringValue']
    date = message_attributes['date']['stringValue']
    time = message_attributes['time']['stringValue']
    people = message_attributes['people']['stringValue']
    email = message_attributes['email']['stringValue']
    rest_ids = get_rest_id(cuisine)
    rest_info = ""
    for i in range(5):
        rest_info += str(i+1)+ ". "+get_restaurant_info(rest_ids[i]) +"\n"+"\n"
    sendMessage = 'Hello! Here are my '+ cuisine +' restaurant suggestions for ' + people + ' people dining on ' + date + ' at ' + time + " " + '\n' +'\n'+rest_info+'Enjoy your meal and do remember to rate us on YELP!'
    temp_email(sendMessage,email)

#get restaurant ID from Elastic Open Search with target user query cusine
def get_rest_id(cuisine):
    es_Query = "https://search-restaurants-g7ikxqgqdbmkt64tdwa7fci6v4.us-east-1.es.amazonaws.com/_search?q={cuisine}".format(
        cuisine=cuisine)
    esResponse = requests.get(es_Query,auth=('himarest', 'London@1985'))
    data = json.loads(esResponse.content.decode('utf-8'))
    try:
        esData = data["hits"]["hits"]
    except KeyError:
        logger.debug("Error extracting hits from ES response")
    rest_ids = []
    nums = random.sample(range(0, len(esData)-1), 5)
    for i in range(5):
        tmpList = esData[nums[i]]
        rest_ids.append(tmpList['_source']['restaurantID'])
    return rest_ids
    
    
#get restaurant name and address from dynamo db based on the id fetched from Elastic search
def get_restaurant_info(rest_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    response = table.get_item(
        Key={
            'id': rest_id
        }
    )
    response_item = response.get("Item")
    rest_name = response_item['restaurent_name']
    rest_address = response_item['address']
    rest_info = ('Restaurant Name: '+rest_name +'\n'+'Address: '+','.join(rest_address))
    return rest_info

#send user the template mail with the suggestions 
#Note: Free Tier only supports sending mails to verified email addresses 
#Verify the E-Mail address in the SES microservice 
def temp_email(sendMessage,email):
    ses_client = boto3.client("ses", region_name="us-east-1")
    CHARSET = "UTF-8"
    ses_client.send_email(
        Destination={
            "ToAddresses": [
                email,
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": sendMessage,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Dining Suggestions",
            },
        },
        Source="kumarkush36@gmail.com",
    )
    
