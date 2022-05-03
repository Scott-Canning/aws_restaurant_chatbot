from keys import MSTR_USER, MSTR_PW
from datetime import datetime
import requests
import logging
import json
import boto3
import random


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# format 24-hour time as 12-hour time
def convert_time(time_24):
    t = datetime.strptime(time_24, '%H:%M')
    t = t.strftime('%I:%M %p')
    return t
    

def convert_date(date):
    d = datetime.strptime(date, '%Y-%m-%d')
    d = d.strftime('%B %d, %Y')
    return d


# poll SQS -> messages[]
def receive_queue_messages():
    
    sqs = boto3.client('sqs')
    queue_url = 'https://queue.amazonaws.com/<Account-ID>/<SQS-Name>'

    try:
        response = sqs.receive_message(
                                      QueueUrl=queue_url,
                                      AttributeNames=['SentTimestamp'],
                                      MaxNumberOfMessages=2,
                                      MessageAttributeNames=['All'],
                                      VisibilityTimeout=15, 
                                      WaitTimeSeconds=20)
        
        messages = response['Messages']
        
        return messages

    except Exception as e:
        raise Exception(" [receive_queue_messages] EXCEPTION: {}".format(e))


# query OS for restaurant recommendations with cuisine input -> restaurant_ids[]
def open_search_recommendations(cuisine):
    
    # initialize credentials
    region = 'us-east-1' 
    service = 'es'
    credentials = boto3.Session().get_credentials()

    # build search path
    host = '<OpenSearch-host-url>' 
    path = '/restaurant/_search'
    url = host + path
    headers = { "Content-Type": "application/json" }
    
    # build query
    query = {
            "query": { 
                "match": {
                    'Cuisine': cuisine
                },
            },
        }

    response = requests.get(url, auth=(MSTR_USER, MSTR_PW), headers=headers, data=json.dumps(query))
    responseJSON = response.json()
    hit_count = responseJSON['hits']['total']
    hits = responseJSON['hits']['hits']
    restaurant_ids = []
    for hit in hits:
        res_id = str(hit['_id'])
        restaurant_ids.append(res_id)

    return restaurant_ids


# query dynamo with restaurant_id_list -> restaurant_details[]
def query_db(restaurant_id_list):
    
    table_name = 'yelp-restaurants'
    resource = boto3.resource('dynamodb')
    table = resource.Table(table_name)
    restaurant_details = []
    
    for res_id in restaurant_id_list:
        query = table.get_item(Key={'restaurant_id': str(res_id)})
        restaurant_details.append(query)
            
    return restaurant_details


# format restaurant details -> formatted_message
def format_message(message, restaurant_details):
    
    # pull user preference information from queue message
    cuisine = message['MessageAttributes']['cuisine']['StringValue']
    dining_time = message['MessageAttributes']['dining_time']['StringValue']
    dining_date = message['MessageAttributes']['dining_date']['StringValue']
    number_of_people = message['MessageAttributes']['number_of_people']['StringValue']
    
    # pull restaurant details for each recommendation
    restaurant_arr = []
    for i in range(len(restaurant_details)):
        res_name = restaurant_details[i]['Item']['name']
        res_address = restaurant_details[i]['Item']['address']
        res_zip_code = restaurant_details[i]['Item']['zip_code']
        restaurant_arr.append(res_name + " located at " + res_address + ", " + str(res_zip_code))
                        
    time = convert_time(dining_time)
    date = convert_date(dining_date)

    formatted_message = f"Hi! Restaurant Chatbot Here!\n" + \
                        f"Please find my restaurant recommendations for {cuisine}\n" + \
                        f"for your party of {number_of_people} at {time} on {date}: \n" + \
                        f"1. {restaurant_arr[0]} \n" + \
                        f"2. {restaurant_arr[1]} \n" + \
                        f"3. {restaurant_arr[2]}"

    return formatted_message


# send SMS to user's phone number (unconfigured in Sandbox)
def send_sms(message, phone_number):
    
    logger.debug("[send_sms] message: {}".format(message))
    logger.debug("[send_sms] phone_number: {}".format(phone_number))
    
    client = boto3.client('sns')
    if '+1' not in phone_number:
        phone_number = '+1' + phone_number
    client.publish(PhoneNumber=phone_number, Message=message)


# send SES to user's email address
def send_ses(message, email_address):
    
    logger.debug("[send_ses] message: {}".format(message))
    logger.debug("[send_ses] email_address: {}".format(email_address))
    
    client = boto3.client('ses')
    if(email_address):
    
        response = client.send_email(
            Source='sc7979@nyu.edu',
            Destination={
                'ToAddresses': [
                    email_address,
                ]
            },
            Message={
                'Subject': {
                    'Data': 'Restaurant Chatbot Recommendations',
                },
                'Body': {
                    'Text': {
                        'Data': message,
                    },
                }
            },
        )


# delete message from SQS
def delete_sqs(receipt_handle):
    
    sqs = boto3.client('sqs')
    sqs.delete_message(QueueUrl='https://queue.amazonaws.com/<Account-ID>/<SQS-Name>',
                       ReceiptHandle=receipt_handle
                      )


def lambda_handler(event, context):
    
    message_queue = receive_queue_messages()
    
    if(message_queue):
        for message in message_queue:
            
            cuisine = message['MessageAttributes']['cuisine']['StringValue']

            # search OpenSearch for recommendations
            restaurant_ids = open_search_recommendations(cuisine)
            
            # recommend randomly selected restaurant sample of size 3
            id_recommendations = random.sample(restaurant_ids, 3)
            
            # query dynamo db
            restaurant_details = query_db(id_recommendations)
            
            # format message 
            formatted_message = format_message(message, restaurant_details)
            
            # extract phone_number
            # phone_number = message['MessageAttributes']['phone_number']['StringValue']
            
            # send SMS
            # send_sms(formatted_message, phone_number)
            
            # extract email_address
            email_address = message['MessageAttributes']['email_address']['StringValue']
            
            # send SES
            send_ses(formatted_message, email_address)
            
            # clear message from queue
            receipt_handle = message['ReceiptHandle']
            delete_sqs(receipt_handle)
