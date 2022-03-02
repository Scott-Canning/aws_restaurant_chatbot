import json
import boto3
import time
import os
import logging
import math
import datetime


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


# format 24-hour time as 12-hour time
def convert_time(time_24):
    t = datetime.strptime(time_24, '%H:%M')
    t = t.strftime('%I:%M %p')
    return t
    

def convert_date(date):
    d = datetime.strptime(date, '%Y-%m-%d')
    d = d.strftime('%B %d, %Y')
    return d
    

# input intent, returns slots
def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


# extract slot from intent
def elicit_slots(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


# confirm user's intent
def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }


# inform Lex of no response
def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    
    return response
 

# choose next path of action for Lex
def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


# pass slot values to SQS (name: LexMessageQueue)
def enque(slots):
    
    attributes = {
        'email_address': {
            'DataType': 'String',
            'StringValue': slots['email_address']
        },
        'cuisine': {
            'DataType': 'String',
            'StringValue': slots['cuisine']
        },
        'number_of_people': {
            'DataType': 'String',
            'StringValue': slots['number_of_people']
        },
        'dining_time': {
            'DataType': 'String',
            'StringValue': slots['dining_time']
        },
        'dining_date': {
            'DataType': 'String',
            'StringValue': slots['dining_date']
        },
        'location': {
            'DataType': 'String',
            'StringValue': slots['location']
        }
    }
    
    data = json.dumps({
        'email_address': slots['email_address'],
        'cuisine': slots['cuisine'],
        'number_of_people': slots['number_of_people'],
        'dining_time': slots['dining_time'],
        'dining_date': slots['dining_date'],
        'location': slots['location']
    })
    

    sqs = boto3.client('sqs')

    try:
        queue_url = 'https://queue.amazonaws.com/472171587515/LexMessageQueue' 
        response = sqs.send_message(
                                   QueueUrl=queue_url,
                                   MessageBody=data,
                                   MessageAttributes=attributes,
                                  )
        logger.debug(" >>>>>> response is {}".format(json.dumps(response)))
           
    except Exception as e:
        raise Exception("{}".format(e))
        

# validate user inputs
def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


# validate user provided inputs
def validate_intent(cuisine, location, dining_time, dining_date, number_of_people):

    cuisine_types = [
                    'cafe', 
                    'bar', 
                    'sushi', 
                    'pizza',
                    'indian',
                    'thai', 
                    'italian', 
                    'french',
                    'chinese',
                    'mexican',
                    ]
    
    if cuisine is not None and cuisine.lower() not in cuisine_types:
        return build_validation_result(False,
                                       'cuisine',
                                       'We do not currently track {} cuisine. We provide suggestions ' \
                                       'for the following cuisine types: cafe, bar, sushi, pizza, indian, ' \
                                       'thai, italian, french, chinese, and mexican - would you like a different ' \
                                       'type of cuisine?'.format(cuisine))
                                   
    if location is not None and location.lower() not in ['new york', 'manhattan', 'brooklyn', 'bronx', 'staten island', 'queens']:
        return build_validation_result(False,
                                       'location',
                                       'We currently only provide restaurant suggestions in New York. Please enter New York')
                                       
    if dining_time is not None:
        if len(dining_time) > 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'dining_time', 'Please enter a valid dining time')

        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'dining_time', 'Please enter a valid dining time')

        if hour < 8 or hour > 21:
            # Outside of business hours
            return build_validation_result(False, 'dining_time', 'We only provide suggestions for restaurants ' \
                                                                 'between 8:00 AM and 10:00 PM. Can you please ' \
                                                                 'specify a time within this range?')

    if dining_date is not None:
        if datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'dining_date', 'Please enter a valid dining date')

    if number_of_people is not None:
        number_of_people = parse_int(number_of_people)
        if(number_of_people < 0 or number_of_people > 15):
            return build_validation_result(False, 
                                           'number_of_people', 
                                           'Please enter a party size greater than 0 and less than 15')
    
    return build_validation_result(True, None, None)
    

# parse intent of type dining suggestions
def dining_suggestions_intent(intent):
    
    # extract values from intent including source
    cuisine = get_slots(intent)['cuisine']
    location = get_slots(intent)['location']
    dining_time = get_slots(intent)['dining_time']
    dining_date = get_slots(intent)['dining_date']
    number_of_people = get_slots(intent)['number_of_people']
    invocation_source = intent['invocationSource']

    # validate fulfillment
    if invocation_source == 'DialogCodeHook':
        slots = get_slots(intent)
        validation_result = validate_intent(
                                            cuisine, 
                                            location, 
                                            dining_time,
                                            dining_date,
                                            number_of_people
                                            )
        
        #print("validation_result:", validation_result)
                                                    
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            
            return elicit_slots(intent['sessionAttributes'],
                                intent['currentIntent']['name'],
                                slots,
                                validation_result['violatedSlot'],
                                validation_result['message'])


        output_session_attributes = intent['sessionAttributes'] if intent['sessionAttributes'] is not None else {}
        
        return delegate(output_session_attributes, get_slots(intent))

    slots = get_slots(intent)
    logger.debug("slots {}".format(slots))
    enque(slots)

    t = datetime.datetime.strptime(dining_time, "%H:%M")
    t = t.strftime("%I:%M %p")
    d = datetime.datetime.strptime(dining_date, '%Y-%m-%d')
    d = d.strftime('%B %d, %Y')
    
    return close(intent['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thank you for using the Restaurant Chatbot to ' \
                             'find your next favorite restaurant! Recommendations ' \
                             'for a {} restaurant at {} for a party of {} on {} ' \
                             'will be sent to the email address provided ' \
                             'momentarily.'.format(cuisine, t, number_of_people, d)})


def greeting_intent():
    
    logger.debug("greeting_intent reached")

    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Hello! How can I help you today?'}
        }
    }


def thank_you_intent():

    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'You\'re welcome!'}
        }
    }


# parse intent name from event and pass to appropriate intent handler
def intent_handler(intent):

    intent_name = intent['currentIntent']['name']
    
    if intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions_intent(intent)
        
    elif intent_name == 'GreetingIntent':
        return greeting_intent()
        
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent()
        
    raise Exception(intent_name + ' intent not supported')


def lambda_handler(event, context):
    
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    
    return intent_handler(event)
