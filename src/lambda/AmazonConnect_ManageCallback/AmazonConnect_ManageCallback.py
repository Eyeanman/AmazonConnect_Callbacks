import boto3
import os
import logging

# logging
ROOT_LOG_LEVEL = os.environ["ROOT_LOG_LEVEL"]
LOG_LEVEL = os.environ["LOG_LEVEL"]
root_logger = logging.getLogger()
root_logger.setLevel(ROOT_LOG_LEVEL)
log = logging.getLogger(__name__)
log.setLevel(LOG_LEVEL)

TABLE_NAME = os.environ["DDB_CALLBACKS"]
INSTANCE_ID = os.environ["INSTANCE_ID"]

def check_callback(ddb_table, phone_number):
    response = ddb_table.get_item(
        Key={
            'phone_number': phone_number
        }
    )
    if 'Item' in response:
        Callback_Status = response['Item']['callback_status']
    else:
        Callback_Status = "notfound"
    return Callback_Status

def create_callback(ddb_table, phone_number, contactid):
    response = ddb_table.put_item(
        Item={
            'phone_number': phone_number,
            'contactid': contactid,
            'callback_status': 'requested'
        }
    )
    return "success"
    
def cancel_callback(ddb_table, phone_number):
    response = ddb_table.get_item(
        Key={
            'phone_number': phone_number
        }
    )
    if 'Item' in response:
        Callback_ContactId = response['Item']['contactid_callback']
        client_connect = boto3.client('connect')
        response = client_connect.stop_contact(
            ContactId=Callback_ContactId,
            InstanceId=INSTANCE_ID
        )
        Callback_Status = "cancelled"
        response = ddb_table.put_item(
            Item={
                'phone_number': phone_number,
                'callback_status': 'cancelled'
            }
        )
    else:
        Callback_Status = "notfound"
    return Callback_Status
    
def get_ddb_table():
    resource_ddb = boto3.resource('dynamodb')
    table = resource_ddb.Table(TABLE_NAME)
    return table
    

def lambda_handler(event, context):
    log.debug(f"Event Data: {event}")
    action = event['Details']['Parameters']['action'] # valid options: check, create, cancel
    contactid = event['Details']['ContactData']['ContactId']
    phone_number = event['Details']['ContactData']['CustomerEndpoint']['Address']
    log.info(f"Completing Action: {action}, for contact id: {contactid}, phone number: {phone_number}")
    
    ddb_table = get_ddb_table()
    response = {
        'Callback_Status': 'error'
    }
    if action == "check":
        response['Callback_Status'] = check_callback(ddb_table, phone_number)
    elif action == "create":
        response['Callback_Status'] = create_callback(ddb_table, phone_number, contactid)
    elif action == "cancel":
        response['Callback_Status'] = cancel_callback(ddb_table, phone_number)
        
    log.info(f"Returning: {response}")
    return response