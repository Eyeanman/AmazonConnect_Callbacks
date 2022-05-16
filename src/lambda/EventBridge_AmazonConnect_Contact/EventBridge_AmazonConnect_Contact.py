import boto3
from boto3.dynamodb.conditions import Key
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

def get_primarykey(ddb_table, initial_contactid):
    log.debug(f"Getting Primary key for InitialContactId: {initial_contactid}")
    response = ddb_table.query(
        IndexName='contactid-index',
        KeyConditionExpression=Key('contactid').eq(initial_contactid)
        )
    log.debug(f"Get PrimaryKey Response: {response}")
    primarykey = response['Items'][0]['phone_number']
    return primarykey

    
def update_callback(ddb_table, primarykey, contactid, callback_status):
    log.debug(f"Updating Table, CurrentContactId: {contactid}, PrimaryKey: {primarykey}, CallbackStatus: {callback_status}")

    response = ddb_table.update_item(
        Key={
            'phone_number': primarykey
        },
        UpdateExpression="set contactid_callback=:cicb, callback_status=:cba",
        ExpressionAttributeValues={
            ':cicb': contactid,
            ':cba': callback_status
        },
    )

def get_ddb_table():
    resource_ddb = boto3.resource('dynamodb')
    table = resource_ddb.Table(TABLE_NAME)
    return table
    

def lambda_handler(event, context):
    log.debug(event)
    eventType = event['detail']['eventType']
    initiationMethod = event['detail']['initiationMethod']
    contactid = event['detail']['contactId']
    log.info(f"Event Type: {eventType}, Initiation Method: {initiationMethod} for ContactId: {contactid}")
    
    ddb_table = get_ddb_table()
    
    if eventType == "INITIATED" and initiationMethod == "CALLBACK":
        log.info(f"NEW CALLBACK CREATED")
        initial_contactid = event['detail']['initialContactId']
        primarykey = get_primarykey(ddb_table, initial_contactid)
        update_callback(ddb_table, primarykey, contactid, "active")
    elif eventType == "CONNECTED_TO_AGENT" and initiationMethod == "CALLBACK":
        log.info(f"CALLBACK CONNECTED")
        initial_contactid = event['detail']['initialContactId']
        primarykey = get_primarykey(ddb_table, initial_contactid)
        update_callback(ddb_table, primarykey, contactid, "completed")