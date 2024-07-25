import json
import os
import subprocess
from azure.servicebus import ServiceBusClient
from azure import identity
import pyodbc, struct
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Union
from azure.core.exceptions import AzureError
import logging
from azure.identity import DefaultAzureCredential
from azure.mgmt.servicebus import ServiceBusManagementClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Based on working in this document https://learn.microsoft.com/en-us/azure/azure-sql/database/azure-sql-python-quickstart
# Create an Azure Web app. Have API endpoint to receive messages from Service Bus and insert into Azure SQL DB

constants = {
    "SBUS_RESOURCE_GROUP": "ServiceBusLearning",
    "SBUS_NAMESPACE": "sbuscl2",
}

connection_string = os.environ["AZURE_SQL_CONNECTIONSTRING"]
azure_subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

class Person(BaseModel):
    PersonID: str
    Email: Union[str, None] = None

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/all")
def get_persons():
    rows = []
    try:
        with get_database_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM PeopleTable")

            for row in cursor.fetchall():
                logger.info(f"Fetched row: {row.PersonID}, {row.Email}")
                rows.append(f"{row.PersonID}, {row.Email}")
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch data from the database.")
    
    return rows

@app.post("/person")
def create_person(item: Person):
    try:
        with get_database_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO PeopleTable (PersonID, Email) VALUES (?, ?)", item.PersonID, item.Email)
            conn.commit()
    except Exception as e:
        logger.error(f"Error creating person: {e}")
        raise HTTPException(status_code=500, detail="Failed to create person in the database.")

    return item

@app.post("/personservicebus/")
def process_messages_from_servicebus_to_sql():
    rows_affected = 0
    
    try:
        SERVICEBUS_CONN_STR = get_servicebus_connection_string()
        
        servicebus_client = ServiceBusClient.from_connection_string(
            conn_str=SERVICEBUS_CONN_STR,
            logging_enable=True
        )

        # Receiving the message
        with servicebus_client:
            receiver = servicebus_client.get_queue_receiver(queue_name="que1-cl")
            with receiver:
                all_received_msgs = []
                while True:
                    received_msgs = receiver.receive_messages(max_message_count=1000, max_wait_time=5)
                    if not received_msgs:
                        break
                    else:
                        for msg in received_msgs:
                            body = ''.join([chunk.decode('utf-8') for chunk in msg.body])
                            logger.info(f"Received: {body}")
                            all_received_msgs.append(body)
                            receiver.complete_message(msg)

        sql_command_text = "INSERT INTO PeopleTable (PersonID, Email) VALUES (?, ?)"
        records_to_insert = []

        for body in all_received_msgs:
            try:
                message_obj = json.loads(body)
                person_id = message_obj["PersonID"]
                email = message_obj["Email"]
                records_to_insert.append((person_id, email))
            except json.JSONDecodeError as json_error:
                logger.error(f"JSON decoding failed: {json_error}")

        if records_to_insert:
            try:
                logger.info("Starting the insert process to SQL table.")
                connection = get_database_connection()
                if connection is None:
                    raise Exception("Failed to connect to the database.")
                cursor = connection.cursor()
                cursor.executemany(sql_command_text, records_to_insert)
                connection.commit()

                rows_affected = cursor.rowcount
                logger.info(f"{rows_affected} rows were inserted.")

                cursor.close()
                connection.close()
            except Exception as e:
                logger.error(f"An error occurred during batch insertion: {e}")
                raise HTTPException(status_code=500, detail=f"An error occurred during batch insertion: {e}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

    return {"rows_affected": rows_affected}


def get_database_connection():
    try:
        credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
        token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by Microsoft in msodbcsql.h
        
        conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise

def get_servicebus_connection_string():
    try:
        credential = DefaultAzureCredential()
        client = ServiceBusManagementClient(credential, subscription_id=azure_subscription_id)
        keys = client.namespaces.list_keys(
            resource_group_name=constants['SBUS_RESOURCE_GROUP'],
            namespace_name=constants['SBUS_NAMESPACE'],
            authorization_rule_name='RootManageSharedAccessKey'
        )
        return keys.primary_connection_string
    except Exception as e:
        logger.error(f"Failed to retrieve Service Bus connection string: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve Service Bus connection string: {e}")



