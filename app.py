import json
import os
import subprocess
from azure.servicebus import ServiceBusClient
from azure import identity
import pyodbc, struct
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Union

# Based on working in this document https://learn.microsoft.com/en-us/azure/azure-sql/database/azure-sql-python-quickstart
# Create an Azure Web app. Have API endpoint to receive messages from Service Bus and insert into Azure SQL DB

constants = {
    "SBUS_RESOURCE_GROUP": "ServiceBusLearning",
    "SBUS_NAMESPACE": "sbuscl2",
}

connection_string = os.environ["AZURE_SQL_CONNECTIONSTRING"]

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
    with get_database_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM PeopleTable")

        for row in cursor.fetchall():
            print(row.PersonID, row.Email)
            rows.append(f"{row.PersonID}, {row.Email}")
    return rows

@app.post("/person")
def create_person(item: Person):
    with get_database_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"INSERT INTO PeopleTable (PersonID, Email) VALUES (?, ?)", item.PersonID, item.Email)
        conn.commit()

    return item

@app.post("/personservicebus/")
def process_messages_from_servicebus_to_sql():
    
    try:
        # Retrieve the Service Bus connection string using Azure CLI
        command = f"az servicebus namespace authorization-rule keys list --resource-group {constants['SBUS_RESOURCE_GROUP']} --namespace-name {constants['SBUS_NAMESPACE']} --name RootManageSharedAccessKey --query primaryConnectionString --output tsv"
        SERVICEBUS_CONN_STR = subprocess.check_output(command, shell=True).decode().strip()

        # Create ServiceBusClient instance
        servicebus_client = ServiceBusClient.from_connection_string(
            conn_str=SERVICEBUS_CONN_STR,
            logging_enable=True
        )

        # Receiving the message
        with servicebus_client:
            receiver = servicebus_client.get_queue_receiver(queue_name="que1-cl")
            with receiver:
                # Initialize a list to store all received messages
                all_received_msgs = []
                # Loop to continuously receive messages
                while True:
                    received_msgs = receiver.receive_messages(max_message_count=1000, max_wait_time=5)
                    if not received_msgs:
                        # Break the loop if no more messages are received
                        break
                    else:
                        for msg in received_msgs:
                            # Assuming msg.body is a byte string
                            body = ''.join([chunk.decode('utf-8') for chunk in msg.body])
                            print(f"Received: {body}")
                            all_received_msgs.append(body)
                            receiver.complete_message(msg)

        # SQL command to insert a row into your table
        sql_command_text = "INSERT INTO PeopleTable (PersonID, Email) VALUES (?, ?)"
        records_to_insert = []  # Initialize an empty list to hold records for batch insertion

        for body in all_received_msgs:
            try:
                # Use the message as a parameter for the SQL command
                message_obj = json.loads(body)
                person_id = message_obj["PersonID"]
                email = message_obj["Email"]

                # Append each record as a tuple into the list
                records_to_insert.append((person_id, email))

            except json.JSONDecodeError as json_error:
                print(f"JSON decoding failed: {json_error}")

        # Check if there are records to insert
        rows_affected = records_to_insert 
        if records_to_insert:
            try:
                # Use the get_database_connection function to establish a connection
                logtel = print(f"Starting the insert process to SQL table.")
                connection = get_database_connection()
                if connection is None:
                    raise Exception("Failed to connect to the database.")
                cursor = connection.cursor()

                # Use executemany to insert all records in a single operation
                cursor.executemany(sql_command_text, records_to_insert)
                connection.commit()

                rows_affected = cursor.rowcount
                print(f"{rows_affected} rows were inserted.")

                cursor.close()
                connection.close()
            except Exception as e:
                print(f"An error occurred during batch insertion: {str(e)}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    return rows_affected


def get_database_connection():
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by Microsoft in msodbcsql.h
    
    conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn

