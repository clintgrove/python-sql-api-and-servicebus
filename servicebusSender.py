import json
import asyncio
import subprocess
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage

constants = {
    "SBUS_RESOURCE_GROUP": "ServiceBusLearning",
    "SBUS_NAMESPACE": "sbuscl2",
    "QUEUE_NAME": "que1-cl"
}

# Fetch the Service Bus connection string using Azure CLI
command = f"az servicebus namespace authorization-rule keys list --resource-group {constants['SBUS_RESOURCE_GROUP']} --namespace-name {constants['SBUS_NAMESPACE']} --name RootManageSharedAccessKey --query primaryConnectionString --output tsv"
SERVICEBUS_CONN_STR = subprocess.check_output(command, shell=True, text=True).strip()

async def send_messages(start_id: int, end_id: int):
    async with ServiceBusClient.from_connection_string(SERVICEBUS_CONN_STR) as client:
        sender = client.get_queue_sender(queue_name=constants['QUEUE_NAME'])
        async with sender:
            for i in range(start_id, end_id):
                try:
                    person_id = i
                    email = f"user{i}@example.com"
                    message_content = json.dumps({"PersonID": str(person_id), "Email": email})
                    message = ServiceBusMessage(message_content)
                    await sender.send_messages(message)
                except Exception as e:
                    print(f"Failed to send message {i}: {e}")

async def main():
    total_messages = 10
    processors = 10
    messages_per_processor = total_messages // processors

    tasks = []
    for i in range(processors):
        start_id = i * messages_per_processor
        end_id = start_id + messages_per_processor
        if i == processors - 1:
            end_id = total_messages  # Ensure the last processor sends all remaining messages
        tasks.append(send_messages(start_id, end_id))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
