import pika
import json
import os
import random
from datetime import datetime
from time import sleep
from pprint import pprint



QUEUE_NAME      = os.environ.get("QUEUE_NAME", "worker")
SUBSCRIBE_SERVER  = os.environ.get("SUBSCRIBE_SERVER", "localhost")
USERNAME        = os.environ.get("AMQP_USER", "guest")
PASSWORD        = os.environ.get("AMQP_PASSWORD", "guest")

subscribe_servers = SUBSCRIBE_SERVER.split(',')

count = len(subscribe_servers)

server = random.choice(subscribe_servers).split(":")[0]
port = random.choice(subscribe_servers).split(":")[1]

connection_parameter = pika.ConnectionParameters(
    server,
    port=port,
    heartbeat=600,
    blocked_connection_timeout=300,
    credentials=pika.PlainCredentials(
        USERNAME,
        PASSWORD
    )
)

connection = pika.BlockingConnection(connection_parameter)
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)


def callback(ch, method, properties, body):
    print("-"*100)
    print(f"Received in  queue_name: {QUEUE_NAME}")
    print(f"chanel: {ch}")
    print(f"method: {method}")

    data = json.loads(body)
    # sleep(1)
    payload = {
        # "data": data['content_html'],
        # "recipient": data['to_recipient_list'],
        "subject": data['subject'],
        "task_id": data['task_id'],
        # "recipient_list": data["to_recipient_list"],
        "time_stamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    # payload = {
    #     "data": data,
    #     "channel": ch,
    #     "method": method,
    #     "properties": properties,
    # }

    pprint(data)

    if properties.content_type == 'created':
        print("created")
    elif properties.content_type == 'update':
        print("updated")
    elif properties.content_type == 'delete':
        print("deleted")


channel.basic_consume(
    queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

pprint("Started Consuming...")
pprint({
    "queue_name": QUEUE_NAME,
    "server": server,
    "port": port,
    "username": USERNAME,
    "password": PASSWORD
})

sleep(1)

print("-"*100)

channel.start_consuming()