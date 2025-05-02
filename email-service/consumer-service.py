# consumer_service.py
from fastapi import FastAPI
from confluent_kafka import Consumer
from email.message import EmailMessage
import smtplib
import json
import threading
import time

app = FastAPI()

conf = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'username',
    'sasl.password': 'password',
    'group.id': 'email-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['DIS-Email-Notification'])

def send_email(to, subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = 'sender@example.com'
    msg['To'] = to
    with smtplib.SMTP("ec2-3-148-254-82.us-east-2.compute.amazonaws.com", 1025) as smtp:
        smtp.send_message(msg)

def consume_messages():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        data = json.loads(msg.value().decode('utf-8'))
        send_email(data['email'], data['subject'], data['message'])

@app.on_event("startup")
def startup_event():
    threading.Thread(target=consume_messages, daemon=True).start()