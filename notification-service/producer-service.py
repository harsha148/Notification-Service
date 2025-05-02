from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
import json

app = FastAPI()

conf = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'VKFP3YCN4IHTSFIJ',
    'sasl.password': '1dhelgxuKXmqFbKaFMweGAIL8DfTVMe8G20ztLFAEUjQSEk7He7NUGjIWiQx33a5'
}

producer = Producer(conf)
TOPIC = 'DIS-Email-Notification'

class EmailRequest(BaseModel):
    email: str
    subject: str
    message: str

@app.post("/send-email/")
def send_email(request: EmailRequest):
    try:
        data = request.dict()
        producer.produce(TOPIC, json.dumps(data).encode('utf-8'))
        producer.flush()
        return {"status": "Message sent to Kafka"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))