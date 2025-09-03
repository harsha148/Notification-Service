# uvicorn producer-service:app --port 8002 --reload
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
from datetime import datetime, timezone
from prometheus_client import Counter, Histogram, make_asgi_app, REGISTRY
import json
import requests
import os
import time
import logging

# --- OpenTelemetry Tracing Setup ---
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.trace import SpanKind
from opentelemetry.propagate import inject

resource = Resource(attributes={"service.name": "notification-service"})
trace.set_tracer_provider(TracerProvider(resource=resource))
jaeger_exporter = JaegerExporter(
    agent_host_name=os.getenv("JAEGER_AGENT_HOST", "jaeger"),
    agent_port=int(os.getenv("JAEGER_AGENT_PORT", 6831)),
)
span_processor = BatchSpanProcessor(jaeger_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)
tracer = trace.get_tracer(__name__)

# --- End OpenTelemetry Setup ---

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI()
FastAPIInstrumentor().instrument_app(app)
RequestsInstrumentor().instrument()

# Add Prometheus metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Define metrics
email_requests_total = Counter('email_requests_total', 'Total number of email requests', ['category'])
email_processing_time = Histogram('email_processing_seconds', 'Time spent processing email requests', ['category'])
email_errors_total = Counter('email_errors_total', 'Total number of email processing errors', ['category'])
# end_to_end_latency = Histogram(
#     'email_end_to_end_latency_seconds',
#     'End-to-end latency from request to email sent',
#     ['category'],
#     buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float('inf'))  # Define buckets in seconds
# )

# Initialize metrics with default values
for category in ['marketing', 'security', 'product_updates']:
    email_requests_total.labels(category=category)._value.set(0)  # Initialize counter
    email_processing_time.labels(category=category)  # Initialize histogram
    email_errors_total.labels(category=category)._value.set(0)  # Initialize counter
    # end_to_end_latency.labels(category=category)  # Initialize histogram

# Kafka Config
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
}

producer = Producer(conf)
TOPIC = os.getenv('KAFKA_TOPIC')

# Service URLs
USER_PREFERENCE_SERVICE_URL = os.getenv('USER_PREFERENCE_SERVICE_URL')
SCHEDULER_SERVICE_URL = os.getenv('SCHEDULER_SERVICE_URL')

# Request Model
class EmailRequest(BaseModel):
    subject: str
    message: str
    category: str
    date_time: datetime | None = None  # Optional

@app.post("/send-email/")
def send_email(request: EmailRequest):
    start_time = time.time()
    with tracer.start_as_current_span("send_email_endpoint"):
        try:
            # Check if future date is valid (if provided)
            if request.date_time:
                if request.date_time <= datetime.now(timezone.utc):
                    email_errors_total.labels(category=request.category).inc()
                    raise HTTPException(status_code=400, detail="date_time must be in the future")

                # Schedule the job
                try:
                    with tracer.start_as_current_span("schedule_notification", kind=SpanKind.CLIENT) as span:
                        headers = {}
                        inject(headers)  # Inject trace context into headers
                        response = requests.post(
                            f"{SCHEDULER_SERVICE_URL}/schedule_notification", 
                            json={
                                "date_time": request.date_time.isoformat(),
                                "category": request.category,
                                "subject": request.subject,
                                "message": request.message
                            },
                            headers=headers
                        )

                    if response.status_code != 200:
                        email_errors_total.labels(category=request.category).inc()
                        raise HTTPException(status_code=response.status_code, detail=response.text)

                    email_requests_total.labels(category=request.category).inc()
                    email_processing_time.labels(category=request.category).observe(time.time() - start_time)
                    
                    return {
                        "status": "Notification job scheduled",
                        "job_id": response.json().get("job_id")
                    }

                except requests.RequestException as e:
                    email_errors_total.labels(category=request.category).inc()
                    raise HTTPException(status_code=500, detail=f"Failed to schedule job: {str(e)}")

            # If no date_time, send immediately to current subscribers
            try:
                with tracer.start_as_current_span("get_subscribers", kind=SpanKind.CLIENT) as span:
                    headers = {}
                    inject(headers)  # Inject trace context into headers
                    subscriber_response = requests.get(
                        f"{USER_PREFERENCE_SERVICE_URL}/subscribers", 
                        params={"category": request.category},
                        headers=headers
                    )

                if subscriber_response.status_code != 200:
                    email_errors_total.labels(category=request.category).inc()
                    raise HTTPException(status_code=subscriber_response.status_code, detail="Failed to fetch subscribers")

                subscribers = subscriber_response.json()

                for subscriber in subscribers:
                    with tracer.start_as_current_span("produce_message", kind=SpanKind.PRODUCER) as span:
                        span.set_attribute("messaging.system", "kafka")
                        span.set_attribute("messaging.destination", TOPIC)
                        span.set_attribute("messaging.destination_kind", "topic")
                        span.set_attribute("messaging.recipient", subscriber["user_id"])
                        span.set_attribute("messaging.category", request.category)

                        # Inject trace context into headers
                        headers = {}
                        inject(headers)
                        kafka_headers = [(k, str(v).encode('utf-8')) for k, v in headers.items()]
                        
                        current_time = time.time()
                        data = {
                            "email": subscriber["user_id"],
                            "subject": request.subject,
                            "message": request.message,
                            "category": request.category,
                            "request_timestamp": current_time,
                            "request_id": str(hash(f"{current_time}{subscriber['user_id']}{request.subject}"))
                        }
                        json_str = json.dumps(data)
                        msg_bytes = json_str.encode('utf-8')
                        logger.info(f"Message size: {len(msg_bytes)}")
                        logger.info(f"Sending message with timestamp: {current_time} for category: {request.category}")
                        producer.produce(
                            TOPIC,
                            json.dumps(data).encode('utf-8'),
                            headers=kafka_headers
                        )

                producer.flush()
                
                email_requests_total.labels(category=request.category).inc()
                email_processing_time.labels(category=request.category).observe(time.time() - start_time)
                
                return {"status": f"Messages sent to {len(subscribers)} subscribers"}

            except Exception as e:
                email_errors_total.labels(category=request.category).inc()
                raise HTTPException(status_code=500, detail=str(e))
                
        except Exception as e:
            email_errors_total.labels(category=request.category).inc()
            raise e
