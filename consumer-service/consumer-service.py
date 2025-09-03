import logging

# uvicorn consumer-service:app --port 8003 --reload
from fastapi import FastAPI
from confluent_kafka import Consumer
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from prometheus_client import Counter, Histogram, make_asgi_app, REGISTRY
import time
import smtplib
import json
import threading
import time
import os

# --- OpenTelemetry Tracing Setup ---
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.trace import SpanKind
from opentelemetry.propagate import extract

resource = Resource(attributes={"service.name": "consumer-service"})
trace.set_tracer_provider(TracerProvider(resource=resource))
jaeger_exporter = JaegerExporter(
    agent_host_name="jaeger",
    agent_port=6831,
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
consumed_messages_total = Counter('consumed_messages_total', 'Total number of messages consumed', ['category'])
email_send_time = Histogram('email_send_seconds', 'Time spent sending emails', ['category'])
email_errors_total = Counter('email_errors_total', 'Total number of email sending errors', ['category'])
kafka_errors_total = Counter('kafka_errors_total', 'Total number of Kafka consumer errors', ['category'])
end_to_end_latency = Histogram(
    'email_end_to_end_latency_seconds',
    'End-to-end latency from request to email sent',
    ['category'],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float('inf'))  # Define buckets in seconds
)

# Initialize metrics with default values
for category in ['marketing', 'security', 'product_updates']:
    consumed_messages_total.labels(category=category)._value.set(0)  # Initialize counter
    email_send_time.labels(category=category)  # Initialize histogram
    email_errors_total.labels(category=category)._value.set(0)  # Initialize counter
    end_to_end_latency.labels(category=category)  # Initialize histogram

# Get SMTP configuration from environment variables
SMTP_HOST = os.getenv('SMTP_HOST', 'mailhog')
SMTP_PORT = int(os.getenv('SMTP_PORT', '1025'))

# Kafka configuration
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'group.id': os.getenv('KAFKA_GROUP_ID', 'python-group-1'),
    'auto.offset.reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
}

consumer = Consumer(conf)
topic = os.getenv('KAFKA_TOPIC', 'email-notifs')

@app.get("/health")
def health_check():
    return {"status": "healthy"}

def send_email(to_email, subject, message, category, request_timestamp=None):
    with tracer.start_as_current_span("send_email", kind=SpanKind.CLIENT) as span:
        span.set_attribute("email.recipient", to_email)
        span.set_attribute("email.category", category)
        
        start_time = time.time()
        
        try:
            msg = MIMEMultipart()
            msg['From'] = 'notification@example.com'
            msg['To'] = to_email
            msg['Subject'] = subject

            msg.attach(MIMEText(message, 'plain'))

            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
                smtp.send_message(msg)
                span.add_event("email.sent")
                
                # Record email send time
                email_send_time.labels(category=category).observe(time.time() - start_time)
                
                # Calculate and record end-to-end latency if request_timestamp is available
                if request_timestamp:
                    end_to_end_latency_value = time.time() - float(request_timestamp)
                    end_to_end_latency.labels(category=category).observe(end_to_end_latency_value)
                    logger.info(f"Recorded end-to-end latency in consumer for category {category}: {end_to_end_latency_value} seconds")
                    logger.info(f"Current metric value: {REGISTRY.get_sample_value('email_end_to_end_latency_seconds_count', {'category': category})}")
                else:
                    logger.warning(f"No request_timestamp available for message to {to_email}")

                return True

        except Exception as e:
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            span.record_exception(e)
            email_errors_total.labels(category=category).inc()
            logger.error(f"Failed to send email: {str(e)}")
            return False

def generate_iso8601_ns():
    # Get current time in total nanoseconds since epoch
    total_ns = time.time_ns()

    # Split into whole seconds and the sub‑second nanosecond part
    seconds = total_ns // 1_000_000_000
    nanos   = total_ns % 1_000_000_000

    # Build a UTC datetime from the whole seconds
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)

    # Format date + time (up to seconds)
    base = dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Format nanoseconds as 9 digits, then pad to 12 if desired:
    # here we append three zeros to get 12 fractional digits
    frac = f"{nanos:09d}000"

    # Combine and add Z suffix
    return f"{base}.{frac}Z"

def consume_messages():
    consumer.subscribe([topic])
    logger.info(f"Consumer started. Listening to topic: {topic}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                with tracer.start_as_current_span("kafka_error", kind=SpanKind.CONSUMER) as span:
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(msg.error())))
                    kafka_errors_total.inc()
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                # Extract trace context from message headers
                logger.info(f"Total Msg size: {len(msg)}")
                logger.info(f"Msg value size: {len(msg.value())}")
                headers = {k: v.decode('utf-8') for k, v in msg.headers() or []}
                logger.info(f"Headers size: {len(headers)}")
                context = extract(headers)

                with tracer.start_as_current_span(
                    "process_message",
                    context=context,
                    kind=SpanKind.CONSUMER
                ) as span:
                    # Add message metadata as span attributes
                    span.set_attribute("messaging.system", "kafka")
                    span.set_attribute("messaging.destination", topic)
                    span.set_attribute("messaging.destination_kind", "topic")
                    
                    data = json.loads(msg.value().decode('utf-8'))
                    to_email = data.get('email')
                    subject = data.get('subject')
                    message = data.get('message')
                    category = data.get('category', 'unknown')
                    request_timestamp = data.get('request_timestamp')
                    request_id = data.get('request_id')

                    logger.info(f"Received message - Category: {category}, Request ID: {request_id}, Timestamp: {request_timestamp}")

                    span.set_attribute("messaging.category", category)
                    span.set_attribute("messaging.recipient", to_email)
                    span.set_attribute("messaging.request_id", request_id)

                    if all([to_email, subject, message]):
                        consumed_messages_total.labels(category=category).inc()
                        send_email(to_email, subject, message, category, request_timestamp)
                        
                    else:
                        span.set_status(trace.Status(trace.StatusCode.ERROR, "Missing required email fields"))
                        logger.warning("Missing required email fields")

            except json.JSONDecodeError as e:
                with tracer.start_as_current_span("json_decode_error", kind=SpanKind.CONSUMER) as span:
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    logger.error("Failed to decode message")
            except Exception as e:
                with tracer.start_as_current_span("message_processing_error", kind=SpanKind.CONSUMER) as span:
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    logger.error(f"Error processing message: {str(e)}")
    except KeyboardInterrupt:
        logger.info("Consumer stopped")
    finally:
        consumer.close()

@app.on_event("startup")
def startup_event():
    threading.Thread(target=consume_messages, daemon=True).start()