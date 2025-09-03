# uvicorn user-preference-service:app --port 8000 --reload
from fastapi import FastAPI, Query, HTTPException
from typing import List
from pymongo import MongoClient
from pydantic import BaseModel
from urllib.parse import quote_plus
from prometheus_client import Counter, Histogram, Gauge, make_asgi_app, REGISTRY
import logging
import time

# --- OpenTelemetry Tracing Setup ---
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

resource = Resource(attributes={"service.name": "user-preference-service"})
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
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI()
FastAPIInstrumentor().instrument_app(app)
RequestsInstrumentor().instrument()

# Add Prometheus metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Define metrics
subscriber_requests_total = Counter('subscriber_requests_total', 'Total number of subscriber requests', ['category'])
subscriber_processing_time = Histogram('subscriber_processing_seconds', 'Time spent processing subscriber requests', ['category'])
subscriber_errors_total = Counter('subscriber_errors_total', 'Total number of subscriber processing errors', ['category'])
subscriber_count = Gauge('subscriber_count_total', 'Current number of subscribers', ['category'])

# Initialize metrics with default values
for category in ['marketing', 'security', 'product_updates']:
    subscriber_requests_total.labels(category=category)
    subscriber_processing_time.labels(category=category)
    subscriber_errors_total.labels(category=category)
    subscriber_count.labels(category=category).set(0)

username = quote_plus("pranav0706")
password = quote_plus("Pokemon123")

uri = f"mongodb+srv://{username}:{password}@cluster0.j5ickkr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri)
db = client["user-preferences"]
users_collection = db["preferences"]

# Response Model
class UserChannels(BaseModel):
    user_id: str
    channels: List[str]

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.get("/subscribers", response_model=List[UserChannels])
def get_subscribed_users(category: str = Query(...)):
    start_time = time.time()
    with tracer.start_as_current_span("get_subscribed_users"):
        try:
            query = {f"categories.{category}": True}
            users = users_collection.find(query)

            # Count subscribers per category
            result = []
            subscriber_count_value = 0
            for user in users:
                preferred_channels = [
                    channel for channel, value in user.get("channels", {}).items() if value
                ]
                result.append(UserChannels(
                    user_id=user["_id"],
                    channels=preferred_channels
                ))
                subscriber_count_value += 1

            subscriber_requests_total.labels(category=category).inc()
            subscriber_processing_time.labels(category=category).observe(time.time() - start_time)
            subscriber_count.labels(category=category).set(subscriber_count_value)
            
            return result
            
        except Exception as e:
            subscriber_errors_total.labels(category=category).inc()
            logger.error(f"Error fetching subscribers: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))