# uvicorn scheduler-service:app --port 8001 --reload
from fastapi import FastAPI, Query, HTTPException
from typing import List
from pymongo import MongoClient
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.base import JobLookupError
from datetime import datetime, timezone
from prometheus_client import Counter, Histogram, Gauge, make_asgi_app, REGISTRY
import requests
import uuid
import logging
import os
import time

# --- OpenTelemetry Tracing Setup ---
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

resource = Resource(attributes={"service.name": "scheduler-service"})
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
scheduled_jobs_total = Counter('scheduled_jobs_total', 'Total number of scheduled jobs', ['category'])
job_processing_time = Histogram('job_processing_seconds', 'Time spent processing job scheduling', ['category'])
job_errors_total = Counter('job_errors_total', 'Total number of job scheduling errors', ['category'])
active_jobs = Gauge('active_jobs', 'Number of currently active scheduled jobs', ['category'])

# Initialize metrics with default values
for category in ['marketing', 'security', 'product_updates']:
    scheduled_jobs_total.labels(category=category)
    job_processing_time.labels(category=category)
    job_errors_total.labels(category=category)
    active_jobs.labels(category=category).set(0)

scheduler = BackgroundScheduler()
scheduler.start()

NOTIFICATION_SERVICE_URL=os.getenv('NOTIFICATION_SERVICE_URL')

@app.get("/health")
def health_check():
    return {"status": "healthy"}

def call_subscribers_api(job_id, category, subject, body):
    try:
        payload = {
            "category": category,
            "subject": subject,
            "message": body,
            "date_time": None  # explicitly set to null
        }

        response = requests.post(
            f"{NOTIFICATION_SERVICE_URL}/send-email/",
            json=payload
        )

        logger.info(f"[{datetime.now()}] Job {job_id} triggered /send-email/")
        logger.info(f"Response: {response.status_code} - {response.text}")

    finally:
        # Clean up job after it's run
        try:
            scheduler.remove_job(job_id)
            active_jobs.labels(category=category).dec()
        except JobLookupError:
            pass

# --------- Request model ---------
class ScheduleRequest(BaseModel):
    subject: str
    message: str
    category: str
    date_time: datetime


# --------- Endpoint to schedule the job ---------
@app.post("/schedule_notification")
def schedule_notification(req: ScheduleRequest):
    start_time = time.time()
    with tracer.start_as_current_span("schedule_notification"):
        try:
            if req.date_time <= datetime.now(timezone.utc):
                job_errors_total.labels(category=req.category).inc()
                raise HTTPException(status_code=400, detail="Scheduled time must be in the future")

            job_id = str(uuid.uuid4())

            scheduler.add_job(
                call_subscribers_api,
                trigger="date",
                run_date=req.date_time,
                args=[job_id, req.category, req.subject, req.message],  # Pass to job
                id=job_id,
                replace_existing=False
            )

            scheduled_jobs_total.labels(category=req.category).inc()
            job_processing_time.labels(category=req.category).observe(time.time() - start_time)
            active_jobs.labels(category=req.category).inc()

            return {
                "message": "Notification job scheduled",
                "job_id": job_id,
                "scheduled_for": req.date_time.isoformat()
            }
        
        except Exception as e:
            job_errors_total.labels(category=req.category).inc()
            logger.error(f"Error scheduling job: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))