# aica-lead-crm-platform
Taking leads and dealing website

# AICA Lead CRM Platform

AICA Lead CRM Platform is a full-stack business automation prototype for lead capture, scoring, CRM workflow tracking, Telegram alerts, and real-time admin dashboard updates.

This project was designed for service businesses that need to collect customer requests, estimate potential deal value, prioritize high-value leads, and respond quickly.

## Features

- Lead capture API with FastAPI
- Async PostgreSQL database layer
- Lead scoring service
- Lead activity tracking
- Telegram alert automation
- Celery background workers
- Redis queue support
- WebSocket-based real-time dashboard updates
- Docker-based deployment structure
- Admin dashboard concept with analytics cards
- Mobile-first funnel UI concept

## Architecture

```text
aica_lead_crm/
├── backend/
│   ├── app/
│   │   ├── core/
│   │   ├── api/
│   │   ├── db/
│   │   ├── schemas/
│   │   ├── services/
│   │   └── worker/
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── app/
│   │   ├── components/
│   │   └── lib/
│   └── Dockerfile
└── docker-compose.yml
````

## Backend Stack

* Python
* FastAPI
* SQLAlchemy
* PostgreSQL
* Redis
* Celery
* WebSockets
* Docker

## Frontend Stack

* Next.js
* React
* Tailwind CSS
* WebSocket client
* Mobile-first UI components

## Core Workflow

1. Customer submits lead form.
2. Backend validates and stores lead.
3. Lead score is calculated based on project area, project type, and estimated value.
4. Lead activity is recorded.
5. Telegram alert is sent to the business owner.
6. Missed-lead follow-up job is scheduled.
7. Admin dashboard receives real-time WebSocket update.

## Lead Scoring Logic

The scoring model estimates lead priority using:

* Project area
* Project type
* Estimated project value
* Behavior / urgency signal

Example tiers:

```text
VIP     = high-value lead
MEDIUM  = mid-value lead
SMALL   = lower-value lead
```

## Example Use Cases

* Construction service lead management
* Interior design quote requests
* Real estate project inquiries
* B2B service sales funnel
* Local business CRM automation
* AI-assisted sales operations

## Status

Prototype / portfolio project.

This project demonstrates backend architecture, business logic separation, async API design, CRM workflow thinking, and scalable deployment planning.

````

---

# `backend/app/db/models.py`

```python
from datetime import datetime

from sqlalchemy import Column, String, Integer, Float, DateTime, ForeignKey, Index
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Lead(Base):
    __tablename__ = "leads"

    phone = Column(String, primary_key=True, index=True)
    task_id = Column(String, nullable=True)

    project_type = Column(String, index=True)
    project_area = Column(Integer)
    project_value = Column(String, index=True)

    estimated_price_min = Column(Float)
    estimated_price_max = Column(Float)

    lead_score = Column(Integer, index=True)
    status = Column(String, default="NEW", index=True)
    deal_value = Column(Float, default=0.0)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    activities = relationship(
        "LeadActivity",
        back_populates="lead",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_leads_status_created", "status", "created_at"),
    )


class LeadActivity(Base):
    __tablename__ = "lead_activities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    phone = Column(String, ForeignKey("leads.phone"))

    action = Column(String)  # CREATED, CONTACTED, MISSED, WON, LOST
    note = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)

    lead = relationship("Lead", back_populates="activities")
````

---

# `backend/app/services/scoring.py`

```python
class LeadScoringService:
    @staticmethod
    def calculate_score(
        area: int,
        project_type: str,
        estimate_max: float,
        response_time_mins: int = 0,
    ) -> int:
        score = 0

        # Area weight: 40%
        if area >= 400:
            score += 40
        elif area >= 200:
            score += 25
        else:
            score += 10

        # Project type weight: 30%
        if project_type == "Барилгын компани":
            score += 30
        elif project_type == "Орон сууц / Хотхон":
            score += 20
        else:
            score += 10

        # Estimated value weight: 20%
        if estimate_max >= 100_000_000:
            score += 20
        elif estimate_max >= 50_000_000:
            score += 15
        else:
            score += 5

        # Behavior / urgency weight: 10%
        if response_time_mins <= 5:
            score += 10
        elif response_time_mins <= 30:
            score += 5

        return min(score, 100)

    @staticmethod
    def determine_value_tier(area: int) -> str:
        if area >= 400:
            return "VIP"
        if area >= 200:
            return "MEDIUM"
        return "SMALL"
```

---

# `backend/app/worker/tasks.py`

```python
import os
import requests

from celery import Celery

celery_app = Celery("aica_workers")

celery_app.conf.broker_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
celery_app.conf.result_backend = os.getenv("REDIS_URL", "redis://redis:6379/0")

celery_app.conf.task_routes = {
    "app.worker.tasks.send_telegram_alert": {"queue": "high_priority"},
    "app.worker.tasks.mark_as_missed": {"queue": "high_priority"},
    "app.worker.tasks.send_auto_sms": {"queue": "normal"},
}


@celery_app.task(bind=True, max_retries=3, default_retry_delay=10)
def send_telegram_alert(
    self,
    phone: str,
    area: int,
    est_max: float,
    score: int,
    vip_status: str,
):
    try:
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if not token or not chat_id:
            return {"ok": False, "reason": "telegram_env_missing"}

        icon = "🚨 VIP LEAD" if vip_status == "VIP" else "🔥 NEW LEAD"

        msg = (
            f"{icon}\n"
            f"Phone: {phone}\n"
            f"Area: {area} m2\n"
            f"Estimate: ~{est_max:,.0f} MNT\n"
            f"Score: {score}/100"
        )

        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": msg},
            timeout=5,
        )

        response.raise_for_status()
        return {"ok": True}

    except Exception as exc:
        raise self.retry(exc=exc)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=10)
def mark_as_missed(self, phone: str):
    # Placeholder for CRM missed-lead workflow.
    # In production, this should update DB status if lead is still NEW.
    return {"ok": True, "phone": phone, "status": "MISSED_CHECKED"}


@celery_app.task(bind=True, max_retries=3, default_retry_delay=10)
def send_auto_sms(self, phone: str, message: str):
    # Placeholder for SMS provider integration.
    return {"ok": True, "phone": phone}
```

---

# `backend/app/schemas/leads.py`

```python
from pydantic import BaseModel, Field


class LeadCreateSchema(BaseModel):
    phone: str = Field(..., min_length=6, max_length=30)
    project_type: str
    area_m2: int = Field(..., ge=1)
    estimate_min: float = Field(..., ge=0)
    estimate_max: float = Field(..., ge=0)


class LeadResponseSchema(BaseModel):
    status: str
    lead_score: int
    project_value: str
```

---

# `backend/app/api/leads.py`

```python
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_async_db
from app.db.models import Lead, LeadActivity
from app.schemas.leads import LeadCreateSchema, LeadResponseSchema
from app.services.scoring import LeadScoringService
from app.worker.tasks import send_telegram_alert, mark_as_missed

router = APIRouter(prefix="/api/leads", tags=["leads"])

active_websockets: list[WebSocket] = []


@router.post("/", response_model=LeadResponseSchema)
async def create_lead(
    req: LeadCreateSchema,
    db: AsyncSession = Depends(get_async_db),
):
    score = LeadScoringService.calculate_score(
        area=req.area_m2,
        project_type=req.project_type,
        estimate_max=req.estimate_max,
    )

    project_value = LeadScoringService.determine_value_tier(req.area_m2)

    new_lead = Lead(
        phone=req.phone,
        project_type=req.project_type,
        project_area=req.area_m2,
        project_value=project_value,
        estimated_price_min=req.estimate_min,
        estimated_price_max=req.estimate_max,
        lead_score=score,
    )

    activity = LeadActivity(
        phone=req.phone,
        action="CREATED",
        note="Lead created from funnel form.",
    )

    db.add(new_lead)
    db.add(activity)
    await db.commit()

    send_telegram_alert.apply_async(
        args=(req.phone, req.area_m2, req.estimate_max, score, project_value),
        queue="high_priority",
    )

    mark_as_missed.apply_async(
        args=(req.phone,),
        countdown=300,
        queue="high_priority",
    )

    dead_sockets = []

    for ws in active_websockets:
        try:
            await ws.send_json({
                "action": "NEW_LEAD",
                "phone": req.phone,
                "score": score,
                "project_value": project_value,
            })
        except Exception:
            dead_sockets.append(ws)

    for ws in dead_sockets:
        if ws in active_websockets:
            active_websockets.remove(ws)

    return {
        "status": "success",
        "lead_score": score,
        "project_value": project_value,
    }


@router.websocket("/ws/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    await websocket.accept()
    active_websockets.append(websocket)

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        if websocket in active_websockets:
            active_websockets.remove(websocket)
```

---

# `backend/app/db/session.py`

```python
import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://aica_user:aica_password@db:5432/aica_db",
)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
)


async def get_async_db():
    async with AsyncSessionLocal() as session:
        yield session
```

---

# `backend/main.py`

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.leads import router as leads_router

app = FastAPI(
    title="AICA Lead CRM Platform",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(leads_router)


@app.get("/health")
async def health():
    return {"ok": True, "service": "aica-lead-crm"}
```

---

# `backend/requirements.txt`

```txt
fastapi==0.115.6
uvicorn[standard]==0.34.0
gunicorn==23.0.0
sqlalchemy==2.0.36
asyncpg==0.30.0
pydantic==2.10.4
celery==5.4.0
redis==5.2.1
requests==2.32.3
```

---

# `backend/Dockerfile`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd -m aica_user
USER aica_user

CMD ["gunicorn", "main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000"]
```

---

# `docker-compose.yml`

```yaml
version: "3.8"

services:
  db:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: ${DB_USER:-aica_user}
      POSTGRES_PASSWORD: ${DB_PASSWORD:-aica_password}
      POSTGRES_DB: ${DB_NAME:-aica_db}
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    restart: always

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    restart: always

  api:
    build:
      context: ./backend
      dockerfile: Dockerfile
    command: gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql+asyncpg://${DB_USER:-aica_user}:${DB_PASSWORD:-aica_password}@db:5432/${DB_NAME:-aica_db}
      REDIS_URL: redis://redis:6379/0
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:-}
      TELEGRAM_CHAT_ID: ${TELEGRAM_CHAT_ID:-}
    depends_on:
      - db
      - redis
    restart: always

  worker:
    build:
      context: ./backend
      dockerfile: Dockerfile
    command: celery -A app.worker.tasks worker --concurrency=4 -Q high_priority,normal --loglevel=info
    environment:
      DATABASE_URL: postgresql+asyncpg://${DB_USER:-aica_user}:${DB_PASSWORD:-aica_password}@db:5432/${DB_NAME:-aica_db}
      REDIS_URL: redis://redis:6379/0
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:-}
      TELEGRAM_CHAT_ID: ${TELEGRAM_CHAT_ID:-}
    depends_on:
      - redis
      - db
    restart: always

volumes:
  pgdata:
```

