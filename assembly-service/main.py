import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, UTC
from enum import StrEnum
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://app:app@postgres:5432/ecommerce")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

pool: AsyncConnectionPool
producer: AIOKafkaProducer | None = None
tasks: list[asyncio.Task] = []


class OrderStatus(StrEnum):
    ASSEMBLING = "ASSEMBLING"
    ASSEMBLED = "ASSEMBLED"
    READY_FOR_PICKUP = "READY_FOR_PICKUP"
    COMPLETED = "COMPLETED"


class Topic(StrEnum):
    ASSEMBLY_TASK = "assembly.task"
    ASSEMBLY_STATUS = "assembly.status"


async def retry(func, attempts: int = 40, delay: float = 2.0):
    for attempt in range(1, attempts + 1):
        try:
            return await func()
        except Exception:
            if attempt == attempts:
                raise
            await asyncio.sleep(delay)


async def init_postgres() -> None:
    async with pool.connection() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assembly_tasks (
                order_id UUID PRIMARY KEY,
                user_id UUID,
                status TEXT NOT NULL,
                items JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute("ALTER TABLE assembly_tasks ADD COLUMN IF NOT EXISTS user_id UUID")


async def handle_task(message: dict[str, Any]) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO assembly_tasks (order_id, user_id, status, items)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE
            SET user_id = EXCLUDED.user_id,
                status = EXCLUDED.status,
                items = EXCLUDED.items,
                updated_at = now()
            """,
            (message["order_id"], message.get("user_id"), OrderStatus.ASSEMBLING.value, json.dumps(message["items"])),
        )


async def consume_tasks() -> None:
    consumer = AIOKafkaConsumer(
        Topic.ASSEMBLY_TASK,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="assembly-service-tasks",
        value_deserializer=lambda raw: json.loads(raw.decode()),
        auto_offset_reset="earliest",
    )
    await retry(consumer.start)
    try:
        async for record in consumer:
            await handle_task(record.value)
    finally:
        await consumer.stop()


async def set_status(order_id: str, next_status: OrderStatus, allowed_current: set[OrderStatus]) -> dict[str, Any]:
    async with pool.connection() as conn:
        async with conn.transaction():
            result = await conn.execute("SELECT * FROM assembly_tasks WHERE order_id = %s FOR UPDATE", (order_id,))
            row = await result.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="assembly task not found")
            current = OrderStatus(row["status"])
            if current not in allowed_current:
                raise HTTPException(status_code=409, detail=f"cannot move from {current} to {next_status}")
            await conn.execute(
                "UPDATE assembly_tasks SET status = %s, updated_at = now() WHERE order_id = %s",
                (next_status.value, order_id),
            )
            row["status"] = next_status.value
            row["updated_at"] = datetime.now(UTC)
    message = {
        "order_id": order_id,
        "user_id": str(row["user_id"]) if row.get("user_id") else None,
        "status": next_status.value,
        "items": row["items"],
        "timestamp": datetime.now(UTC).isoformat(),
    }
    await producer.send_and_wait(Topic.ASSEMBLY_STATUS, message)
    return {"order_id": order_id, "user_id": row.get("user_id"), "status": next_status.value}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool, producer, tasks
    pool = AsyncConnectionPool(POSTGRES_DSN, open=False, kwargs={"row_factory": dict_row})
    await retry(pool.open)
    await init_postgres()
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode())
    await retry(producer.start)
    tasks = [asyncio.create_task(consume_tasks())]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if producer:
        await producer.stop()
    await pool.close()


app = FastAPI(title="Assembly Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/assembly/orders")
async def list_tasks():
    async with pool.connection() as conn:
        result = await conn.execute("SELECT * FROM assembly_tasks ORDER BY updated_at DESC")
        return await result.fetchall()


@app.get("/assembly/orders/{order_id}")
async def get_task(order_id: str):
    async with pool.connection() as conn:
        result = await conn.execute("SELECT * FROM assembly_tasks WHERE order_id = %s", (order_id,))
        row = await result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="assembly task not found")
    return row


@app.post("/assembly/orders/{order_id}/assembled")
async def mark_assembled(order_id: str):
    return await set_status(order_id, OrderStatus.ASSEMBLED, {OrderStatus.ASSEMBLING})


@app.post("/assembly/orders/{order_id}/ready")
async def mark_ready(order_id: str):
    return await set_status(order_id, OrderStatus.READY_FOR_PICKUP, {OrderStatus.ASSEMBLED})


@app.post("/assembly/orders/{order_id}/complete")
async def mark_complete(order_id: str):
    return await set_status(order_id, OrderStatus.COMPLETED, {OrderStatus.READY_FOR_PICKUP})
