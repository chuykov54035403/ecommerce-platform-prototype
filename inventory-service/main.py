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
    COMPLETED = "COMPLETED"


class Topic(StrEnum):
    INVENTORY_REQUEST = "inventory.request"
    INVENTORY_RESPONSE = "inventory.response"
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
            CREATE TABLE IF NOT EXISTS inventory (
                product_id UUID PRIMARY KEY,
                quantity_available INTEGER NOT NULL CHECK (quantity_available >= 0),
                quantity_reserved INTEGER NOT NULL DEFAULT 0 CHECK (quantity_reserved >= 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        seeds = [
            ("11111111-1111-1111-1111-111111111111", 8),
            ("11111111-1111-1111-1111-111111111112", 3),
            ("22222222-2222-2222-2222-222222222221", 10),
            ("22222222-2222-2222-2222-222222222222", 6),
            ("33333333-3333-3333-3333-333333333331", 12),
            ("33333333-3333-3333-3333-333333333332", 1),
        ]
        for product_id, quantity in seeds:
            await conn.execute(
                """
                INSERT INTO inventory (product_id, quantity_available, quantity_reserved)
                VALUES (%s, %s, 0)
                ON CONFLICT (product_id) DO NOTHING
                """,
                (product_id, quantity),
            )


async def reserve_order(message: dict[str, Any]) -> dict[str, Any]:
    order_id = message["order_id"]
    user_id = message.get("user_id")
    items = message["items"]
    async with pool.connection() as conn:
        async with conn.transaction():
            product_ids = [item["product_id"] for item in items]
            rows = await conn.execute(
                "SELECT * FROM inventory WHERE product_id = ANY(%s::uuid[]) FOR UPDATE",
                (product_ids,),
            )
            inventory = {str(row["product_id"]): row for row in await rows.fetchall()}
            missing = [pid for pid in product_ids if pid not in inventory]
            insufficient = [
                {
                    "product_id": item["product_id"],
                    "requested": item["quantity"],
                    "available": inventory[item["product_id"]]["quantity_available"] - inventory[item["product_id"]]["quantity_reserved"],
                }
                for item in items
                if item["product_id"] in inventory
                and inventory[item["product_id"]]["quantity_available"] - inventory[item["product_id"]]["quantity_reserved"] < item["quantity"]
            ]
            if missing or insufficient:
                return {
                    "order_id": order_id,
                    "user_id": user_id,
                    "success": False,
                    "reason": "INSUFFICIENT_STOCK",
                    "missing_product_ids": missing,
                    "insufficient": insufficient,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            for item in items:
                await conn.execute(
                    """
                    UPDATE inventory
                    SET quantity_reserved = quantity_reserved + %s, updated_at = now()
                    WHERE product_id = %s
                    """,
                    (item["quantity"], item["product_id"]),
                )
    return {"order_id": order_id, "user_id": user_id, "success": True, "timestamp": datetime.now(UTC).isoformat()}


async def finalize_order(message: dict[str, Any]) -> None:
    if message.get("status") != OrderStatus.COMPLETED:
        return
    items = message.get("items", [])
    async with pool.connection() as conn:
        async with conn.transaction():
            for item in items:
                await conn.execute(
                    """
                    UPDATE inventory
                    SET quantity_available = quantity_available - %s,
                        quantity_reserved = quantity_reserved - %s,
                        updated_at = now()
                    WHERE product_id = %s AND quantity_reserved >= %s
                    """,
                    (item["quantity"], item["quantity"], item["product_id"], item["quantity"]),
                )


async def consume_inventory_requests() -> None:
    consumer = AIOKafkaConsumer(
        Topic.INVENTORY_REQUEST,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="inventory-service-reservations",
        value_deserializer=lambda raw: json.loads(raw.decode()),
        auto_offset_reset="earliest",
    )
    await retry(consumer.start)
    try:
        async for record in consumer:
            response = await reserve_order(record.value)
            await producer.send_and_wait(Topic.INVENTORY_RESPONSE, response)
    finally:
        await consumer.stop()


async def consume_completed_orders() -> None:
    consumer = AIOKafkaConsumer(
        Topic.ASSEMBLY_STATUS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="inventory-service-completions",
        value_deserializer=lambda raw: json.loads(raw.decode()),
        auto_offset_reset="earliest",
    )
    await retry(consumer.start)
    try:
        async for record in consumer:
            await finalize_order(record.value)
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool, producer, tasks
    pool = AsyncConnectionPool(POSTGRES_DSN, open=False, kwargs={"row_factory": dict_row})
    await retry(pool.open)
    await init_postgres()
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode())
    await retry(producer.start)
    tasks = [asyncio.create_task(consume_inventory_requests()), asyncio.create_task(consume_completed_orders())]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if producer:
        await producer.stop()
    await pool.close()


app = FastAPI(title="Inventory Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/inventory")
async def list_inventory():
    async with pool.connection() as conn:
        result = await conn.execute("SELECT * FROM inventory ORDER BY product_id")
        return await result.fetchall()


@app.get("/inventory/{product_id}")
async def get_inventory(product_id: str):
    async with pool.connection() as conn:
        result = await conn.execute("SELECT * FROM inventory WHERE product_id = %s", (product_id,))
        row = await result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="inventory row not found")
    row["quantity_free"] = row["quantity_available"] - row["quantity_reserved"]
    return row
