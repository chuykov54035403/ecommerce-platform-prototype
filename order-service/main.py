import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, UTC
from enum import StrEnum
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra").split(",")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

cluster: Cluster
session: Any
producer: AIOKafkaProducer | None = None
tasks: list[asyncio.Task] = []


class OrderStatus(StrEnum):
    CREATED = "CREATED"
    ASSEMBLING = "ASSEMBLING"
    ASSEMBLED = "ASSEMBLED"
    READY_FOR_PICKUP = "READY_FOR_PICKUP"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"


class Topic(StrEnum):
    INVENTORY_REQUEST = "inventory.request"
    INVENTORY_RESPONSE = "inventory.response"
    ASSEMBLY_TASK = "assembly.task"
    ASSEMBLY_STATUS = "assembly.status"


class OrderItem(BaseModel):
    product_id: uuid.UUID
    quantity: int = Field(gt=0)


class CreateOrder(BaseModel):
    user_id: uuid.UUID
    items: list[OrderItem] = Field(min_length=1)


def now() -> datetime:
    return datetime.now(UTC)


async def retry(func, attempts: int = 60, delay: float = 2.0):
    for attempt in range(1, attempts + 1):
        try:
            return await asyncio.to_thread(func)
        except Exception:
            if attempt == attempts:
                raise
            await asyncio.sleep(delay)


def init_cassandra() -> None:
    global cluster, session
    cluster = Cluster(CASSANDRA_HOSTS)
    root = cluster.connect()
    root.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS ecommerce
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
    )
    session = cluster.connect("ecommerce")
    session.row_factory = dict_factory
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id uuid PRIMARY KEY,
            user_id uuid,
            status text,
            items text,
            created_at timestamp,
            updated_at timestamp
        )
        """
    )
    columns = session.execute(
        """
        SELECT column_name
        FROM system_schema.columns
        WHERE keyspace_name = 'ecommerce' AND table_name = 'orders'
        """
    )
    if "user_id" not in {row["column_name"] for row in columns}:
        session.execute("ALTER TABLE orders ADD user_id uuid")
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS order_status_history (
            order_id uuid,
            changed_at timestamp,
            status text,
            note text,
            PRIMARY KEY (order_id, changed_at)
        ) WITH CLUSTERING ORDER BY (changed_at ASC)
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS order_kafka_messages (
            order_id uuid,
            created_at timestamp,
            topic text,
            direction text,
            payload text,
            PRIMARY KEY (order_id, created_at, topic)
        ) WITH CLUSTERING ORDER BY (created_at ASC)
        """
    )


def insert_history(order_id: uuid.UUID, status: OrderStatus, note: str) -> None:
    session.execute(
        "INSERT INTO order_status_history (order_id, changed_at, status, note) VALUES (%s, %s, %s, %s)",
        (order_id, now(), status.value, note),
    )


def audit_message(order_id: str, topic: str, direction: str, payload: dict[str, Any]) -> None:
    session.execute(
        "INSERT INTO order_kafka_messages (order_id, created_at, topic, direction, payload) VALUES (%s, %s, %s, %s, %s)",
        (uuid.UUID(order_id), now(), str(topic), direction, json.dumps(payload)),
    )


def update_status(order_id: str, status: OrderStatus, note: str) -> dict[str, Any] | None:
    order_uuid = uuid.UUID(order_id)
    row = session.execute("SELECT * FROM orders WHERE order_id = %s", (order_uuid,)).one()
    if not row:
        return None
    session.execute(
        "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
        (status.value, now(), order_uuid),
    )
    insert_history(order_uuid, status, note)
    row["status"] = status.value
    row["updated_at"] = now()
    return row


def get_order_record(order_id: uuid.UUID) -> dict[str, Any] | None:
    row = session.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,)).one()
    if not row:
        return None
    row["items"] = json.loads(row["items"])
    history = session.execute("SELECT changed_at, status, note FROM order_status_history WHERE order_id = %s", (order_id,))
    messages = session.execute("SELECT created_at, topic, direction, payload FROM order_kafka_messages WHERE order_id = %s", (order_id,))
    row["history"] = list(history)
    row["kafka_messages"] = [
        {**message, "payload": json.loads(message["payload"])}
        for message in messages
    ]
    return row


async def handle_inventory_response(message: dict[str, Any]) -> None:
    audit_message(message["order_id"], Topic.INVENTORY_RESPONSE, "consume", message)
    if message.get("success"):
        row = await asyncio.to_thread(update_status, message["order_id"], OrderStatus.ASSEMBLING, "inventory reservation succeeded")
        if row:
            task_message = {
                "order_id": message["order_id"],
                "user_id": message.get("user_id") or (str(row["user_id"]) if row.get("user_id") else None),
                "status": OrderStatus.ASSEMBLING,
                "items": json.loads(row["items"]) if isinstance(row["items"], str) else row["items"],
                "timestamp": now().isoformat(),
            }
            await producer.send_and_wait(Topic.ASSEMBLY_TASK, task_message)
            await asyncio.to_thread(audit_message, message["order_id"], Topic.ASSEMBLY_TASK, "produce", task_message)
    else:
        await asyncio.to_thread(update_status, message["order_id"], OrderStatus.REJECTED, message.get("reason", "inventory reservation failed"))


async def handle_assembly_status(message: dict[str, Any]) -> None:
    status = OrderStatus(message["status"])
    audit_message(message["order_id"], Topic.ASSEMBLY_STATUS, "consume", message)
    await asyncio.to_thread(update_status, message["order_id"], status, "assembly status accepted")


async def consume_inventory_responses() -> None:
    consumer = AIOKafkaConsumer(
        Topic.INVENTORY_RESPONSE,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="order-service-inventory-responses",
        value_deserializer=lambda raw: json.loads(raw.decode()),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for record in consumer:
            await handle_inventory_response(record.value)
    finally:
        await consumer.stop()


async def consume_assembly_statuses() -> None:
    consumer = AIOKafkaConsumer(
        Topic.ASSEMBLY_STATUS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="order-service-assembly-statuses",
        value_deserializer=lambda raw: json.loads(raw.decode()),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for record in consumer:
            await handle_assembly_status(record.value)
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, tasks
    await retry(init_cassandra)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode())
    await producer.start()
    tasks = [asyncio.create_task(consume_inventory_responses()), asyncio.create_task(consume_assembly_statuses())]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if producer:
        await producer.stop()
    cluster.shutdown()


app = FastAPI(title="Order Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/orders")
async def create_order(payload: CreateOrder):
    order_id = uuid.uuid4()
    items = [{"product_id": str(item.product_id), "quantity": item.quantity} for item in payload.items]
    created = now()
    await asyncio.to_thread(
        session.execute,
        "INSERT INTO orders (order_id, user_id, status, items, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)",
        (order_id, payload.user_id, OrderStatus.CREATED.value, json.dumps(items), created, created),
    )
    await asyncio.to_thread(insert_history, order_id, OrderStatus.CREATED, "order created")
    message = {"order_id": str(order_id), "user_id": str(payload.user_id), "items": items, "timestamp": now().isoformat()}
    await producer.send_and_wait(Topic.INVENTORY_REQUEST, message)
    await asyncio.to_thread(audit_message, str(order_id), Topic.INVENTORY_REQUEST, "produce", message)
    return {"order_id": order_id, "user_id": payload.user_id, "status": OrderStatus.CREATED}


@app.get("/orders/{order_id}")
async def get_order(order_id: uuid.UUID):
    row = await asyncio.to_thread(get_order_record, order_id)
    if not row:
        raise HTTPException(status_code=404, detail="order not found")
    return row
