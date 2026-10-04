import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, UTC
from decimal import Decimal
from enum import StrEnum
from typing import Any

from aiokafka import AIOKafkaProducer
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field


POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://app:app@postgres:5432/ecommerce")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_ENABLED = os.getenv("KAFKA_ENABLED", "true").lower() == "true"

pool: AsyncConnectionPool
es: AsyncElasticsearch
producer: AIOKafkaProducer | None = None


class Topic(StrEnum):
    CATALOG_PRODUCT_CHANGED = "catalog.product.changed"


class ProductIn(BaseModel):
    name: str
    category: str
    brand: str
    price: Decimal = Field(gt=0)
    description: str = ""
    characteristics: dict[str, Any] = Field(default_factory=dict)


class Product(ProductIn):
    id: uuid.UUID
    created_at: datetime
    updated_at: datetime


async def retry(label: str, func, attempts: int = 40, delay: float = 2.0):
    for attempt in range(1, attempts + 1):
        try:
            return await func()
        except Exception:
            if attempt == attempts:
                raise
            await asyncio.sleep(delay)


def product_doc(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "product_id": str(row["id"]),
        "name": row["name"],
        "category": row["category"],
        "brand": row["brand"],
        "price": float(row["price"]),
        "description": row["description"],
        "characteristics": row["characteristics"],
        "updated_at": row["updated_at"].isoformat(),
    }


def parse_filter_value(value: str) -> str | int | float | bool:
    normalized = value.strip()
    if normalized.lower() in {"true", "false"}:
        return normalized.lower() == "true"
    try:
        return int(normalized)
    except ValueError:
        try:
            return float(normalized)
        except ValueError:
            return normalized


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool, es, producer
    pool = AsyncConnectionPool(POSTGRES_DSN, open=False, kwargs={"row_factory": dict_row})
    await retry("postgres", pool.open)
    es = AsyncElasticsearch(ELASTICSEARCH_URL)
    await retry("elasticsearch", es.info)
    if KAFKA_ENABLED:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode())
        await retry("kafka", producer.start)
    yield
    if producer:
        await producer.stop()
    await es.close()
    await pool.close()


app = FastAPI(title="Catalog Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/products", response_model=Product)
async def create_product(payload: ProductIn):
    product_id = uuid.uuid4()
    async with pool.connection() as conn:
        result = await conn.execute(
            """
            INSERT INTO catalog_products
                (id, name, category, brand, price, description, characteristics)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                product_id,
                payload.name,
                payload.category.upper(),
                payload.brand,
                payload.price,
                payload.description,
                json.dumps(payload.characteristics),
            ),
        )
        row = await result.fetchone()
    await es.index(index="products", id=str(product_id), document=product_doc(row), refresh=True)
    if producer:
        await producer.send_and_wait(
            Topic.CATALOG_PRODUCT_CHANGED,
            {"product_id": str(product_id), "operation": "upsert", "timestamp": datetime.now(UTC).isoformat()},
        )
    return row


@app.get("/products", response_model=list[Product])
async def list_products(category: str | None = None):
    async with pool.connection() as conn:
        if category:
            result = await conn.execute("SELECT * FROM catalog_products WHERE category = %s ORDER BY name", (category.upper(),))
        else:
            result = await conn.execute("SELECT * FROM catalog_products ORDER BY category, name")
        return await result.fetchall()


@app.get("/products/search")
async def search_products(
    q: str | None = None,
    category: str | None = None,
    brand: str | None = None,
    characteristic: list[str] = Query(default=[]),
):
    filters: list[dict[str, Any]] = []
    if category:
        filters.append({"term": {"category": category.upper()}})
    if brand:
        filters.append({"term": {"brand": brand}})
    for item in characteristic:
        if "=" in item:
            key, value = item.split("=", 1)
            parsed_value = parse_filter_value(value)
            field = f"characteristics.{key.strip()}"
            if isinstance(parsed_value, str):
                field = f"{field}.keyword"
            filters.append({"term": {field: parsed_value}})

    query: dict[str, Any] = {"bool": {"filter": filters}}
    if q:
        query["bool"]["must"] = [{"multi_match": {"query": q, "fields": ["name^3", "brand", "description"]}}]
    else:
        query["bool"]["must"] = [{"match_all": {}}]

    response = await es.search(index="products", query=query, size=50)
    return [{"score": hit["_score"], **hit["_source"]} for hit in response["hits"]["hits"]]


@app.get("/search/status")
async def search_status():
    count = await es.count(index="products")
    return {"index": "products", "reachable": True, "document_count": count["count"]}


@app.get("/products/{product_id}", response_model=Product)
async def get_product(product_id: uuid.UUID):
    async with pool.connection() as conn:
        result = await conn.execute("SELECT * FROM catalog_products WHERE id = %s", (product_id,))
        row = await result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="product not found")
    return row
