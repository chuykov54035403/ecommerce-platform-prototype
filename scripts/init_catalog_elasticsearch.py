import asyncio
import os
from typing import Any

from elasticsearch import AsyncElasticsearch
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://app:app@localhost:5432/ecommerce")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")


async def retry(label: str, func, attempts: int = 60, delay: float = 2.0):
    for attempt in range(1, attempts + 1):
        try:
            return await func()
        except Exception:
            if attempt == attempts:
                raise
            print(f"{label} is not ready, retry {attempt}/{attempts}")
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


async def main() -> None:
    pool = AsyncConnectionPool(POSTGRES_DSN, open=False, kwargs={"row_factory": dict_row})
    await retry("postgres", pool.open)
    es = AsyncElasticsearch(ELASTICSEARCH_URL)
    await retry("elasticsearch", es.info)
    try:
        await es.indices.create(
            index="products",
            ignore=400,
            mappings={
                "properties": {
                    "product_id": {"type": "keyword"},
                    "name": {"type": "text"},
                    "category": {"type": "keyword"},
                    "brand": {"type": "keyword"},
                    "price": {"type": "double"},
                    "description": {"type": "text"},
                    "characteristics": {"type": "object", "enabled": True},
                }
            },
        )
        async with pool.connection() as conn:
            rows = await conn.execute("SELECT * FROM catalog_products")
            products = await rows.fetchall()
        for row in products:
            await es.index(index="products", id=str(row["id"]), document=product_doc(row), refresh=True)
        print(f"Indexed {len(products)} products into Elasticsearch index products")
    finally:
        await es.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
