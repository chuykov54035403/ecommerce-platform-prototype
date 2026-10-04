import asyncio
import json
import os

from psycopg_pool import AsyncConnectionPool

from catalog_seed import PRODUCTS


POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://app:app@localhost:5432/ecommerce")


async def retry(label: str, func, attempts: int = 60, delay: float = 2.0):
    for attempt in range(1, attempts + 1):
        try:
            return await func()
        except Exception:
            if attempt == attempts:
                raise
            print(f"{label} is not ready, retry {attempt}/{attempts}")
            await asyncio.sleep(delay)


async def main() -> None:
    pool = AsyncConnectionPool(POSTGRES_DSN, open=False)
    await retry("postgres", pool.open)
    try:
        async with pool.connection() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_products (
                    id UUID PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    brand TEXT NOT NULL,
                    price NUMERIC(12,2) NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    characteristics JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            for row in PRODUCTS:
                await conn.execute(
                    """
                    INSERT INTO catalog_products
                        (id, name, category, brand, price, description, characteristics)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET name = EXCLUDED.name,
                        category = EXCLUDED.category,
                        brand = EXCLUDED.brand,
                        price = EXCLUDED.price,
                        description = EXCLUDED.description,
                        characteristics = EXCLUDED.characteristics,
                        updated_at = now()
                    """,
                    (*row[:6], json.dumps(row[6])),
                )
        print(f"Initialized PostgreSQL catalog table with {len(PRODUCTS)} products")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
