import asyncio


async def main() -> None:
    from init_catalog_elasticsearch import main as init_elasticsearch
    from init_catalog_postgres import main as init_postgres

    await init_postgres()
    await init_elasticsearch()


if __name__ == "__main__":
    asyncio.run(main())
