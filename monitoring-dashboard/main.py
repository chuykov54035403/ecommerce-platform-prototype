import os

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates


ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:8000")
ASSEMBLY_SERVICE_URL = os.getenv("ASSEMBLY_SERVICE_URL", "http://assembly-service:8000")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL", "http://inventory-service:8000")
CATALOG_SERVICE_URL = os.getenv("CATALOG_SERVICE_URL", "http://catalog-service:8000")

app = FastAPI(title="Monitoring Dashboard")
templates = Jinja2Templates(directory="service/templates")


async def maybe_get(client: httpx.AsyncClient, url: str):
    try:
        response = await client.get(url, timeout=5)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, order_id: str | None = None):
    data = {"order": None, "assembly": None, "inventory": [], "search": None}
    if order_id:
        async with httpx.AsyncClient() as client:
            order = await maybe_get(client, f"{ORDER_SERVICE_URL}/orders/{order_id}")
            data["order"] = order
            data["assembly"] = await maybe_get(client, f"{ASSEMBLY_SERVICE_URL}/assembly/orders/{order_id}")
            data["search"] = await maybe_get(client, f"{CATALOG_SERVICE_URL}/search/status")
            if order and not order.get("error") and order.get("items"):
                for item in order["items"]:
                    inventory = await maybe_get(client, f"{INVENTORY_SERVICE_URL}/inventory/{item['product_id']}")
                    data["inventory"].append({"order_item": item, "inventory": inventory})
    return templates.TemplateResponse("index.html", {"request": request, "order_id": order_id or "", **data})
