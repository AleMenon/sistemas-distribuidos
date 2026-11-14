
import httpx

from pathlib import Path
from typing import Literal
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

app = FastAPI()

MOCK_URL = "http://localhost:8004"

# Armazena as cobranças em memória
cobrancas = {}


class CobrancaRequest(BaseModel):
    order_id: int
    webhook_url: str


class ResultadoRequest(BaseModel):
    status: Literal["APROVADO", "RECUSADO"]


# Recebe a solicitação de cobrança do MS Pagamento
@app.post("/v1/cobranca")
def criar_cobranca(data: CobrancaRequest):
    cobrancas[data.order_id] = {
        "webhook_url": data.webhook_url,
        "status": "PENDENTE"
    }

    return {
        "checkout_url": f"{MOCK_URL}/checkout/{data.order_id}"
    }


# Exibe a página HTML de pagamento
@app.get("/checkout/{order_id}")
def abrir_checkout(order_id: int):
    if order_id not in cobrancas:
        raise HTTPException(404, "Cobrança não encontrada")

    return FileResponse(
        Path(__file__).with_name("checkout.html"),
        media_type="text/html"
    )


# Recebe a decisão do usuário e envia o webhook
@app.post("/checkout/{order_id}/resultado")
async def processar_pagamento(order_id: int, data: ResultadoRequest):
    cobranca = cobrancas.get(order_id)

    if not cobranca:
        raise HTTPException(404, "Cobrança não encontrada")

    if cobranca["status"] != "PENDENTE":
        raise HTTPException(409, "Pagamento já processado")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                cobranca["webhook_url"],
                json={
                    "order_id": order_id,
                    "status": data.status
                },
                timeout=10.0
            )

            response.raise_for_status()

    except httpx.HTTPError:
        raise HTTPException(502, "Erro ao enviar webhook")

    cobranca["status"] = data.status

    return {
        "order_id": order_id,
        "status": data.status
    }
