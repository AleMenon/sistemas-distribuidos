import asyncio
import pika
import threading
import json
import base64
import httpx
import sqlite3

import database

from pika.exceptions import StreamLostError
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

DB_NAME = "main"

EXCHANGE = 'ecommerce'
QUEUE = 'fila.principal'
KEY_NAME = 'main'

MAIN_LOOP = None

app = FastAPI()
database.config_database()

sse_clients = {}

#NOTE: RestAPI
@app.get("/v1/products")
async def get_products(request: Request):
    async with httpx.AsyncClient() as client:
        response = await client.get("http://127.0.0.1:8001/v1/products")
        return response.json()

@app.post("/v1/orders")
async def post_orders(request: Request):
    body = await request.json()

    conn = sqlite3.connect(f"{DB_NAME}.db")
    cursor = conn.cursor()

    cursor.execute("INSERT INTO orders (id, status) VALUES (?, ?)", (body['id'], 'PEDIDO_CRIADO'))

    for item in body['items']:
        cursor.execute(
            "INSERT INTO items (order_id, product_id, quantity) VALUES (?, ?, ?)",
            (body['id'], item['id'], item['quantity'])
        )

    conn.commit()
    conn.close()
    publish("pedido.criado", body)
    return {"status": "PROCESSANDO"}

@app.post("/v1/promotion/sign")
async def post_promotion_sign(request: Request):
    body = await request.json()
    publish("interesse.promocao", body)
    return {"status": "APROVADO"}

@app.delete("/v1/promotion/unsign")
async def delete_promotion_sign(request: Request):
    body = await request.json()
    email = body.get("email")

    if not isinstance(email, str) or not email.strip():
        raise HTTPException(400, "E-mail obrigatório")

    publish("interesse.cancelado", {
        "email": email.strip()
    })
    return {"status": "PROCESSANDO"}

@app.on_event("startup")
def event_startup():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    threading.Thread(target=listener, daemon=True).start()

@app.get("/v1/sse/{client_id}")
async def sse_endpoint(request: Request, client_id: str):
    queue = asyncio.Queue()
    sse_clients[client_id] = queue

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break

                try:
                    message = await asyncio.wait_for(
                        queue.get(),
                        timeout=1
                    )
                    yield message

                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"

        except asyncio.CancelledError:
            pass

        finally:
            if sse_clients.get(client_id) is queue:
                del sse_clients[client_id]

    return StreamingResponse(event_generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})

#NOTE: RabbitMQ
def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    json_body = json.loads(bytes.decode(body))
    if method.routing_key == 'pedido.estoque_ok':
        if not validate_signature(json_body, 'estoque'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PEDIDO_NO_CARRINHO')

    elif method.routing_key == 'estoque.indisponivel':
        if not validate_signature(json_body, 'estoque'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PEDIDO_EXCLUIDO')
        str_body = sign_content(json_body)
        ch.basic_publish(exchange=EXCHANGE, routing_key='pedido.excluido', body=str_body)

    elif method.routing_key == 'pagamento.checkout_criado':
        if not validate_signature(json_body, 'pagamento'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return

        set_status(json_body['content']['id'], 'AGUARDANDO_PAGAMENTO')

    elif method.routing_key == 'pagamento.aprovado':
        if not validate_signature(json_body, 'pagamento'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PAGAMENTO_APROVADO')

    elif method.routing_key == 'pagamento.recusado':
        if not validate_signature(json_body, 'pagamento'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PEDIDO_EXCLUIDO')
        str_body = sign_content(json_body)
        ch.basic_publish(exchange=EXCHANGE, routing_key='pedido.excluido', body=str_body)

    elif method.routing_key == 'pedido.enviado':
        if not validate_signature(json_body, 'entrega'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PEDIDO_ENVIADO')

    print(f'Recebido - {method.routing_key}')

    sse_notify(method.routing_key, json_body['content'])

    ch.basic_ack(delivery_tag=method.delivery_tag)

def listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

    channel.queue_declare(queue=QUEUE, exclusive=True)

    routing_keys = [
        'pedido.estoque_ok',
        'estoque.indisponivel',
        'pagamento.checkout_criado',
        'pagamento.aprovado',
        'pagamento.recusado',
        'pedido.enviado'
    ]
    for v in routing_keys:
        channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=v)

    channel.basic_consume(queue=QUEUE, on_message_callback=callback)
    channel.start_consuming()

def publish(routing_key, body):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    str_body = sign_content({'content': body})
    channel.basic_publish(exchange=EXCHANGE, routing_key=routing_key, body=str_body)
    connection.close()

#NOTE: Utils
def sign_content(body):
    event_str = json.dumps(body['content'], sort_keys=True)
    event_hash = SHA256.new(event_str.encode('utf-8'))

    with open(f'{KEY_NAME}_private.pem', 'r') as f:
        priv_key = RSA.import_key(f.read())

    signature = pkcs1_15.new(priv_key).sign(event_hash)
    body['signature'] = base64.b64encode(signature).decode('utf-8')

    return json.dumps(body)

def validate_signature(body, src):
    event_str = json.dumps(body['content'], sort_keys=True)
    event_hash = SHA256.new(event_str.encode('utf-8'))

    try:
        with open(f'{src}_public.pem', 'r') as f:
            pub_key = RSA.import_key(f.read())
        pkcs1_15.new(pub_key).verify(event_hash, base64.b64decode(body['signature']))
    except ValueError:
        print(f"ASSINATURA INVÁLIDA! Evento {src} adulterado ou de fonte desconhecida.")
        return False
    return True

def set_status(order_id, status):
    conn = sqlite3.connect(f"{DB_NAME}.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE orders SET status = ? WHERE id = ?", (status, int(order_id)))
    conn.commit()
    conn.close()

def sse_notify(routing_key, content):
    if MAIN_LOOP is None:
        return

    client_id = str(content["id"])

    message = f"data: {json.dumps({'event': routing_key, 'content': content})}\n\n"

    def notify():
        queue = sse_clients.get(client_id)

        if queue is not None:
            queue.put_nowait(message)

    MAIN_LOOP.call_soon_threadsafe(notify)
