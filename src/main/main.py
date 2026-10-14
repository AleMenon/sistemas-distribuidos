import asyncio
import pika
import threading
import json
import base64
import httpx

from pika.exceptions import StreamLostError
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

from fastapi import FastAPI, Request

EXCHANGE = 'ecommerce'
QUEUE = 'fila.principal'
KEY_NAME = 'main'

ORDERS = []

MAIN_LOOP = None

app = FastAPI()

#NOTE: RestAPI
@app.get("/v1/products")
async def get_products(request: Request):
    async with httpx.AsyncClient() as client:
        response = await client.get("http://127.0.0.1:8001/v1/products")
        return response.json()

@app.post("/v1/orders")
async def post_orders(request: Request):
    body = await request.json()
    publish("pedido.criado", body)
    return {"status": "PROCESSANDO"}

@app.post("/v1/promotion/sign")
async def post_promotion_sign(request: Request):
    body = await request.json()
    publish("interesse.promocao", body)
    return {"status": "APROVADO"}

@app.on_event("startup")
def event_startup():
    MAIN_LOOP = asyncio.get_running_loop()
    threading.Thread(target=listener, daemon=True).start()

#NOTE: RabbitMQ
#TODO: Entender sse e fazer alterações aqui
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

    elif method.routing_key == 'pagamento.aprovado':
        if not validate_signature(json_body, 'pagamento'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PAGAMENTO_APROVADO')

    elif method.routing_key == 'pagamento.reprovado':
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

    ch.basic_ack(delivery_tag=method.delivery_tag)

def listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

    channel.queue_declare(queue=QUEUE, exclusive=True)

    routing_keys = [
        'pedido.estoque_ok',
        'estoque.indisponivel',
        'pagamento.aprovado',
        'pagamento.reprovado',
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
    order_id = int(order_id)
    target = next(
        order for order in ORDERS
        if order['id'] == order_id
    )
    target['status'] = status
