import pika
import json
import base64
import httpx
import threading

from pika.exceptions import StreamLostError
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

from fastapi import FastAPI, Request

EXCHANGE = 'ecommerce'
QUEUE = 'fila.pagamento'
KEY_NAME = 'pagamento'

WEBHOOK_URL = "http://localhost:8002/v1/webhook"
MOCK_URL = "http://localhost:8004/v1/cobranca"

app = FastAPI()

#NOTE: RestAPI
@app.post("/v1/webhook")
async def webhook(request: Request):
    body = await request.json()
    order_id = body.get("order_id")
    status = body.get("status")

    event_body = {"id": order_id}

    publish(f"pagamento.{status.lower()}", event_body)
    print(f"Pagamento {status.lower()} para o pedido {order_id}")

#NOTE: RabbitMQ
def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    json_body = json.loads(bytes.decode(body))

    if method.routing_key == 'pedido.estoque_ok':
        if not validate_signature(json_body, 'estoque'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return

        order = json_body['content']

        try:
            with httpx.Client() as client:
                payload = {
                    "order_id": order['id'],
                    "webhook_url": WEBHOOK_URL
                }
                response = client.post(MOCK_URL, json=payload)
                mock_data = response.json()

                checkout_url = mock_data.get('checkout_url')

                if not checkout_url:
                    raise ValueError("Mock não retornou a URL do checkout")
                
                publish("pagamento.checkout_criado", {
                    "id": order["id"],
                    "payment_url": checkout_url
                })

                print(f"Cobrança criada. Aguardando pagamento do pedido {order['id']}")
        except Exception as e:
            print(f"Erro ao contatar o Mock: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

def listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

    channel.queue_declare(queue=QUEUE, exclusive=True)

    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key='pedido.estoque_ok')

    channel.basic_consume(queue=QUEUE, on_message_callback=callback)
    channel.start_consuming()

@app.on_event("startup")
def startup_event():
    threading.Thread(target=listener, daemon=True).start()

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

def publish(routing_key, body):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    str_body = sign_content({'content': body})
    channel.basic_publish(exchange=EXCHANGE, routing_key=routing_key, body=str_body)
    connection.close()
