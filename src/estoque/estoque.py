import pika
import random
import json
import base64
import sqlite3
import threading

from pika.exceptions import StreamLostError
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

from fastapi import FastAPI, Request

EXCHANGE = 'ecommerce'
QUEUE = 'fila.estoque'
KEY_NAME = 'estoque'

app = FastAPI()

#NOTE: FastAPI
@app.get("/v1/products")
def get_products(request: Request):
    connection = sqlite3.connect('estoque.db')
    cursor = connection.cursor()
    cursor.execute('SELECT id, name, quantity FROM products')
    response = [{"id": line[0], "name": line[1], "stock": line[2]} for line in cursor.fetchall()]
    connection.close()
    return response

#NOTE: RabbitMQ
def listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

    channel.queue_declare(queue=QUEUE, exclusive=True)

    for v in ['criado', 'excluido']:
        channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=f'pedido.{v}')

    channel.basic_consume(queue=QUEUE, on_message_callback=callback)
    channel.start_consuming()

def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    json_body = json.loads(bytes.decode(body))

    if not validate_signature(json_body, 'main'):
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return

    connection = sqlite3.connect("estoque.db")
    cursor = connection.cursor()

    if method.routing_key == 'pedido.criado':
        valid_stock = True
        for item in json_body['content']['items']:
            cursor.execute('SELECT quantity FROM products WHERE id = ?', (item['id'],))
            result = cursor.fetchone()


            if not result or item['quantity'] > result[0]:
                valid_stock = False
                break

        if not valid_stock:
            str_body = sign_content(json_body)
            ch.basic_publish(exchange=EXCHANGE, routing_key='estoque.indisponivel', body=str_body)
        else:
            for item in json_body['content']['items']:
                cursor.execute(
                    'UPDATE products SET quantity = quantity - ? WHERE id = ?',
                    (item['quantity'], item['id'])
                )
            connection.commit()
            str_body = sign_content(json_body)
            ch.basic_publish(exchange=EXCHANGE, routing_key='pedido.estoque_ok', body=str_body)

    elif method.routing_key == 'pedido.excluido':
        for item in json_body['content']['items']:
            cursor.execute(
                'UPDATE products SET quantity = quantity + ? WHERE id = ?',
               (item['quantity'], item['id'])
            )
        connection.commit()

    connection.close()
    ch.basic_ack(method.delivery_tag)

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
