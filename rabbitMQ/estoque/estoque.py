import pika
import random
import json
import base64

from pika.exceptions import StreamLostError
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from pika.spec import Basic, BasicProperties
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

EXCHANGE = 'ecommerce'
QUEUE = 'fila.estoque'
KEY_NAME = 'estoque'

PRODUCTS = [
    {
        'name': 'produto_a',
        'stock': 5,
        'id': 1
    },
    {
        'name': 'produto_b',
        'stock': 5,
        'id': 2
    },
    {
        'name': 'produto_c',
        'stock': 5,
        'id': 3
    },
]

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

channel.queue_declare(queue=QUEUE, exclusive=True)

for v in ['criado', 'excluido']:
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=f'pedido.{v}')

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

def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    json_body = json.loads(bytes.decode(body))

    if not validate_signature(json_body, 'main'):
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return

    if method.routing_key == 'pedido.criado':

        for item in json_body['content']['items']:
            target = next(
                product for product in PRODUCTS
                if product['id'] == item['id']
            )

            if item['quantity'] > target['stock']:
                str_body = sign_content(json_body)
                ch.basic_publish(exchange=EXCHANGE, routing_key='estoque.indisponivel', body=str_body)
                return

            target['stock'] -= item['quantity']
            str_body = sign_content(json_body)
            ch.basic_publish(exchange=EXCHANGE, routing_key='pedido.estoque_ok', body=str_body)

    elif method.routing_key == 'pedido.excluido':
        for item in json_body['content']['items']:
            target = next(
                product for product in PRODUCTS
                if product['id'] == item['id']
            )
            target['stock'] += item['quantity']

    ch.basic_ack(method.delivery_tag)

def main():
    channel.basic_consume(queue=QUEUE, on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        print('Iniciando...')
        main()
    except KeyboardInterrupt:
        print('Encerrando...')
        try:
            if channel.is_open:
                channel.close()
            if connection.is_open:
                connection.close()
        except StreamLostError:
            pass
