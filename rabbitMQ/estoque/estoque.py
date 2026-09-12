import pika
import random
import json
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

EXCHANGE = 'ecommerce'
QUEUE = 'fila.estoque'

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

def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    if method.routing_key == 'pedido.criado':
        order = json.loads(bytes.decode(body))

        for item in order['items']:
            target = next(
                product for product in PRODUCTS
                if product['id'] == item['id']
            )

            if item['quantity'] > target['stock']:
                response = {
                    'order_id': order['id'],
                    'product_id': target['id']
                }
                ch.basic_publish(exchange=EXCHANGE, routing_key='estoque.indisponivel', body=json.dumps(response))
                return

            target['stock'] -= item['quantity']
            ch.basic_publish(exchange=EXCHANGE, routing_key='pedido.estoque_ok', body=str(order['id']))

    elif method.routing_key == 'pedido.excluido':
        order = json.loads(bytes.decode(body))

        for item in order['items']:
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
        channel.close()
