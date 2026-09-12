import pika
import random
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

EXCHANGE = 'ecommerce'
QUEUE = 'fila.pagamento'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

channel.queue_declare(queue=QUEUE, exclusive=True)

channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key='pedido.estoque_ok')

def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    if not (method.routing_key == 'pedido.estoque_ok'):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    if random.choice([True, False]):
        ch.basic_publish(exchange=EXCHANGE, routing_key='pagamento.aprovado', body=body)
    else:
        ch.basic_publish(exchange=EXCHANGE, routing_key='pagamento.reprovado', body=body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

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
