import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

EXCHANGE = 'promocoes'
QUEUE = 'fila.C2'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

channel.queue_declare(queue=QUEUE, exclusive=True)

channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key='#')

def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    print(f"Promoção recebida: {bytes.decode(body)}")
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
