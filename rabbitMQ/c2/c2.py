import pika
import json

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

EXCHANGE = 'promocoes'
QUEUE = 'fila.C2'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

channel.queue_declare(queue=QUEUE, exclusive=True)

channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key='#')

def validate_signature(body, src):
    event_str = json.dumps(body['content'], sort_keys=True)
    event_hash = SHA256.new(event_str.encode('utf-8'))

    try:
        with open(f'{src}_public.pem', 'r') as f:
            pub_key = RSA.import_key(f.read())
        pkcs1_15.new(pub_key).verify(event_hash, body['signature'])
    except ValueError:
        print("ASSINATURA INVÁLIDA! Evento adulterado ou de fonte desconhecida. Descartando...")
        return False
    return True


def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    json_body = json.loads(bytes.decode(body))

    if not validate_signature(json_body, 'promocoes'):
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return

    print(f'Promoção recebida: {json_body["content"]}')
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
