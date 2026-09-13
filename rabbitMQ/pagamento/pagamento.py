import pika
import random
import json
import base64

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

EXCHANGE = 'ecommerce'
QUEUE = 'fila.pagamento'
KEY_NAME = 'pagamento'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

channel.queue_declare(queue=QUEUE, exclusive=True)

channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key='pedido.estoque_ok')

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
        pkcs1_15.new(pub_key).verify(event_hash, body['signature'])
    except ValueError:
        print("ASSINATURA INVÁLIDA! Evento adulterado ou de fonte desconhecida. Descartando...")
        return False
    return True

def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    json_body = json.loads(bytes.decode(body))

    if not validate_signature(json_body, 'estoque'):
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return

    if not (method.routing_key == 'pedido.estoque_ok'):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    str_body = sign_content(json_body)
    if random.choice([True, False]):
        ch.basic_publish(exchange=EXCHANGE, routing_key='pagamento.aprovado', body=str_body)
    else:
        ch.basic_publish(exchange=EXCHANGE, routing_key='pagamento.reprovado', body=str_body)
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
