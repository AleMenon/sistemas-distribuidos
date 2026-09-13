import pika
import sys
import json
import base64

from pika.exceptions import StreamLostError
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

EXCHANGE = 'promocoes'
KEY_NAME = 'promocoes'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

def sign_content(body):
    event_str = json.dumps(body['content'], sort_keys=True)
    event_hash = SHA256.new(event_str.encode('utf-8'))

    with open(f'{KEY_NAME}_private.pem', 'r') as f:
        priv_key = RSA.import_key(f.read())

    signature = pkcs1_15.new(priv_key).sign(event_hash)
    body['signature'] = base64.b64encode(signature).decode('utf-8')

    return json.dumps(body)

def main():
    promocoes = sys.argv[1:]

    for promocao in promocoes:
        str_body = sign_content({'content': f'PROMOÇÃO {promocao}!!!'})
        channel.basic_publish(exchange=EXCHANGE, routing_key=f'promocao.categoria.{promocao}', body=str_body)

if __name__ == '__main__':
    try:
        main()
    finally:
        print('Encerrando...')
        try:
            if channel.is_open:
                channel.close()
            if connection.is_open:
                connection.close()
        except StreamLostError:
            pass
