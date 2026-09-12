import pika
import sys

EXCHANGE = 'promocoes'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

def main():
    promocoes = sys.argv[1:]

    for promocao in promocoes:
        channel.basic_publish(exchange=EXCHANGE, routing_key=f'promocao.categoria.{promocao}', body=f'PROMOÇÃO {promocao}!!!')

if __name__ == '__main__':
    try:
        main()
    finally:
        print('Encerrando...')
        channel.close()
