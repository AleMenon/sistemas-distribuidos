import pika
import threading
import json
import random
import base64

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

EXCHANGE = 'ecommerce'
QUEUE = 'fila.principal'
KEY_NAME = 'main'

PRODUCTS = [
    {
        'name': 'produto_a',
        'id': 1
    },
    {
        'name': 'produto_b',
        'id': 2
    },
    {
        'name': 'produto_c',
        'id': 3
    },
]

ORDERS = []

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

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

def set_status(order_id, status):
    order_id = int(order_id)
    target = next(
        order for order in ORDERS
        if order['id'] == order_id
    )
    target['status'] = status

def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    json_body = json.loads(bytes.decode(body))
    if method.routing_key == 'pedido.estoque_ok':
        if not validate_signature(json_body, 'estoque'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PEDIDO_NO_CARRINHO')

    elif method.routing_key == 'estoque.indisponivel':
        if not validate_signature(json_body, 'estoque'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PEDIDO_EXCLUIDO')
        str_body = sign_content(json_body)
        ch.basic_publish(exchange=EXCHANGE, routing_key='pedido.excluido', body=str_body)

    elif method.routing_key == 'pagamento.aprovado':
        if not validate_signature(json_body, 'pagamento'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PAGAMENTO_APROVADO')

    elif method.routing_key == 'pagamento.reprovado':
        if not validate_signature(json_body, 'pagamento'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PEDIDO_EXCLUIDO')
        str_body = sign_content(json_body)
        ch.basic_publish(exchange=EXCHANGE, routing_key='pedido.excluido', body=str_body)

    elif method.routing_key == 'pedido.enviado':
        if not validate_signature(json_body, 'entrega'):
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        set_status(json_body['content']['id'], 'PEDIDO_ENVIADO')

    ch.basic_ack(delivery_tag=method.delivery_tag)

def listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

    channel.queue_declare(queue=QUEUE, exclusive=True)

    routing_keys = [
        'pedido.estoque_ok',
        'estoque.indisponivel',
        'pagamento.aprovado',
        'pagamento.reprovado',
        'pedido.enviado'
    ]
    for v in routing_keys:
        channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=v)

    channel.basic_consume(queue=QUEUE, on_message_callback=callback)
    try:
        channel.start_consuming()
    finally:
        if not channel.is_closed:
            channel.close()

        if not connection.is_closed:
            connection.close()

def list_products():
    print('===== Produtos =====')
    for v in PRODUCTS:
        print(f'{v["id"]} - {v["name"]}')

def create_orders():
    list_products()
    products = []
    choice = 1
    while choice == 1:
        id = int(input('Produto: '))
        if not any(product for product in PRODUCTS if product['id'] == id):
            print('Produto inválido')
            continue
        quantity = int(input('Quantidade: '))
        products.append({'id': id, 'quantity': quantity})
        choice = int(input('Deseja escolher mais produtos (1 - sim, 2 - nao)?'))

    order = {
        'id': random.randint(1, 1000),
        'status': 'PEDIDO_CRIADO',
        'items': products
    }
    ORDERS.append(order)

    str_body = sign_content({'content': order})

    channel.basic_publish(exchange=EXCHANGE, routing_key='pedido.criado', body=str_body)

def orders_status():
    print('===== Pedidos =====')
    for order in ORDERS:
        print(f'{order["id"]}: {order["status"]}')
        for item in order["items"]:
            print(f'Produto {item["id"]} - {item["quantity"]} unidades')

def delete_orders():
    orders_status()
    orders = []
    choice = 1
    while choice == 1:
        id = int(input('Pedido: '))

        target = next((order for order in ORDERS if order['id'] == id), None)
        if not target or target['status'] == 'PEDIDO_EXCLUIDO':
            print('Pedido inválido')
            continue

        target['status'] = 'PEDIDO_EXCLUIDO'
        orders.append(target)
        choice = int(input('Deseja escolher outros pedidos (1 - sim, 2 - nao)?'))

    for order in orders:
        str_body = sign_content({'content': order})
        channel.basic_publish(exchange=EXCHANGE, routing_key='pedido.excluido', body=str_body)

def main():
    thread_listener = threading.Thread(target=listener, daemon=True)
    thread_listener.start()

    while True:
        print('===== MENU ECOMMERCE =====')
        print('1. Listar produtos')
        print('2. Realizar pedidos')
        print('3. Excluir pedidos')
        print('4. Consultar pedidos')
        print('5. Sair')
        opt = int(input('Escolha uma opção: '))

        match opt:
            case 1:
                list_products()
            case 2:
                create_orders()
            case 3:
                delete_orders()
            case 4:
                orders_status()
            case 5:
                break
            case _:
                print('Opção inválida')


if __name__ == '__main__':
    print('Iniciando...')
    try:
        main()
    except KeyboardInterrupt:
        ...
    finally:
        print('Encerrando...')
        channel.close()
