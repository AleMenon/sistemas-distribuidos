import pika
import random
import json
import base64
import sqlite3
import threading

import database

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
database.config_database()

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

def callback(ch: BlockingChannel, method: Basic.Deliver,
             properties: BasicProperties, body: bytes):

    try:
        json_body = json.loads(body.decode("utf-8"))

        if not validate_signature(json_body, "main"):
            ch.basic_reject(method.delivery_tag, requeue=False)
            return

        content = json_body["content"]
        order_id = content["id"]

        if type(order_id) is not int or order_id <= 0:
            raise ValueError("ID do pedido inválido")

        connection = sqlite3.connect("estoque.db")

        try:
            cursor = connection.cursor()

            # Bloqueia concorrência entre leitura e atualização do estoque
            cursor.execute("BEGIN IMMEDIATE")

            if method.routing_key == "pedido.criado":

                # 1. Validar e agrupar quantidades
                quantities = {}

                items = content["items"]

                if not isinstance(items, list) or not items:
                    raise ValueError("Pedido sem itens válidos")

                for item in items:
                    product_id = item["id"]
                    quantity = item["quantity"]

                    if (type(product_id) is not int or product_id <= 0
                            or type(quantity) is not int or quantity <= 0):
                        raise ValueError("Produto ou quantidade inválida")

                    quantities[product_id] = (
                        quantities.get(product_id, 0) + quantity
                    )

                # 2. Verificar se o pedido já foi reservado
                cursor.execute(
                    "SELECT product_id, quantity FROM reservations WHERE order_id = ?",
                    (order_id,)
                )
                existing = dict(cursor.fetchall())

                if existing:
                    if existing != quantities:
                        raise ValueError("Pedido já reservado com itens diferentes")

                    routing_key = "pedido.estoque_ok"

                else:
                    # 3. Verificar disponibilidade total
                    valid_stock = True

                    for product_id, quantity in quantities.items():
                        cursor.execute(
                            "SELECT quantity FROM products WHERE id = ?",
                            (product_id,)
                        )

                        result = cursor.fetchone()

                        if result is None or quantity > result[0]:
                            valid_stock = False
                            break

                    if not valid_stock:
                        routing_key = "estoque.indisponivel"

                    else:
                        # 4. Baixar estoque e registrar reservas
                        for product_id, quantity in quantities.items():
                            cursor.execute(
                                """
                                UPDATE products
                                SET quantity = quantity - ?
                                WHERE id = ?
                                """,
                                (quantity, product_id)
                            )

                            cursor.execute(
                                """
                                INSERT INTO reservations
                                (order_id, product_id, quantity)
                                VALUES (?, ?, ?)
                                """,
                                (order_id, product_id, quantity)
                            )

                        routing_key = "pedido.estoque_ok"

                connection.commit()

                # 5. Publicar resultado no RabbitMQ
                str_body = sign_content(json_body)

                ch.basic_publish(
                    exchange=EXCHANGE,
                    routing_key=routing_key,
                    body=str_body
                )

            elif method.routing_key == "pedido.excluido":

                # 1. Buscar somente produtos realmente reservados
                cursor.execute(
                    """
                    SELECT product_id, quantity
                    FROM reservations
                    WHERE order_id = ?
                    """,
                    (order_id,)
                )

                reserved_items = cursor.fetchall()

                # 2. Devolver os produtos reservados
                for product_id, quantity in reserved_items:
                    cursor.execute(
                        """
                        UPDATE products
                        SET quantity = quantity + ?
                        WHERE id = ?
                        """,
                        (quantity, product_id)
                    )

                # 3. Remover as reservas devolvidas
                cursor.execute(
                    "DELETE FROM reservations WHERE order_id = ?",
                    (order_id,)
                )

                connection.commit()

            else:
                raise ValueError("Evento desconhecido")

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        ch.basic_ack(method.delivery_tag)

    except (ValueError, KeyError, TypeError) as error:
        print(f"Mensagem inválida: {error}")
        ch.basic_reject(method.delivery_tag, requeue=False)

    except Exception as error:
        print(f"Erro ao processar mensagem: {error}")
        ch.basic_nack(method.delivery_tag, requeue=True)

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
