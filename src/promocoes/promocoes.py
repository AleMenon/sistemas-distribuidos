import pika
import json
import base64
import os
import sqlite3
import threading
import resend

import database

from dotenv import load_dotenv

from pika.exceptions import StreamLostError
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

from fastapi import FastAPI, Request

load_dotenv()
RESEND_API_KEY = os.getenv("RESEND_API_KEY")

EXCHANGE = 'ecommerce'
KEY_NAME = 'promocoes'
QUEUE = 'fila.promocoes'

app = FastAPI()
database.config_database()
resend.api_key = RESEND_API_KEY

#NOTE: FastAPI
@app.post("/v1/promotion")
async def promotion(request: Request):
    body = await request.json()
    category = body.get("category")
    discount = body.get("discount")

    conn = sqlite3.connect('promocoes.db')
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT email FROM promotions WHERE category = ?", (category,))
    targets = cursor.fetchall()
    conn.close()

    emails = []

    for (email,) in targets:
        resend.Emails.send(
            {
                "from": "onboarding@resend.dev",
                "to": email,
                "subject": f"Promoção de {category}!",
                "html": f"<strong>Aproveite!</strong> discontos de até {discount}% na categoria {category}."
            }
        )
        emails.append(email)

    return {
        "status": "Promoções disparadas",
        "category": category,
        "emails": emails
    }

@app.on_event("startup")
def startup_event():
    threading.Thread(target=listener, daemon=True).start()

#NOTE: RabbitMQ
def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    json_body = json.loads(bytes.decode(body))

    if not validate_signature(json_body, 'main'):
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return

    content = json_body['content']
    email = content.get('email')
    category = content.get('category')

    if email:
        conn = sqlite3.connect('promocoes.db')
        cursor = conn.cursor()
        if method.routing_key == "interesse.promocao":
            cursor.execute("INSERT OR IGNORE INTO promotions (email, category) VALUES (?, ?)", (email, category))
            print(f"Interesse registrado: {email} deseja promoções de {category}")
        if method.routing_key == "interesse.cancelado":
            print(f"Interesse registrado: {email} não deseja mais receber promoções")
            cursor.execute("DELETE FROM promotions WHERE email = ?", (email,))
        conn.commit()
        conn.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)

def listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct')

    channel.queue_declare(queue=QUEUE, exclusive=True)

    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key='interesse.promocao')
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key='interesse.cancelado')

    channel.basic_consume(queue=QUEUE, on_message_callback=callback)
    channel.start_consuming()


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
