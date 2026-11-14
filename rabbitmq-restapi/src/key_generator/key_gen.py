from Crypto.PublicKey import RSA

def generate_keys(service):
    key = RSA.generate(2048)

    private_key = key.export_key()
    with open(f"{service}_private.pem", "wb") as f:
        f.write(private_key)

    public_key = key.publickey().export_key()
    with open(f"{service}_public.pem", "wb") as f:
        f.write(public_key)

    print(f"Chaves geradas para {service}")

generate_keys("estoque")
generate_keys("main")
generate_keys("pagamento")
generate_keys("entrega")
generate_keys("promocoes")
