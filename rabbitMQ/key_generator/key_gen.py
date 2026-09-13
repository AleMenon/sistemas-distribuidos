from Crypto.PublicKey import RSA

def gerar_par_de_chaves(nome_servico):
    key = RSA.generate(2048)

    # Exportar Chave Privada (Fica APENAS no próprio serviço)
    private_key = key.export_key()
    with open(f"{nome_servico}_private.pem", "wb") as f:
        f.write(private_key)

    # Exportar Chave Pública (Distribuída para os outros serviços)
    public_key = key.publickey().export_key()
    with open(f"{nome_servico}_public.pem", "wb") as f:
        f.write(public_key)

    print(f"Chaves geradas para {nome_servico}")

# Gerando chaves para o serviço que ENVIA e para o que RECEBE
gerar_par_de_chaves("estoque")
gerar_par_de_chaves("main")
gerar_par_de_chaves("pagamento")
gerar_par_de_chaves("entrega")
