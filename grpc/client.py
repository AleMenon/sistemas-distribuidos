"""Interface de terminal: uv run --no-sync python client.py."""

import argparse

from cluster_protocol import cluster_pb2 as pb
from client_api import ClusterClient


def show_status(client):
    failed = False
    for result in client.statuses():
        address = client.addresses[result.node_id]
        if result.error:
            failed = True
            print(f"{address} node_id={result.node_id} erro de conexão: {result.error}")
            continue
        status = result.response
        print(f"{address} node_id={result.node_id} role={pb.Role.Name(status.role)} "
              f"term={status.term} commit_index={status.commit_index} "
              f"last_log_index={status.last_log_index} leader_id={status.leader_id or 'desconhecido'}")
    return int(failed)


def show_read(result):
    if result.error:
        print(f"Nó {result.node_id}: erro de conexão ({result.error}).")
    else:
        response = result.response
        text = f"valor={response.value!r}" if response.exists else "chave inexistente"
        print(f"Nó {result.node_id}, commit_index={response.commit_index}: {text}.")


def show_write(result):
    if result.success:
        print(f"Escrita confirmada: nó {result.node_id}, índice {result.index}.")
        return
    messages = {
        "CONNECTION_ERROR": "Erro de conexão com o cluster.",
        "NO_LEADER": "Nenhum líder disponível; limite de descoberta/redirecionamento atingido.",
        "NO_QUORUM": "Sem quórum: escrita rejeitada, não inserida no log.",
        "NOT_COMMITTED": "Escrita não confirmada: timeout aguardando commit por quórum.",
        "NOT_LEADER": "Escrita não confirmada: o nó perdeu a liderança.",
        "BUSY": "Servidor ocupado; escrita não aceita.",
    }
    print(messages.get(result.code, f"Escrita não confirmada: {result.code}."))
    if result.uncertain:
        print("Resultado incerto: a operação pode ser comitada depois; não foi reenviada automaticamente.")


def choose_node(client):
    node_id = int(input("Nó (1 a 4): "))
    if node_id not in client.addresses:
        raise ValueError("ID de nó inválido")
    return node_id


def main():
    parser = argparse.ArgumentParser(description="Cliente gRPC do cluster")
    parser.add_argument("--status", action="store_true", help="Consulta status e sai, sem menu")
    args = parser.parse_args()
    client = ClusterClient()
    if args.status:
        return show_status(client)
    try:
        while True:
            print("\n1. Consultar status dos nós\n2. Gravar chave e valor\n"
                  "3. Consultar chave em um nó\n4. Consultar chave em todos os nós\n5. Sair")
            option = input("Opção: ").strip()
            try:
                if option == "1":
                    show_status(client)
                elif option == "2":
                    node_id = choose_node(client)
                    key, value = input("Chave: "), input("Valor: ")
                    show_write(client.write(node_id, key, value))
                elif option == "3":
                    node_id = choose_node(client)
                    show_read(client.read(node_id, input("Chave: ")))
                elif option == "4":
                    for result in client.read_all(input("Chave: ")):
                        show_read(result)
                elif option == "5":
                    return 0
                else:
                    print("Opção inválida.")
            except ValueError:
                print("Nó inválido; escolha um ID de 1 a 4.")
    except (EOFError, KeyboardInterrupt):
        print("\nCliente encerrado.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
