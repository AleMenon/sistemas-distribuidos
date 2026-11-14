# Cluster gRPC — protocolo

O ambiente `uv` e as dependências `grpcio` e `grpcio-tools` estão no
projeto do diretório pai. Execute os comandos abaixo neste diretório (`grpc`).

Gerar novamente as mensagens e os stubs Python:

```bash
uv run --no-sync python scripts/generate_proto.py
```

Arquivos gerados: `cluster_protocol/cluster_pb2.py` e
`cluster_protocol/cluster_pb2_grpc.py`. Não edite esses arquivos manualmente.
O script chama `grpc_tools.protoc` e pode ser executado de qualquer diretório.

Executar os testes (incluem geração em diretório temporário):

```bash
uv run --no-sync python -m unittest discover -s tests -v
```

O contrato está em `proto/cluster.proto`. Índice 0 significa log vazio;
entradas começam em 1. IDs são strings, com líder desconhecido representado
por string vazia. Chaves e valores são strings.

`FetchRequest` identifica o seguidor e informa sua última entrada persistida.
`FetchResponse` informa o prefixo exigido (`prev_log_index`, `prev_log_term`),
as entradas e, opcionalmente, o último índice a preservar no truncamento.
A presença de `truncate_after_index` distingue “sem truncamento” de “preservar
índice 0”. A futura implementação deverá validar o prefixo, nunca truncar
dados comitados e negociar prefixos anteriores se houver divergência.
As regras de consenso e a validação de índices são responsabilidades dos
handlers futuros; protobuf define apenas os formatos das mensagens.

Esta etapa não implementa servidores, handlers, eleição ou replicação.
As classes de serviço geradas automaticamente são apenas scaffolding do gRPC.
