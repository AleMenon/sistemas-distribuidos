# Cluster gRPC — infraestrutura inicial

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

## Executar os nós e consultar status

Em quatro terminais, execute um comando por terminal, no diretório `grpc/`:

```bash
uv run --no-sync python node.py --id 1
uv run --no-sync python node.py --id 2
uv run --no-sync python node.py --id 3
uv run --no-sync python node.py --id 4
```

`config.py` define os endereços: IDs 1, 2, 3 e 4 usam respectivamente
`127.0.0.1:50051`, `127.0.0.1:50052`, `127.0.0.1:50053` e `127.0.0.1:50054`.
Não é permitido compartilhar a mesma porta entre servidores.

Em outro terminal, consulte todos os nós:

```bash
uv run --no-sync python client.py
```

Um banco novo começa com termo, índice de commit e último índice do log em 0.
Ao reiniciar, o nó recupera esses valores do SQLite, mas sempre assume o papel
`FOLLOWER` e líder desconhecido. O cliente continua consultando os demais
caso algum nó esteja indisponível e retorna código 1 se houver falhas.
Use Ctrl+C no terminal de um nó para encerrá-lo, aguardando até 2 segundos
pelas RPCs em andamento e finalizando o pool de threads. Os demais continuam.
Os logs de ciclo de vida contêm timestamp, ID, papel, termo e commit_index.

`node_state.py` concentra o estado em `Node`, protegido por um lock por instância,
e fornece snapshots imutáveis. `service.py` adapta esse estado para gRPC;
`node.py` configura o servidor e o encerramento. Apenas `GetStatus` está
implementado; as outras RPCs retornam `UNIMPLEMENTED`. Não há votação,
eleição ou replicação.

O comando de testes acima também inicia quatro subprocessos nas portas
configuradas, consulta todos via gRPC e encerra um deles com SIGINT, verificando
que os outros continuam disponíveis. **Pare os nós manuais antes dos testes**
para deixar as quatro portas livres. Os subprocessos de teste são encerrados
automaticamente, inclusive em caso de falha.

## Persistência SQLite

Cada nó mantém seu banco em `data/node_<id>.db`, relativo à pasta deste projeto,
independentemente do diretório de execução. Os arquivos são ignorados pelo Git
e nunca são apagados na inicialização ou no encerramento. A opção
`--data-dir /caminho` permite selecionar outro diretório; os testes usam
diretórios temporários, sem alterar bancos de execução manual.

`storage.py` fornece:

- `initialize`, `load_metadata` e `save_metadata`: inicialização idempotente e
  persistência conjunta de termo e voto (`None` significa voto não concedido).
- `append_entries`, `get_entries` e `last_log_position`: log contíguo a partir
  de 1, com posição `(0, 0)` quando vazio. Inserções em lote são atômicas.
- `truncate_from(index)`: remove o sufixo incluindo o índice informado,
  rejeitando índices comitados e o índice 0.
- `commit_and_apply(index)`: aplica as operações em ordem ao estado final e
  atualiza `commit_index` e `last_applied` em uma única transação. Rejeita
  retrocesso e índices além do log. A validação futura de quórum cabe ao nó.
- `read(key)`: consulta exclusivamente o estado comitado; retorna `None` para
  ausência e preserva string vazia como valor válido.
- `snapshot` e `close`: leitura consistente de metadados/posição e fechamento.

Um lock por instância protege a conexão SQLite compartilhada entre threads.
As mutações usam transações `BEGIN IMMEDIATE`, com rollback em caso de falha;
nenhuma API permite modificar os índices de commit sem aplicar os dados.
`Node` consulta o estado persistido para evitar cópias de metadados desatualizadas.
O servidor termina suas requisições antes de fechar a conexão SQLite.

Os testes incluem reabertura dos bancos, visibilidade de dados, truncamento,
isolamento dos quatro nós, concorrência e rollback após falha durante aplicação.
Execute toda a suíte com o comando `unittest` documentado acima.
