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

Em outro terminal, abra o menu do cliente:

```bash
uv run --no-sync python client.py
```

Um banco novo começa com termo, índice de commit e último índice do log em 0.
Ao reiniciar, o nó recupera esses valores do SQLite, mas sempre assume o papel
`FOLLOWER` e líder desconhecido. O cliente continua consultando os demais
caso algum nó esteja indisponível. A opção `--status` consulta os quatro nós
sem abrir o menu e retorna código 1 se houver falhas.
Use Ctrl+C no terminal de um nó para encerrá-lo, aguardando até 2 segundos
pelas RPCs em andamento e finalizando o pool de threads. Os demais continuam.
Os logs de ciclo de vida contêm timestamp, ID, papel, termo e commit_index.

`node_state.py` concentra o estado em `Node`, protegido por um lock por instância,
e fornece snapshots imutáveis. `service.py` adapta esse estado para gRPC;
`node.py` configura o servidor e o encerramento. `GetStatus`, `RequestVote`,
`Fetch`, `Write` e `Read` estão implementados. `Read` consulta exclusivamente
o banco final e retorna seu commit_index em uma leitura protegida pelo lock.

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
  retrocesso e índices além do log. A validação de quórum cabe à replicação do nó.
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

## Eleição de líder

Os mesmos comandos de execução ativam automaticamente a eleição.
`config.py` define timeout aleatório entre 1 e 2 segundos, prazo de 0,5 segundo
por RPC de voto e quórum fixo de 3 nós. Cada nova tentativa sorteia outro timeout.

`election.py` implementa candidatura, voto persistido, comparação do log por
`(last_log_term, last_log_index)`, contagem de votos distintos e transição para
seguidor ao observar termo superior. Usa o mesmo lock do estado do nó; chamadas
de rede acontecem fora desse lock. `transport.py` envia votos em gRPC com prazo
limitado; `service.py` adapta a RPC `RequestVote` para as regras de eleição.
Termo e voto são gravados antes de enviar solicitações ou conceder votos.

O Fetch agora funciona como heartbeat: uma resposta válida do líder renova o
timeout e interrompe candidaturas de termo igual ou inferior. Falhas de conexão
não renovam o timeout, permitindo nova eleição quando o líder fica inacessível.

Os testes verificam quórum, votos repetidos/concorrentes, comparação de logs,
termos superiores, respostas atrasadas, recuperação do voto e timeout com nova
tentativa. O teste dos quatro processos aguarda a primeira eleição via gRPC,
verifica estabilidade durante vários timeouts e encerra o líder para verificar
uma nova eleição com os três nós restantes.

## Replicação Pull (Fetch)

Os comandos de execução permanecem os mesmos. `replication.py` executa o loop
em uma thread própria, sem bloquear o servidor. `config.py` configura:

- `FETCH_INTERVAL = 0.1`: espera de 100 ms entre tentativas.
- `FETCH_RPC_TIMEOUT = 0.2`: prazo de cada consulta de descoberta ou Fetch.
- `HEARTBEAT_TIMEOUT = 0.5`: inatividade após 500 ms sem Fetch do seguidor.

Quando não conhece o líder, o nó consulta `GetStatus` dos pares por gRPC e
confirma a liderança pela resposta de `Fetch`. Candidatos também podem fazer
essa descoberta para abandonar uma candidatura ao encontrar um líder válido.
O seguidor envia ID, termo atual e última posição persistida do log. O líder
responde apenas a pedidos; não há envio espontâneo de entradas nem heartbeat Push.

O líder mantém `last_fetch_time` por seguidor usando `time.monotonic()` e
atualiza `active_followers` periodicamente. `activity()` retorna cópias desses
mapas sob lock. Os registros são descartados ao mudar de termo ou sair da liderança.
Atividade não é confirmação de commit e não faz o índice de commit avançar.

`Storage.apply_fetch` valida o prefixo, persiste as entradas e aplica apenas até
o commit informado e o prefixo sincronizado, em uma única transação. Se o último
termo/índice do seguidor divergir, a resposta inclui o log completo para validar
o prefixo e substituir o sufixo não comitado; conflitos com entradas comitadas
são rejeitados. Dados recebidos mas não comitados continuam invisíveis em `read`.

O líder calcula commit a partir das confirmações validadas nos FetchRequests.
Os testes também verificam Fetch periódico,
expiração de atividade, respostas obsoletas, falhas de comunicação, proteção de
dados comitados e encerramento do loop antes do fechamento do SQLite.

## Escritas e confirmação por quórum

`Write(key, value)` aceita entradas somente no líder. Seguidores e candidatos
retornam `success=false`, `error="NOT_LEADER"`, índice 0 e o ID do líder
conhecido (ou string vazia). O líder verifica se há pelo menos dois seguidores
ativos, além de si mesmo; caso contrário retorna `NO_QUORUM`, índice 0, sem
inserir a operação. Um líder recém-eleito aguarda os primeiros Fetches para
conhecer a atividade das réplicas.

`Node.write` protege a verificação do papel, atribuição de índice/termo e
persistência com o mesmo lock usado pelas eleições. Depois de persistir a
entrada com índice sequencial e termo atual, aguarda usando `threading.Condition`,
que libera o lock durante a espera para permitir Fetches e eleições.

O líder mantém `match_index` por seguidor. Somente um FetchRequest cujo último
índice e termo correspondam ao log do líder confirma armazenamento; enviar
entradas em uma resposta não conta como confirmação. A própria cópia do líder
participa do quórum fixo de 3. O maior índice confirmado por três nós só avança
o commit diretamente se sua entrada pertencer ao termo atual; isso também
comita seu prefixo de termos anteriores. Mudanças de mandato descartam as
confirmações em memória, exigindo novos Fetches.

O avanço usa a transação SQLite de `commit_and_apply` e notifica as escritas
aguardando. O índice atualizado segue nas respostas de Fetch: uma réplica que
apenas recebeu entradas continua sem aplicá-las até receber a atualização.

Respostas de Write:

- `success=true`, erro vazio: operação comitada e mandato ainda válido.
- `NOT_COMMITTED`: expirou `WRITE_TIMEOUT` (1,5 segundo), sem confirmação.
- `NOT_LEADER`: não era líder ou perdeu o mandato durante a espera.
- `NO_QUORUM`: menos de 3 nós conhecidos como ativos; nada foi inserido.
- `BUSY`: limite de escritas simultâneas no serviço atingido; nada foi inserido.

O serviço limita escritas em espera a `MAX_WORKERS - 1`, deixando capacidade
para Fetch e votação. Também limita a espera ao prazo da RPC, quando presente.
O índice retornado é 0 quando não houve inserção; caso contrário identifica a
entrada persistida. Timeout não desfaz essa entrada: ela pode ser comitada
depois. Não há deduplicação, portanto repetir Write cria outra operação.

Os testes verificam confirmação com três cópias, timeout com somente duas,
rejeição sem quórum ativo, propagação posterior, proteção contra confirmações
divergentes e segurança ao mudar de mandato. O teste com quatro processos
verifica sucesso de Write e avanço do commit em todos os nós via gRPC.

## Cliente de terminal

`client.py` contém apenas o menu e a apresentação; `client_api.py` concentra
chamadas gRPC, descoberta e redirecionamento, sem acessar bancos ou objetos Node.

O menu oferece status dos nós, gravação, leitura em um nó escolhido, leitura
em todos os nós e saída. Na gravação, escolha qualquer nó inicial: o cliente
envia Write a ele e segue `NOT_LEADER` quando nenhuma entrada foi inserida.
Se o líder for desconhecido ou o destino estiver indisponível antes do envio,
consulta GetStatus dos nós disponíveis para descobrir o líder de maior termo.
Cada nó recebe no máximo uma tentativa de Write por operação, com prazo de
2 segundos por RPC; ciclos de redirecionamento são interrompidos.

Erros de conexão, ausência de líder, falta de quórum, timeout de commit e
servidor ocupado têm mensagens distintas. Somente `success=true` é apresentado
como escrita confirmada. Falha de comunicação após enviar Write, timeout de
commit ou perda de mandato após inserção são resultados incertos: o cliente
não reenvia automaticamente, pois não existe deduplicação de operações.

Leituras consultam diretamente o nó escolhido, sem redirecionamento, e mostram
ID e commit_index local. Uma chave inexistente é distinta de um valor vazio.
Leituras podem estar atrasadas até o próximo Fetch, mas nunca expõem operações
não comitadas. Consultas a todos os nós continuam mesmo com algum indisponível.
Ctrl+C ou fim da entrada encerra o cliente sem traceback.

```bash
uv run --no-sync python client.py
uv run --no-sync python client.py --status
```
