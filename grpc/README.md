# Replicação Pull com gRPC — Sistemas Distribuídos

Trabalho acadêmico: quatro nós independentes, um cliente, eleição inspirada
no Raft e replicação exclusivamente Pull. Cada nó usa o mesmo código, tem um
banco SQLite próprio e assume FOLLOWER, CANDIDATE ou LEADER dinamicamente.
O quórum é sempre 3, mesmo se apenas dois nós estiverem disponíveis.

## Instalação no Arch Linux a partir de um clone limpo

Instale Git e uv pelo gerenciador do sistema. `uv` está no repositório oficial
[Extra do Arch Linux](https://archlinux.org/packages/extra/x86_64/uv/).

```bash
sudo pacman -Syu --needed git uv
git clone https://github.com/AleMenon/sistemas-distribuidos.git
cd sistemas-distribuidos
uv python install 3.11
uv sync --locked --group dev
cd grpc
uv run --no-sync python scripts/generate_proto.py
uv run --no-sync python -m pytest -q
```

O projeto uv fica na raiz do repositório, um nível acima de `grpc/`.
`.python-version` seleciona Python 3.11 e `uv.lock` fixa as versões resolvidas.
`uv sync --locked` instala as dependências sem alterar o lockfile; veja a
[documentação de sincronização do uv](https://docs.astral.sh/uv/concepts/projects/sync/).
Não é preciso ativar manualmente o ambiente nem usar pip no Python do sistema.
Depois da instalação, os demais comandos deste README são executados em `grpc/`.
`--no-sync` pressupõe que a sincronização inicial já foi feita.

### Dependências

| Dependência | Uso |
| --- | --- |
| Python >= 3.11 (ambiente fixado em 3.11) | Execução |
| `grpcio` | Servidor e clientes gRPC |
| `grpcio-tools` | Compilação dos arquivos `.proto` |
| `protobuf` (dependência transitiva) | Mensagens e serialização |
| `pytest` (grupo `dev`) | Suíte completa, incluindo integração |
| `sqlite3`, `threading`, `concurrent.futures`, `subprocess` | Biblioteca padrão Python |

As versões exatas estão em `../uv.lock`; os requisitos estão em
`../pyproject.toml`. O manifesto também atende outro trabalho do repositório e
inclui FastAPI, Pika e outras dependências que **não são usadas por este cluster**.
Não é necessário iniciar nenhum serviço HTTP, RabbitMQ, banco externo ou Docker.
Não é necessário instalar `protoc` separado: a geração usa `grpc_tools.protoc`.

## Gerar protobuf e executar testes

Gerar novamente as mensagens e os stubs Python:

```bash
uv run --no-sync python scripts/generate_proto.py
```

Arquivos gerados: `cluster_protocol/cluster_pb2.py` e
`cluster_protocol/cluster_pb2_grpc.py`, além de `demo_pb2.py` e
`demo_pb2_grpc.py` no mesmo pacote. Não edite esses arquivos manualmente.
O script chama `grpc_tools.protoc` e pode ser executado de qualquer diretório.

Executar a suíte completa, incluindo testes end-to-end e geração em diretório temporário:

```bash
uv run --no-sync python -m pytest -q
```

O contrato está em `proto/cluster.proto`. Índice 0 significa log vazio;
entradas começam em 1. IDs são strings, com líder desconhecido representado
por string vazia. Chaves e valores são strings.

`FetchRequest` identifica o seguidor e informa sua última entrada persistida.
`FetchResponse` informa o prefixo exigido (`prev_log_index`, `prev_log_term`),
as entradas e, opcionalmente, o último índice a preservar no truncamento.
A presença de `truncate_after_index` distingue “sem truncamento” de “preservar
índice 0”. A implementação valida o prefixo, nunca permite truncar
dados comitados e negocia prefixos anteriores se houver divergência.
As regras de consenso e a validação de índices são responsabilidades dos
handlers e das camadas de negócio; protobuf define apenas os formatos.

## Arquitetura e fluxo de uma operação

| Arquivo | Responsabilidade |
| --- | --- |
| `node.py`, `config.py` | Processo, endereços, servidor, temporizadores e encerramento |
| `node_state.py` | Estado do nó, lock, Write/Read e espera de commit |
| `election.py` | Termos, votação e transições de papéis |
| `replication.py` | Fetch periódico, heartbeat, match_index, commit e recuperação |
| `storage.py` | Transações SQLite, metadados, log e estado comitado |
| `service.py`, `transport.py` | Adaptação e chamadas gRPC |
| `client.py`, `client_api.py` | Menu, leituras e redirecionamento de escritas |
| `fault_injection.py`, `demo.py` | Controles opcionais de demonstração |

1. O cliente envia Write a um nó; um não líder indica o líder conhecido.
2. O líder verifica a atividade do quórum e persiste a nova entrada no log.
3. Seguidores enviam Fetch a cada 100 ms e persistem as entradas recebidas.
4. No próximo Fetch, informam o último índice/termo armazenado. O líder valida
   esse prefixo e atualiza a confirmação de cada seguidor.
5. Com três cópias confirmadas e uma entrada do termo atual, o líder comita e
   aplica o prefixo em uma transação, acordando o Write para responder sucesso.
6. Os seguidores aplicam o novo commit ao recebê-lo em respostas de Fetch.
   Read consulta só esse estado aplicado; uma réplica pode estar temporariamente
   atrasada em relação ao líder, mas não mostra dados não comitados.

Não existe envio espontâneo de entradas pelo líder. RequestVote realiza eleição,
GetStatus auxilia a descoberta e Fetch realiza replicação/heartbeat. Toda
comunicação entre processos usa gRPC, inclusive os controles de demonstração.

O lock do nó protege transições, escrita e tratamento de Fetch; o lock do SQLite
é adquirido depois dele. Nenhum caminho normal adquire esses locks na ordem
inversa. RPCs externas e joins de threads ocorrem fora do lock do nó.
`Condition.wait` libera o lock durante a espera do Write; o serviço limita as
escritas em espera para deixar capacidade para Fetch. Estes cuidados são
verificados por testes de concorrência, mas não constituem prova formal.

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
Não é permitido compartilhar a mesma porta entre servidores. Para executar
outro cluster local, defina `CLUSTER_PORTS` com quatro portas distintas, na ordem
dos IDs. Todos os nós e clientes desse cluster devem receber o mesmo valor:

```bash
export CLUSTER_PORTS=51051,51052,51053,51054
```

Sem essa variável, as portas padrão continuam sendo 50051 a 50054.

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
que os outros continuam disponíveis. Com pytest, os testes de processos usam
portas livres próprias e bancos temporários. Os subprocessos são encerrados
automaticamente, inclusive em caso de falha. A execução legada por `unittest`
continua disponível, mas não executa os novos testes pytest e usa as portas
configuradas para os testes antigos; nesse caso, pare antes os nós manuais.

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
Execute toda a suíte com o comando `pytest` documentado acima.

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
termo/índice do seguidor divergir, a resposta contém `truncate_after_index` e
nenhuma entrada: o seguidor remove apenas o sufixo não comitado e faz novo Fetch.
Se o índice exceder o log do líder, volta ao último índice do líder; caso contrário,
recua um índice por tentativa até encontrar um prefixo comum. Nesse ponto recebe
as entradas faltantes e pode aplicar o commit informado. Respostas de truncamento
não avançam o commit, pois ainda não validaram um prefixo comum.
Conflitos com entradas comitadas são rejeitados. Dados recebidos mas não
comitados continuam invisíveis em `read`.

O progresso da recuperação está no próprio log SQLite: reiniciar entre truncar
e buscar entradas retoma a sincronização a partir da última posição persistida.
Cada resposta é aplicada em uma transação; falhas durante a aplicação desfazem
as alterações daquela resposta. O commit local é limitado às entradas existentes
e ao prefixo validado. Não há alteração no protocolo nem na lógica de eleição.

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

## Demonstração controlada de falhas

Por padrão não existe injeção ativa e o serviço auxiliar `DemoControl` não é
registrado: suas RPCs retornam `UNIMPLEMENTED`. Para habilitá-lo, inicie cada
nó com `--demo`. Use um diretório separado para os dados da apresentação;
nenhum banco é apagado automaticamente. Em quatro terminais, execute:

```bash
uv run --no-sync python node.py --id 1 --demo --data-dir data/demo
uv run --no-sync python node.py --id 2 --demo --data-dir data/demo
uv run --no-sync python node.py --id 3 --demo --data-dir data/demo
uv run --no-sync python node.py --id 4 --demo --data-dir data/demo
```

Abra `client.py` em outro terminal. Consulte o status e identifique o líder
antes de cada cenário. Os comandos abaixo usam ID 1 como exemplo de líder:
**substitua pelo ID efetivamente eleito**. Os controles usam apenas gRPC,
na mesma porta do nó, e são voláteis: reiniciar ou mudar de mandato os limpa.
`arm-crash` e `pause-commit` são recusados se o alvo não for líder.

### Cenário 1 — escrita e leitura distribuída

Com os quatro nós ativos, execute `uv run --no-sync python client.py`.
Escolha **1** para identificar o líder, **2** para gravar `apresentacao=valor`
(pode escolher inicialmente um seguidor) e **4** para consultar a chave em todos.
Resultado esperado: escrita confirmada, seguida de visibilidade nos quatro nós,
com commit_index pelo menos igual ao índice da operação. Se alguma leitura
ainda estiver atrasada, repita a consulta após o próximo Fetch.

### Cenário 2 — eleição após queda do líder

Use `uv run --no-sync python client.py --status` para registrar líder e termo.
Encerre esse processo com Ctrl+C no respectivo terminal. Repita o comando de
status até um dos três sobreviventes aparecer como LEADER com termo maior.
Resultado esperado: os sobreviventes continuam acessíveis e elegem novo líder.
Reinicie o nó encerrado com seu comando original e o mesmo diretório de dados.

### Cenário 3 — perda de quórum

Com os quatro nós ativos, encerre dois seguidores por Ctrl+C. No menu do cliente,
escolha **2** e tente gravar uma chave nova no líder. Resultado esperado:
nenhum sucesso; após detectar inatividade, `NO_QUORUM` rejeita sem inserir.
Se a tentativa coincidir com a detecção da falha, pode ocorrer timeout de commit
ou perda de liderança. Com apenas dois nós, tampouco há votos para novo líder.
Reinicie os dois processos com os comandos originais, aguarde eleição e
descoberta via `client.py --status` e faça uma nova escrita: deve confirmar.
Uma operação que chegou a entrar no log antes da detecção pode comitar depois;
timeout não significa cancelamento.

### Cenário 4A — seguidor desatualizado

1. Identifique um seguidor e encerre-o com Ctrl+C em seu terminal.
2. No cliente, grave várias chaves com sucesso usando os três nós restantes.
3. Reinicie o seguidor com exatamente o mesmo ID e `--data-dir data/demo`.
4. Consulte seu status até `last_log_index` e `commit_index` acompanharem o
   líder; leia as chaves diretamente nesse seguidor.

Esse cenário também funciona no modo normal, sem controles auxiliares.

### Cenário 4B — crash após persistência e antes de replicação

```bash
uv run --no-sync python demo.py --id 1 arm-crash
```

1. Pelo cliente, envie ao líder uma escrita, por exemplo `orfao=antigo`.
2. O processo do líder termina abruptamente com código **86**, sem executar
   encerramento organizado. A entrada já está persistida no SQLite, mas o
   lock compartilhado com Fetch impede sua replicação antes do crash.
3. O cliente informa erro de conexão/resultado incerto; não reenvie essa
   operação. Aguarde a eleição entre os três sobreviventes.
4. Grave outra chave, por exemplo `substituta=novo`, e aguarde sucesso.
5. Reinicie o líder antigo com o mesmo ID e diretório de dados. Ele retorna
   como FOLLOWER, trunca a entrada conflitante e busca a entrada correta.
6. Consulte esse nó: `orfao` deve estar ausente, `substituta` deve existir, e
   seus índices devem acompanhar os do líder atual.

O crash é disparado somente pela próxima escrita aceita e persistida. Uma
escrita rejeitada por falta de quórum não dispara o crash. Para desarmá-lo
antes de uma escrita, use `reset`.

### Cenário 5 — entrada replicada, mas não comitada

```bash
uv run --no-sync python demo.py --id 1 pause-commit
```

1. Grave uma chave nova no cliente. O Write termina com `NOT_COMMITTED`, pois
   a pausa impede o avanço do commit mesmo com confirmações suficientes.
2. Consulte os status: o último índice avança nos seguidores, enquanto o
   commit_index permanece anterior à nova entrada.
3. Leia a chave em um seguidor: ela continua inexistente. Heartbeats e Fetch
   permanecem ativos durante toda a pausa; não é necessário acertar uma janela
   curta de tempo para realizar a leitura.
4. Retome o commit:

```bash
uv run --no-sync python demo.py --id 1 resume-commit
```

Os próximos FetchRequests permitem calcular o quórum normalmente, e as
respostas propagam o commit. Aguarde o avanço do commit_index no seguidor e
repita a leitura: agora o valor deve existir. Não é necessário reenviar Write.

### Inspecionar e restaurar execução normal

```bash
uv run --no-sync python demo.py --id 1 status
uv run --no-sync python demo.py --id 1 reset
```

`reset` remove a pausa e desarma o crash naquele nó, sem alterar seus dados.
Repita para outros nós controlados. Para remover completamente o acesso às
RPCs auxiliares, encerre os processos e reinicie **sem `--demo`**, mantendo o
mesmo ID e diretório se quiser preservar os dados. A execução normal original
usa `node.py --id N` e os bancos em `data/`.

`tests/test_faults.py` automatiza A, B e C com quatro subprocessos e bancos
temporários, verifica a persistência após o crash, a ausência de replicação da
entrada órfã, a recuperação e a retomada após `reset`. A suíte normal verifica
também que os controles são inacessíveis sem `--demo`. Para executar somente
os cenários, pare os nós manuais e rode:

```bash
uv run --no-sync python -m unittest discover -s tests -p test_faults.py -v
```

## Testes end-to-end com pytest

`pytest` está registrado no grupo `dev` do projeto uv no diretório pai.
Para preparar um ambiente novo, execute `uv sync --group dev` nesse diretório.
Em `grpc/`, use:

```bash
# Suíte completa: testes anteriores + end-to-end
uv run --no-sync python -m pytest -q

# Somente os cenários obrigatórios end-to-end
uv run --no-sync python -m pytest -v -m e2e
```

`tests/test_e2e.py` cobre os cinco cenários obrigatórios:
escrita/leitura distribuída, eleição após queda do líder, perda/restauração de
quórum, recuperação de seguidor atrasado, recuperação de líder com entrada
conflitante e isolamento de dados não comitados.
Também verifica três escritas gRPC simultâneas e reinicialização completa do
cluster, preservando os dados comitados.

Cada teste inicia quatro processos reais de `node.py`, com bancos em `tmp_path`
e quatro portas livres definidas por `CLUSTER_PORTS`. Não depende de um ID
específico vencer a eleição. Os controles `--demo` tornam crash e pausa de
commit reproduzíveis. A sincronização consulta estados e dados via gRPC com
polling e deadlines, em vez de presumir prontidão após uma espera fixa.
A inspeção SQLite em modo somente leitura comprova a persistência e o
truncamento; não altera nem controla os nós.

A fixture encerra os processos em `finally`, tenta encerramento organizado e
usa kill apenas se o processo exceder o prazo. Em falhas de convergência, o teste
inclui os últimos logs dos nós no diagnóstico. Os testes anteriores de processos
também recebem portas isoladas quando executados pelo pytest.

## Limites conhecidos e cuidados na apresentação

- Este é um cluster acadêmico fixo de quatro nós locais, não uma implementação
  completa de Raft para produção. Não há mudança dinâmica de membros,
  autenticação, TLS ou tolerância a nós maliciosos.
- Leituras são eventualmente consistentes, não linearizáveis. Timeout de Write
  pode deixar uma operação pendente que comitará depois. Não há deduplicação.
- Sem escrita do mandato atual, entradas antigas ainda não comitadas podem
  continuar pendentes: não há inserção automática de operação vazia na eleição.
- O log não tem compactação ou snapshots; Fetch envia o sufixo inteiro e usa
  os limites padrão de mensagem do gRPC. Use dados pequenos na apresentação.
- Recuperação conflitante recua um índice por Fetch. Discos cheios/corrompidos,
  corrupção manual de bancos e partições arbitrárias não têm recuperação
  especializada. Use apenas um processo por ID e diretório de dados.
- Testes automatizados não substituem a defesa obrigatória com explicação do
  código. Não foi encontrado `AGENTS.md` na árvore/ancestrais examinados na
  revisão; o PDF e os requisitos da tarefa são as referências verificadas.
- Bancos `data/node_<id>.db` nunca são apagados automaticamente. Para uma
  demonstração sem dados antigos, escolha outro `--data-dir` em todos os nós;
  para recuperar falhas, preserve exatamente o diretório usado anteriormente.

## Resultado da revisão final — 16/11/2026

Validação executada em Arch Linux, Python 3.11.15, numa cópia limpa dos arquivos
versionados com as alterações desta revisão, sem ambiente virtual, caches ou
bancos preexistentes. `uv sync --locked --group dev` criou o ambiente; a geração
protobuf reproduziu os quatro arquivos versionados; `node.py`, `client.py` e
`demo.py` responderam a `--help`. A suíte completa terminou com **64 testes e
18 subtestes aprovados em 41,43 segundos**. O comando pacman e o clone remoto
estão documentados, mas não foram reexecutados: Git/uv já estavam instalados e
a revisão utilizou a cópia local para incluir as alterações ainda não publicadas.

Foram conferidos os critérios do PDF: voto/termo persistidos, comparação de
logs, quórum 3, Pull e heartbeat por Fetch, commit atômico, leitura somente
comitada, redirecionamento, recuperação, truncamento e reinicialização.
Não foram identificados deadlocks ou condições de corrida evidentes nos
caminhos revisados; os testes de concorrência e processos passaram.
Não foi necessária alteração na lógica de consenso. Foram corrigidos textos
desatualizados e removido um import não utilizado. Caches locais foram removidos
e o `.gitignore` passou a cobrir caches pytest, ambientes e arquivos SQLite,
inclusive journals/WAL. Os bancos persistentes existentes foram preservados.
