# Design

## Visão geral

O sistema é um pipeline serverless de ingestão de eventos composto por duas funções Lambda, um Kinesis Data Stream e um bucket S3. A primeira Lambda expõe um endpoint HTTPS (via API Gateway), valida o payload e publica no Kinesis. A segunda Lambda é acionada pelo Kinesis em lotes controlados por tamanho (30 registros) e janela de tempo (20 segundos), agrega os registros, separa por sexo e grava arquivos Parquet em um bucket S3 particionado.

### Diagrama de arquitetura

```
                         HTTPS POST
   Cliente  ───────────────────────────────►  API Gateway (REST/HTTP API)
                                                      │
                                                      ▼
                                          ┌───────────────────────┐
                                          │  Lambda de Ingestão    │
                                          │  - valida schema       │
                                          │  - PutRecord no Kinesis│
                                          └───────────┬────────────┘
                                                      │ PutRecord (partition key = sexo)
                                                      ▼
                                          ┌───────────────────────┐
                                          │  Kinesis Data Stream   │
                                          └───────────┬────────────┘
                                                      │ Event Source Mapping
                                                      │ BatchSize=30
                                                      │ MaximumBatchingWindowInSeconds=20
                                                      ▼
                                          ┌───────────────────────┐
                                          │  Lambda de Agregação   │
                                          │  - decode records      │
                                          │  - agrupa por sexo     │
                                          │  - escreve Parquet      │
                                          └───────────┬────────────┘
                                                      │ PutObject
                                                      ▼
                                          ┌───────────────────────┐
                                          │  Bucket S3             │
                                          │  sexo=M/ *.parquet     │
                                          │  sexo=F/ *.parquet     │
                                          └───────────────────────┘
                                                      │ (falhas)
                                                      ▼
                                                 SQS DLQ
```

## Decisão de arquitetura central: agregação por lote

O requisito "30 registros OU 20 segundos" é atendido nativamente pelo **Event Source Mapping do Kinesis para Lambda**, sem necessidade de store de estado intermediário (DynamoDB):

- `BatchSize = 30`: a Lambda é invocada quando 30 registros estão disponíveis.
- `MaximumBatchingWindowInSeconds = 20`: se 30 registros não forem atingidos, a Lambda é invocada com o que houver após 20 segundos.

Isso simplifica drasticamente a operação e elimina a necessidade de buffer externo e lógica de flush por timeout. A separação por sexo (30 para ambos) é feita dentro da Lambda, após o recebimento do lote.

**Observação sobre semântica do lote:** com essa abordagem, o lote de até 30 registros pode conter uma mistura de `M` e `F`. A Lambda separa o lote em dois grupos e grava um arquivo Parquet por grupo presente no lote. O limite de 30 refere-se ao tamanho do lote agregado pelo Kinesis, conforme confirmado (mesmo volume de 30 para ambos os casos).

### Alternativa considerada e descartada

Buffer com DynamoDB acumulando até 30 registros por sexo e flush por TTL/agendamento. Descartada por adicionar complexidade operacional (gestão de estado, concorrência, limpeza) sem ganho frente ao requisito, que é bem atendido pelo batching nativo do event source mapping.

## Decisão sobre a partition key do Kinesis

A partition key dos records publicados é o campo **`sexo`**. Registros com o mesmo valor de `sexo` compartilham a mesma partition key e, portanto, o mesmo hash de distribuição entre shards.

### Implicações e trade-offs

- **Distribuição por shard:** o Kinesis roteia records por `hash(partition_key)`. Com `sexo` como key, existem apenas 2 valores de hash distintos (`M` e `F`). A partition key **não** segrega dados de forma diretamente consumível por categoria — um shard pode conter múltiplas keys e a mesma key pode ser atendida por shards diferentes conforme o número de shards. A separação efetiva por sexo continua ocorrendo na Lambda de agregação e no particionamento do S3.
- **Risco de hot shard:** com poucos valores de partition key (2) e possível desbalanceamento entre M e F, o tráfego pode concentrar em poucos shards. Em modo on-demand o Kinesis ajusta a capacidade automaticamente, o que mitiga parte do risco em cargas moderadas; em modo provisionado, deve-se monitorar `WriteProvisionedThroughputExceeded` e revisar a estratégia caso ocorra throttling.
- **Motivação:** a escolha foi feita visando flexibilidade futura para novas categorias de agrupamento. À medida que mais valores de categoria forem introduzidos, a distribuição entre shards tende a melhorar.

### Nota de evolução

Se no futuro o volume crescer significativamente e a cardinalidade de categorias permanecer baixa, recomenda-se reavaliar a partition key (ex.: usar uma key composta `sexo#<id>` ou `sexo#<bucket-aleatório>`) para preservar o agrupamento lógico por categoria enquanto distribui a carga entre shards e evita hot shards. Essa mudança afeta apenas a Lambda de ingestão e não altera o contrato de saída no S3.


## Componentes e interfaces

### 1. API Gateway

- Tipo: HTTP API (API Gateway v2) — menor custo e latência para proxy simples de Lambda.
- Rota: `POST /eventos`.
- Integração: proxy Lambda (payload format 2.0).
- TLS: HTTPS imposto por padrão (API Gateway só expõe endpoints HTTPS).

### 2. Lambda de Ingestão

- Runtime sugerido: Python 3.12 (consistência com a Lambda de agregação, que usa DuckDB).
- Responsabilidades:
  1. Fazer parse do corpo JSON.
  2. Validar o schema (ver seção Modelos de Dados).
  3. Em caso de sucesso, publicar no Kinesis via `put_record` usando `sexo` como partition key.
  4. Retornar 202 em sucesso, 400 em erro de validação, 5xx em falha de publicação.
- Variáveis de ambiente:
  - `KINESIS_STREAM_NAME`
- Permissões IAM: `kinesis:PutRecord` no stream específico; `logs:*` básico.

Contrato de resposta:

| Situação                     | Status | Corpo                                           |
|------------------------------|--------|-------------------------------------------------|
| Registro válido publicado    | 202    | `{"status":"accepted","id":"<id>"}`             |
| Corpo ausente/JSON inválido  | 400    | `{"error":"invalid_body","detail":"..."}`       |
| Falha de validação de campo  | 400    | `{"error":"validation_error","field":"<campo>"}`|
| Falha ao publicar no Kinesis | 502    | `{"error":"publish_failed"}`                    |

### 3. Kinesis Data Stream

- Nome: `eventos-pessoas-stream`.
- Modo de capacidade: On-demand (simplifica POC; escala automática). Pode ser trocado por modo provisionado com N shards se o volume for conhecido.
- Retenção: padrão de 24 horas (suficiente para reprocessamento de curto prazo).

### 4. Lambda de Agregação

- Runtime: Python 3.12.
- Trigger: Event Source Mapping do Kinesis.
  - `BatchSize = 30`
  - `MaximumBatchingWindowInSeconds = 20`
  - `StartingPosition = LATEST` (ou `TRIM_HORIZON` conforme necessidade de reprocessar histórico)
  - `BisectBatchOnFunctionError = true` (isola registros problemáticos em retry)
  - `MaximumRetryAttempts` configurado, com destino de falha (DLQ) em SQS.
- Dependências: `duckdb` (via Lambda Layer) para escrita Parquet. A mesma dependência é usada pela Lambda de consulta, mantendo o layer pequeno (pandas+pyarrow ultrapassavam o limite de 250MB do Lambda).
- Responsabilidades:
  1. Decodificar cada record do Kinesis (base64 → JSON).
  2. Agrupar registros por `sexo`.
  3. Para cada grupo, serializar em Parquet com DuckDB (gravação em arquivo local em `/tmp` e upload via boto3) em `s3://<bucket>/sexo=<valor>/<arquivo>.parquet`.
- Variáveis de ambiente:
  - `OUTPUT_BUCKET`
- Permissões IAM: `s3:PutObject` no bucket; leitura do stream Kinesis (`kinesis:GetRecords`, `GetShardIterator`, `DescribeStream`, `ListShards`); envio para a DLQ (`sqs:SendMessage`); `logs:*` básico.

### 5. Bucket S3

- Nome: `eventos-pessoas-<account-id>-<region>` (nome globalmente único).
- Estrutura de chaves (partition style Hive, compatível com Athena/Glue):

```
s3://<bucket>/sexo=M/<timestamp>-<uuid>.parquet
s3://<bucket>/sexo=F/<timestamp>-<uuid>.parquet
```

- Criptografia: SSE-S3 (ou SSE-KMS se houver requisito de compliance).
- Versionamento: habilitado (proteção contra sobrescrita acidental; nomes já são únicos).
- Bloqueio de acesso público: habilitado.

### 6. Dead-Letter Queue (SQS)

- Recebe metadados dos lotes que falharam após o número máximo de retries da Lambda de agregação.
- Permite inspeção e reprocessamento manual.

## Infraestrutura como código (Serverless Framework)

Todos os recursos são provisionados e versionados com o **Serverless Framework** (arquivo `serverless.yml`), que gera e aplica uma stack do CloudFormation.

- **Funções Lambda:** declaradas no bloco `functions`.
  - `ingestao`: evento `httpApi` (`POST /eventos`) — o Serverless cria o API Gateway HTTP API automaticamente.
  - `agregacao`: evento `stream` apontando para o ARN do Kinesis, com `batchSize: 30`, `batchWindow: 20` (mapeado para o `MaximumBatchingWindowInSeconds` do event source mapping), `bisectBatchOnFunctionError: true`, `maximumRetryAttempts` e `destinations.onFailure` apontando para a DLQ.
- **Recursos não nativos** (Kinesis Data Stream, bucket S3, DLQ SQS): declarados no bloco `resources` como recursos CloudFormation, ou via plugins quando conveniente.
- **Permissões IAM:** definidas por função usando `iam.role.statements` (ou `iamRoleStatements`), seguindo o princípio de menor privilégio descrito em cada componente acima.
- **Dependências Python / Parquet:** o layer com `duckdb` é empacotado com o `serverless-python-requirements` (nativo no v4). A agregação e a consulta usam a mesma dependência (DuckDB lê/escreve Parquet), o que mantém o layer bem abaixo do limite de 250MB — pandas+pyarrow juntos o ultrapassavam. Pontos de configuração essenciais (`custom.pythonRequirements`):
  - `useUv: false` + `fileName: requirements.txt`: **obrigatório**. O v4, ao detectar um `uv.lock` no projeto, roda `uv export` e ignora o `requirements.txt` da raiz — como o `uv.lock` não contém as deps de runtime, o layer saía vazio e todas as Lambdas falhavam com `No module named ...`. Forçar o `requirements.txt` resolve.
  - `dockerizePip: false`: o devcontainer é Linux x86_64 compatível com o runtime Lambda, então o pip baixa os wheels nativos corretos sem Docker.
  - `slim: false`: com `slim: true`, os binários nativos `.so` (ex.: `duckdb`) eram removidos.
  - `useStaticCache: false` / `useDownloadCache: false` / `invalidateCaches: true`: evitam reusar um layer vazio cacheado durante a depuração.
- **Variáveis de ambiente:** `KINESIS_STREAM_NAME` (ingestão) e `OUTPUT_BUCKET` (agregação) resolvidas via referências do `serverless.yml`.

> Nota de licenciamento: a partir da v4, o Serverless Framework exige registro/licença para uso comercial acima do limite de faturamento definido pela Serverless Inc.; o uso é gratuito para pequenas organizações e projetos open source. Para esta POC o uso gratuito é suficiente, mas o ponto deve ser revisado antes de produção. Alternativas sem essa restrição incluem AWS SAM ou AWS CDK, caso o licenciamento se torne um impedimento.

## Ambiente de desenvolvimento (Devcontainer)

O projeto roda sob um **Dev Container** para padronizar o ambiente entre máquinas e CI. Configuração em `.devcontainer/`:

- **Imagem base:** imagem customizada (`robsondouglas/dev-lambda-node`) com Python e Node já alinhados ao runtime Lambda (Linux x86_64), dispensando a instalação dessas ferramentas via features.
- **`postCreateCommand`** (`.devcontainer/post-create.sh`): atualiza o pip, instala as dependências Python de desenvolvimento a partir do `requirements-dev.txt` e valida as versões das ferramentas (Python, Node, Serverless, AWS CLI).
- **Credenciais:** `AWS_REGION` é definido via `remoteEnv` (`sa-east-1`).

> Empacotamento sem Docker: como a imagem do devcontainer já é Linux x86_64 compatível com o runtime Lambda, dispensou-se o docker-in-docker. O `serverless-python-requirements` usa `dockerizePip: false` e o pip baixa os wheels nativos corretos. Caso no futuro o build precise rodar em arquitetura divergente (ex.: ARM) ou compilar libs sem wheel disponível, reavaliar o uso de Docker ou de imagem de container para a Lambda.

## Modelos de dados

### Registro de entrada (JSON)

```json
{
  "id": "a1b2c3",
  "nome": "Maria Silva",
  "nascimento": 631152000,
  "sexo": "F"
}
```

### Regras de validação (Lambda de Ingestão)

| Campo        | Tipo     | Regra                                                    |
|--------------|----------|----------------------------------------------------------|
| `id`         | string   | obrigatório, não vazio                                   |
| `nome`       | string   | obrigatório, não vazio                                   |
| `nascimento` | integer  | obrigatório, inteiro (unixtime em segundos)              |
| `sexo`       | string   | obrigatório, valor em {`"M"`, `"F"`}                     |

### Schema Parquet (saída)

| Coluna       | Tipo Parquet / Arrow |
|--------------|----------------------|
| `id`         | string               |
| `nome`       | string               |
| `nascimento` | int64                |
| `sexo`       | string               |

> Observação: `sexo` é mantido como coluna no arquivo além de ser usado na chave de partição, para que os dados permaneçam autocontidos.

## Consumo e visualização dos dados

A camada de consumo expõe os dados por meio de uma página HTML, sem jamais tornar nenhum bucket S3 público. O CloudFront atua como ponto de entrada único, com dois behaviors de roteamento:

```
                 ┌─ /, /index.html, /app.js ──► S3 site (privado, via OAC)   [estático]
Navegador ─► CloudFront ─┤
                 └─ /api/* ──────────────────► API Gateway ─► Lambda Consulta ─► S3 dados (IAM role)
                                                                                  [retorna JSON]
```

### Componentes

- **Bucket do site (estático):** armazena `index.html`, JS e CSS. Mantido privado (Block Public Access habilitado) e acessível apenas pelo CloudFront via **Origin Access Control (OAC)**; uma bucket policy autoriza somente a distribuição CloudFront a ler os objetos.
- **CloudFront:**
  - Behavior padrão (`/*` estático) → origem bucket do site (OAC).
  - Behavior `/api/*` → origem API Gateway (Lambda de consulta).
  - HTTPS/TLS gerenciado; opção de anexar WAF.
  - Origem única elimina a necessidade de CORS.
- **Lambda de Consulta:**
  - Lê os arquivos Parquet do **bucket de dados** (que permanece privado, acessado somente pela IAM role da Lambda — o navegador nunca o acessa).
  - Usa **DuckDB** (ou Athena, como alternativa gerenciada) para consultar os Parquet no S3 com pushdown de partição (`WHERE sexo = ...`).
  - Aplica **paginação** (limit/offset ou token) para respeitar os limites de payload (Lambda 6 MB síncrono, API Gateway 10 MB) e não devolver o dataset inteiro.
  - Retorna **JSON** para o navegador — nunca Parquet cru.
- **Bucket de dados:** permanece privado. Não é exposto via CloudFront nem tornado público. A leitura ocorre exclusivamente pela IAM role da Lambda de consulta (`s3:GetObject` restrito ao bucket).

### Decisão: por que não servir os Parquet diretamente via CloudFront

Servir os arquivos `.parquet` diretamente ao navegador via CloudFront+OAC foi descartado porque (1) o navegador não lê Parquet nativamente e (2) o CloudFront entrega o objeto inteiro, sem filtro por partição, o que exporia todos os dados (incluindo PII como `nome` e `nascimento`) ao cliente e seria ineficiente. A leitura dos dados permanece intermediada pela Lambda, que filtra, pagina e devolve apenas o necessário.

### Tratamento de PII

Os campos `nome` e `nascimento` são dados pessoais. A Lambda de consulta deve retornar apenas os campos necessários à visualização e pode mascarar/omitir PII conforme a necessidade da tela. Caso a página seja pública, recomenda-se proteger o endpoint `/api/*` (API key, Cognito ou restrição via WAF).

## Tratamento de erros

| Ponto de falha                    | Estratégia                                                               |
|-----------------------------------|--------------------------------------------------------------------------|
| JSON inválido na ingestão         | Retorna 400 ao cliente; não publica no Kinesis.                          |
| Falha de validação de campo       | Retorna 400 com o campo que falhou; não publica no Kinesis.              |
| Falha no `put_record` do Kinesis  | Retorna 502; cliente pode reenviar. Erro logado no CloudWatch.           |
| Registro inválido no lote Kinesis | Lambda de agregação ignora/loga o record individual e continua o lote.   |
| Falha ao gravar Parquet no S3     | Lança exceção → retry do event source mapping → DLQ após limite.         |
| Record "venenoso" repetido        | `BisectBatchOnFunctionError` isola o record problemático em retries.     |

## Estratégia de testes

- **Testes unitários da Lambda de Ingestão:** validação de schema (casos válidos e cada caso de erro por campo), construção do record do Kinesis, mapeamento de respostas HTTP.
- **Testes unitários da Lambda de Agregação:** decodificação de records, agrupamento por sexo, geração do schema Parquet, construção da chave S3 (partição + unicidade).
- **Testes de integração (ambiente local/mocks):** uso de `moto` ou LocalStack para simular Kinesis e S3; verificar que registros publicados resultam em objetos Parquet nas partições corretas.
- **Testes end-to-end (ambiente de dev AWS):** envio de requisições HTTPS reais e verificação da presença dos arquivos Parquet no S3; validação de leitura via Athena/pandas.
- **Teste de janela de tempo:** enviar menos de 30 registros e confirmar que a agregação dispara em ~20s.
