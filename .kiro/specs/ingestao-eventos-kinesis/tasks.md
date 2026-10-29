# Plano de Implementação

- [x] 1. Configurar a estrutura base do projeto e IaC (Serverless Framework)
  - Criar a estrutura de diretórios (ex.: `src/ingestao/`, `src/agregacao/`, `tests/`) e o `serverless.yml` na raiz.
  - Configurar o Dev Container (`.devcontainer/devcontainer.json` + `post-create.sh`) com imagem Linux x86_64 compatível com Lambda (Python + Node) e AWS CLI, sem docker-in-docker. (concluído)
  - Inicializar o Serverless Framework (`serverless.yml` com provider aws, runtime `python3.12`, region `sa-east-1`).
  - Adicionar o `serverless-python-requirements` (nativo no v4) com `dockerizePip: false`, `useUv: false` e `fileName: requirements.txt` para empacotar o `duckdb` no layer.
  - _Requisitos: todos (fundação)_

- [x] 2. Definir os recursos de infraestrutura
- [x] 2.1 Declarar o Kinesis Data Stream
  - No bloco `resources` do `serverless.yml`, criar o stream `eventos-pessoas-stream` em modo on-demand com retenção de 24h.
  - _Requisitos: 3.1_

- [x] 2.2 Declarar o bucket S3
  - No bloco `resources`, criar o bucket com nome único, SSE-S3, versionamento e bloqueio de acesso público.
  - _Requisitos: 5.2_

- [x] 2.3 Declarar a DLQ (SQS) e papéis/políticas IAM
  - No bloco `resources`, criar a fila SQS de dead-letter.
  - Definir as policies mínimas por função via `iam.role.statements` (PutRecord no Kinesis; PutObject no S3; GetRecords no stream; SendMessage na DLQ).
  - _Requisitos: 6.2, 6.3_

- [x] 3. Implementar a Lambda de Ingestão
- [x] 3.1 Implementar validação de schema
  - Escrever função de validação dos campos `id`, `nome`, `nascimento` (inteiro unixtime) e `sexo` (`M`/`F`).
  - Retornar erro estruturado indicando o campo que falhou.
  - _Requisitos: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [x] 3.2 Implementar o handler HTTP e publicação no Kinesis
  - Fazer parse do corpo da requisição do API Gateway (payload v2).
  - Em sucesso, publicar no Kinesis com `sexo` como partition key.
  - Mapear respostas: 202 (aceito), 400 (validação/corpo), 502 (falha de publicação).
  - Garantir que registros inválidos não sejam publicados.
  - _Requisitos: 1.1, 1.3, 1.4, 2.7, 3.1, 3.2, 3.3, 3.4_

- [x] 3.3 Escrever testes unitários da Lambda de Ingestão
  - Cobrir casos válidos e um caso de erro para cada campo.
  - Cobrir falha de publicação no Kinesis (mock) e mapeamento de status.
  - _Requisitos: 2.1–2.7, 3.1–3.4_

- [x] 4. Configurar o API Gateway (evento httpApi)
  - Declarar o evento `httpApi` `POST /eventos` na função de ingestão (o Serverless cria o HTTP API).
  - Confirmar que apenas HTTPS é exposto.
  - _Requisitos: 1.1, 1.2_

- [x] 5. Implementar a Lambda de Agregação
- [x] 5.1 Preparar a dependência duckdb (Lambda Layer)
  - Empacotar `duckdb` via `serverless-python-requirements` (nativo). DuckDB atende tanto a escrita (agregação) quanto a leitura (consulta) de Parquet, mantendo o layer pequeno.
  - _Requisitos: 5.1_

- [x] 5.2 Implementar decodificação e agrupamento por sexo
  - Decodificar os records do Kinesis (base64 → JSON).
  - Agrupar o lote por `sexo`, ignorando e logando records individualmente inválidos.
  - _Requisitos: 4.4_

- [x] 5.3 Implementar geração e gravação de Parquet no S3
  - Serializar cada grupo em Parquet com DuckDB (arquivo local em `/tmp` + upload via boto3), com o schema (`id`, `nome`, `nascimento` BIGINT, `sexo`).
  - Gravar em `sexo=M/` e `sexo=F/` com nome de objeto único (timestamp + uuid).
  - Gerar arquivos separados quando o lote contém ambos os sexos.
  - _Requisitos: 5.1, 5.3, 5.4, 5.5, 5.6, 5.7_

- [x] 5.4 Escrever testes unitários da Lambda de Agregação
  - Cobrir decodificação, agrupamento, schema Parquet e construção da chave S3.
  - _Requisitos: 4.4, 5.1, 5.3–5.7_

- [x] 6. Configurar o Event Source Mapping do Kinesis → Lambda de Agregação
  - No evento `stream` da função de agregação, definir `batchSize: 30` e `batchWindow: 20`.
  - Configurar `bisectBatchOnFunctionError`, `maximumRetryAttempts` e `destinations.onFailure` apontando para a DLQ.
  - _Requisitos: 4.1, 4.2, 4.3, 6.2, 6.3_

- [~] 7. Implementar observabilidade — FORA DE ESCOPO (decisão do usuário)
  - Métricas/alarmes CloudWatch dispensados para esta POC.
  - Logs das Lambdas continuam disponíveis nativamente no CloudWatch Logs (comportamento padrão do Lambda), sem configuração adicional.
  - _Requisitos: 6.1, 6.4 (parcial: logs nativos sim; métricas/alarmes não)_

- [x] 8. Implementar a camada de consumo e visualização
- [x] 8.1 Implementar a Lambda de Consulta
  - Ler os Parquet do bucket de dados via DuckDB (ou Athena), com filtro por `sexo` (pushdown de partição).
  - Implementar paginação/limite de resultados e retornar JSON.
  - Retornar apenas os campos necessários à visualização (minimizar PII).
  - _Requisitos: 7.4, 7.5, 7.6, 7.7_

- [x] 8.2 Declarar o bucket do site estático e o CloudFront com OAC
  - Criar o bucket do site (privado, Block Public Access) no bloco `resources`.
  - Criar a distribuição CloudFront com OAC e bucket policy autorizando somente o CloudFront.
  - Configurar behavior padrão → bucket do site; behavior `/api/*` → API Gateway da Lambda de consulta.
  - Garantir que o bucket de dados permaneça privado (sem exposição pública).
  - _Requisitos: 7.1, 7.2, 7.3, 7.4_

- [x] 8.3 Criar a página HTML
  - Página que consome `/api/*` e renderiza os dados (tabela/gráfico), com filtro por `sexo` e paginação.
  - _Requisitos: 7.1, 7.5, 7.6_

- [x] 9. Testes de integração e end-to-end
- [x] 9.1 Testes de integração com mocks (moto/LocalStack)
  - Verificar que registros publicados resultam em objetos Parquet nas partições corretas.
  - _Requisitos: 3.1–3.4, 5.1–5.7_

- [ ] 9.2 Teste da janela de tempo de 20s
  - Enviar menos de 30 registros e confirmar o disparo da agregação por timeout.
  - _Requisitos: 4.2_

- [ ] 9.3 Teste end-to-end em ambiente de dev
  - Enviar requisições HTTPS reais e validar os arquivos Parquet no S3 (ex.: leitura via Athena/pandas).
  - Validar o fluxo de consulta: página via CloudFront → Lambda → JSON.
  - _Requisitos: 1.1, 5.1–5.7, 7.1–7.7_
