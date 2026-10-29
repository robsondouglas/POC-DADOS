# POC-DADOS — Pipeline serverless de ingestão de eventos

Pipeline na AWS que recebe registros de pessoas via HTTPS, publica em um Kinesis
Data Stream, agrega em lotes e grava arquivos Parquet particionados por sexo em
um bucket S3. Inclui uma camada de visualização (página HTML servida via
CloudFront + Lambda de consulta).

## Arquitetura

```
Cliente --HTTPS--> API Gateway --> Lambda Ingestão --> Kinesis Data Stream
                                                             |
                                        (BatchSize=30 / janela 20s)
                                                             v
                                          Lambda Agregação --> S3 (Parquet: sexo=M/ | sexo=F/)
                                                             ^
Navegador --> CloudFront --/api/*--> Lambda Consulta --------/  (lê via DuckDB, retorna JSON)
          \--/ (site estático S3 via OAC)
```

Especificações completas em `.kiro/specs/ingestao-eventos-kinesis/`
(`requirements.md`, `design.md`, `tasks.md`).

## Estrutura

```
src/
  ingestao/    handler HTTP + validação de schema -> Kinesis
  agregacao/   consome Kinesis, agrupa por sexo, grava Parquet no S3
  consulta/    consulta Parquet (DuckDB) e retorna JSON
site/          página HTML de visualização
tests/         testes unitários e de integração (pytest + moto)
serverless.yml infraestrutura (Serverless Framework)
```

## Ambiente de desenvolvimento

O projeto roda sob Dev Container (`.devcontainer/`) com Python 3.12, Node,
docker-in-docker e AWS CLI. Abra o projeto no VS Code e "Reopen in Container".

## Testes

```bash
pip install -r requirements-dev.txt
pytest
```

## Deploy

Requer credenciais AWS configuradas. As dependências Python (DuckDB) são
empacotadas sem Docker (wheels nativos do devcontainer Linux x86_64). Região
padrão: `sa-east-1`.

```bash
npm install
npx serverless deploy --stage dev
```

Após o deploy:
- O endpoint de ingestão é exibido como saída do HTTP API (`POST /eventos`).
- O domínio do CloudFront é exibido em `CloudFrontDomain`.
- Suba o `site/index.html` no bucket do site (saída `SiteBucketName`).

## Formato do registro

```json
{ "id": "a1", "nome": "Maria", "nascimento": 631152000, "sexo": "F" }
```

`nascimento` é unixtime (epoch em segundos); `sexo` é `M` ou `F`.
