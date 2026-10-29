# Requisitos

## Introdução

Este documento especifica os requisitos de um sistema serverless de ingestão de eventos na AWS. O sistema recebe registros de pessoas via requisição HTTPS, publica esses registros em um Kinesis Data Stream e, em seguida, agrega os registros em lotes e os grava em arquivos Parquet em um bucket S3 particionado por sexo.

O fluxo de alto nível é:

```
Cliente --HTTPS--> API Gateway --> Lambda de Ingestão --> Kinesis Data Stream
                                                               |
                                                               v
                                          Lambda de Agregação (lote de 30 ou 20s)
                                                               |
                                                               v
                                          S3 (Parquet, partição sexo=M / sexo=F)
```

### Formato do registro de entrada

```json
{
  "id": "string",
  "nome": "string",
  "nascimento": 631152000,
  "sexo": "M"
}
```

- `id`: identificador único do registro (string, obrigatório, não vazio).
- `nome`: nome da pessoa (string, obrigatório, não vazio).
- `nascimento`: data de nascimento em unixtime/epoch em segundos (number inteiro, obrigatório).
- `sexo`: enum com valores permitidos `"M"` ou `"F"` (string, obrigatório).

## Requisitos

### Requisito 1 — Recepção de requisição HTTPS

**História de usuário:** Como sistema cliente, quero enviar um registro de pessoa via requisição HTTPS, para que o dado seja aceito e encaminhado para processamento.

#### Critérios de aceitação

1. QUANDO uma requisição HTTPS POST é recebida com um corpo JSON válido O SISTEMA DEVE aceitar a requisição e retornar status HTTP 202 (Accepted).
2. QUANDO a requisição é recebida via protocolo diferente de HTTPS O SISTEMA DEVE rejeitar a requisição.
3. SE a requisição não contiver corpo ENTÃO O SISTEMA DEVE retornar status HTTP 400 com uma mensagem de erro descritiva.
4. QUANDO a requisição é aceita O SISTEMA DEVE responder sem aguardar a conclusão do processamento downstream (processamento assíncrono).

### Requisito 2 — Validação de schema do registro

**História de usuário:** Como operador do sistema, quero que todo registro recebido seja validado contra o schema esperado, para que dados malformados não entrem no pipeline.

#### Critérios de aceitação

1. QUANDO um registro é recebido O SISTEMA DEVE validar a presença dos campos `id`, `nome`, `nascimento` e `sexo`.
2. SE o campo `id` estiver ausente, vazio ou não for string ENTÃO O SISTEMA DEVE rejeitar o registro com status HTTP 400.
3. SE o campo `nome` estiver ausente, vazio ou não for string ENTÃO O SISTEMA DEVE rejeitar o registro com status HTTP 400.
4. SE o campo `nascimento` estiver ausente ou não for um número inteiro (unixtime) ENTÃO O SISTEMA DEVE rejeitar o registro com status HTTP 400.
5. SE o campo `sexo` estiver ausente ou tiver valor diferente de `"M"` ou `"F"` ENTÃO O SISTEMA DEVE rejeitar o registro com status HTTP 400.
6. QUANDO um registro é rejeitado por validação O SISTEMA DEVE retornar uma mensagem de erro indicando qual campo falhou.
7. QUANDO um registro é rejeitado por validação O SISTEMA NÃO DEVE publicá-lo no Kinesis Data Stream.

### Requisito 3 — Publicação no Kinesis Data Stream

**História de usuário:** Como sistema, quero publicar cada registro válido em um Kinesis Data Stream, para que os registros sejam processados de forma desacoplada e escalável.

#### Critérios de aceitação

1. QUANDO um registro válido é recebido O SISTEMA DEVE publicá-lo como um record no Kinesis Data Stream.
2. QUANDO um registro é publicado O SISTEMA DEVE usar o campo `sexo` como partition key do record, de forma que registros da mesma categoria compartilhem a mesma partition key.
3. SE a publicação no Kinesis falhar ENTÃO O SISTEMA DEVE retornar status HTTP 5xx ao cliente e registrar o erro em log.
4. O SISTEMA DEVE publicar o registro preservando os campos originais `id`, `nome`, `nascimento` e `sexo`.

### Requisito 4 — Agregação em lotes

**História de usuário:** Como operador do sistema, quero que os registros sejam agregados em lotes antes da gravação, para que os arquivos no S3 tenham tamanho adequado e o número de objetos seja controlado.

#### Critérios de aceitação

1. QUANDO 30 registros estiverem acumulados no stream O SISTEMA DEVE disparar a agregação desses registros.
2. QUANDO 20 segundos se passarem desde o primeiro registro acumulado e houver menos de 30 registros O SISTEMA DEVE disparar a agregação dos registros disponíveis.
3. O SISTEMA DEVE aplicar o mesmo limite de 30 registros independentemente do valor de `sexo`.
4. QUANDO a agregação é disparada O SISTEMA DEVE separar os registros do lote por valor de `sexo`.

### Requisito 5 — Gravação em S3 particionado (Parquet)

**História de usuário:** Como consumidor de dados analíticos, quero que os registros sejam gravados em arquivos Parquet particionados por sexo em um único bucket, para que eu possa consultar os dados de forma eficiente.

#### Critérios de aceitação

1. QUANDO um lote é processado O SISTEMA DEVE gravar os registros em formato Apache Parquet.
2. O SISTEMA DEVE gravar todos os arquivos em um único bucket S3.
3. QUANDO registros com `sexo = "M"` são gravados O SISTEMA DEVE armazená-los sob o prefixo de partição `sexo=M/`.
4. QUANDO registros com `sexo = "F"` são gravados O SISTEMA DEVE armazená-los sob o prefixo de partição `sexo=F/`.
5. QUANDO um lote contém registros de ambos os sexos O SISTEMA DEVE gerar arquivos Parquet separados por partição.
6. QUANDO um arquivo Parquet é gravado O SISTEMA DEVE usar um nome de objeto único para evitar sobrescrita (ex.: timestamp + identificador único).
7. O SISTEMA DEVE preservar os campos `id`, `nome`, `nascimento` e `sexo` no schema do arquivo Parquet.

### Requisito 6 — Observabilidade e tratamento de erros

**História de usuário:** Como operador do sistema, quero logs e tratamento de falhas, para que eu possa diagnosticar e recuperar erros no pipeline.

#### Critérios de aceitação

1. O SISTEMA DEVE registrar logs de execução de ambas as Lambdas no CloudWatch Logs.
2. SE a Lambda de agregação falhar ao processar um lote ENTÃO O SISTEMA DEVE permitir o reprocessamento do lote (retry) conforme a política do event source mapping.
3. QUANDO o número máximo de tentativas de reprocessamento for excedido O SISTEMA DEVE enviar os registros com falha para uma fila/destino de dead-letter.
4. O SISTEMA DEVE emitir métricas de erro que possam ser monitoradas via CloudWatch.

### Requisito 7 — Consumo e visualização dos dados

**História de usuário:** Como usuário analista, quero visualizar os dados gravados no S3 através de uma página web, para que eu possa consultá-los sem acessar diretamente a infraestrutura.

#### Critérios de aceitação

1. O SISTEMA DEVE disponibilizar uma página HTML servida via CloudFront.
2. O SISTEMA DEVE manter o bucket do site estático privado, acessível apenas pelo CloudFront via Origin Access Control (OAC).
3. O SISTEMA DEVE manter o bucket de dados (Parquet) privado, NÃO exposto publicamente nem servido diretamente via CloudFront.
4. QUANDO a página solicita dados O SISTEMA DEVE roteá-la via CloudFront (`/api/*`) para uma Lambda de consulta que lê o S3 e retorna JSON.
5. QUANDO uma consulta filtra por `sexo` O SISTEMA DEVE aproveitar o particionamento para ler apenas a partição correspondente.
6. O SISTEMA DEVE paginar ou limitar os resultados para respeitar os limites de payload de Lambda e API Gateway.
7. O SISTEMA NÃO DEVE retornar ao navegador mais campos de PII (`nome`, `nascimento`) do que os necessários para a visualização.
