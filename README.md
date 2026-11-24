# BiniTech SMS

Plataforma própria de SMS da BiniTech: API pública em Go, Apache Kafka como espinha dorsal do fluxo assíncrono e PostgreSQL como fonte da verdade. O planejamento completo está no documento "Plataforma BiniTech SMS" (BTPDV-220).

## Arquitetura hexagonal

```
cmd/                      binários: só montam as peças (composition root)
internal/
  domain/                 regras puras: segmentação GSM-7/UCS-2, dinheiro, telefone, estados da mensagem
  contract/events/        eventos Kafka, tópicos e headers (contrato entre serviços)
  application/
    port/in/              o que a aplicação oferece (casos de uso)
    port/out/             o que a aplicação precisa (banco, Kafka, relógio, ids)
    usecase/              implementação dos casos de uso, só com portas
  adapter/
    in/http/              chi: rotas, envelope de resposta, métricas
    out/postgres/         pgx: transação, outbox, health
    out/kafka/            franz-go: publicação, health
    out/system/           relógio e geração de ids
  platform/               configuração, logs, conexões e servidor HTTP
migrations/               SQL versionado (golang-migrate)
deploy/local/             Docker Compose: Kafka KRaft, Postgres, Redis, kafka-ui, Prometheus, Grafana
```

Regras de dependência, verificadas em dois lugares:

| Camada | Pode depender de | Não pode depender de |
| --- | --- | --- |
| `domain` | biblioteca padrão | `application`, `adapter`, `platform`, `contract`, bibliotecas de infraestrutura |
| `contract` | `domain` | `application`, `adapter`, `platform` |
| `application` | `domain`, `contract` | `adapter`, `platform`, `net/http`, pgx, franz-go |
| `adapter/in` | `application/port/in` | `adapter/out`, `platform` |
| `adapter/out` | `application/port/out`, infraestrutura | `adapter/in`, `application/usecase` |

- `internal/architecture_test.go` falha o `go test` se alguma regra for quebrada.
- O `depguard` do golangci-lint aplica as mesmas regras no editor e no CI.

## Rodar local

Requisitos: Go 1.26+, Docker com Compose.

```sh
cp .env.example .env        # troque as senhas
make up                     # Kafka, Postgres, Redis, kafka-ui, Prometheus, Grafana
make topics                 # cria os tópicos (idempotente)
make migrate                # aplica as migrações
make run-api                # core-api em :8080
make run-relay              # outbox-relay, métricas em :9101
```

| Serviço | Endereço |
| --- | --- |
| core-api | http://localhost:8080 (`/healthz`, `/readyz`, `/metrics`) |
| kafka-ui | http://localhost:8088 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (usuário `admin`, senha `GRAFANA_PASSWORD`) |
| Kafka (do host) | `127.0.0.1:29092` |
| Postgres | `127.0.0.1:5433` |

`make reset` derruba tudo e apaga os volumes.

## Tópicos

`deploy/local/kafka/create-topics.sh` cria os tópicos do planejamento com partições, retenção e os retry topics (`.retry.5m`, `.retry.1h`) de cada um. Em produção, rode o mesmo script com `REPLICATION=3`, `MIN_ISR=2` e `ROUTES` com as rotas cadastradas.

## O que já existe

- Domínio com testes: segmentação GSM-7/UCS-2 (153/67 por parte, sem partir extensão nem emoji), dinheiro em 4 casas, telefone E.164, chave de API e a máquina de estados da mensagem.
- `core-api`:
  - `POST /v1/message/send`, `GET /v1/message/{id}` e `GET /v1/account/balance`, gerados do `api/openapi.yaml` (oapi-codegen, modo strict);
  - chave em `X-Api-Key` ou `Authorization: Bearer`, hash scrypt e cache de 5 minutos; chave na URL é recusada;
  - limite por conta com GCRA no Redis, cabeçalhos `RateLimit-*` e `Retry-After`;
  - `Idempotency-Key` gravada na mesma transação da mensagem, antes do Kafka;
  - reserva de saldo, lançamento `reserve` no ledger, mensagem e outbox numa transação, e publicação imediata com o relay como garantia;
  - chave `sk_test_` não cobra e marca o evento como teste.
- `outbox-relay`: lê o outbox com `FOR UPDATE SKIP LOCKED`, publica com `acks=all` e só marca depois da confirmação do broker.
- `smsctl`: cria conta, emite e revoga chave, credita saldo, cadastra tarifa e aprova remetente.
- `router-worker`: consome `sms.submit`, escolhe a rota (LCR por prioridade, custo e peso, só rotas saudáveis, recarga por `LISTEN routes_changed`) e publica em `sms.dispatch.<rota>`. Chave de teste vai sempre para a rota `sandbox`.
- `route-adapter` (`ROUTE_ID=...`): envia pela rota com limite de vazão (`tps`), publica o resultado e os DLRs em `sms.status.dlr` e grava a saúde da rota. Rotas disponíveis:
  - `sandbox`: simula entrega; destino terminado em `0000` falha e em `9999` é recusado;
  - `http`: atacadista genérico configurado no JSON da rota (URL, header de autenticação lido de variável de ambiente, corpo com `{{to}}`, `{{to_digits}}`, `{{from}}`, `{{text}}`, `{{message_id}}`, campo do id na resposta e callback de DLR em `CALLBACK_ADDR` com `X-Callback-Token`).
  - `smpp`: SMPP 3.4 por transceiver (`addr`, `system_id`, senha em `password_env`, TLS opcional, TON/NPI por tipo de remetente, `dlr_id_format` para operadoras que mandam o id do recibo em outra base). Mensagem longa vai com UDH; throttling (0x58), fila cheia e erro de sistema voltam pelo retry, os demais status viram recusa `smpp_0xNN`.
  - `gsm`: modem por comandos AT em modo texto (`device`, `baud`, `pin_env`), só uma parte por mensagem, com relatório de entrega por `+CDS`. Use apenas com linha corporativa autorizada para esse tráfego.
- `status-engine`: aplica aceite e recusa da operadora, DLR por parte (multipart) e expiração; publica `webhook.outbound` e `ledger.events` (débito no aceite, liberação na recusa ou expiração).
- `ledger-consumer`: efetiva `debit` (sai do disponível e da reserva) e `release` (volta só a reserva) com `op_id` único no ledger; reserva menor que o lançamento vai para a DLQ.
- `webhook-dispatcher`: transforma cada mudança de status em uma entrega por webhook inscrito (`message.submitted`, `message.delivered`, `message.failed`), assina com `X-BiniTech-Signature: t=<unix>,v1=<HMAC-SHA256 de "t.corpo">`, não segue redirecionamento, recusa endereços internos (SSRF) e desativa o webhook depois de 50 falhas seguidas.
- `GET/POST /v1/webhook` e `GET/PUT/DELETE /v1/webhook/{id}`: o segredo `whsec_...` aparece só na criação e fica cifrado (AES-GCM com `WEBHOOK_SECRET_KEY`) no banco.
- `POST /v1/message/sendBulk`: até 10.000 destinos por lote, com texto do lote ou de cada item; destinos inválidos voltam em `rejected` com o índice e não barram o resto; uma reserva para o total e uma transação para tudo.
- `GET /v1/message/list`: mais novas primeiro, filtros de período, status e campanha, cursor estável (`created_at`, `id`).
- `POST /v1/campaign/create` e `GET /v1/campaign/{id}/summary`: até 100.000 destinatários, variáveis `{{chave}}` por destinatário, janela de horário com fuso; o `campaign-scheduler` envia em blocos de 1.000 com idempotência por bloco, pausa a campanha quando falta saldo e só um scheduler trabalha por vez (advisory lock).
- `GET /v1/alphaname/list`: remetentes da conta e situação.
- `POST /v1/otp/send` e `POST /v1/otp/verify`: código de 4 a 8 dígitos guardado só como HMAC (`OTP_HMAC_KEY`), validade de 1 a 30 minutos, até 10 tentativas, no máximo 5 códigos por hora e 30 s entre reenvios para o mesmo destino; a conferência devolve `verified`, `invalid`, `expired`, `blocked` ou `used`. Com chave `sk_test_` a resposta traz `test_code`.
- `POST /v1/link/shorten` e `link-redirector`: código de 7 caracteres em `LINK_BASE_URL`, só http/https, redirecionamento 302 sem cache e contagem de cliques (IP guardado só como hash).
- Opt-out: resposta `SAIR`, `PARAR`, `STOP`, `CANCELAR`, `DESCADASTRAR` ou `REMOVER` recebida por SMPP bloqueia o número para as contas que usam aquele remetente; o envio para ele passa a ser recusado com 403.

### Primeiro envio local

```sh
export DATABASE_URL=...            # o mesmo do .env
go run ./cmd/smsctl account create -name "Minha loja"
go run ./cmd/smsctl key issue -account acc_... -env test
go run ./cmd/smsctl tariff set -plan default -mcc 724 -price 0.0800
go run ./cmd/smsctl sender approve -account acc_... -sender BINITECH -kind shortcode -countries BR
go run ./cmd/smsctl balance credit -account acc_... -amount 10

curl -s localhost:8080/v1/message/send -H "X-Api-Key: sk_test_..." -H "Idempotency-Key: teste-1" -H "Content-Type: application/json" -d '{"to":"+5511987654321","from":"BINITECH","text":"Seu codigo e 482913"}'
```

O contrato fica em `api/openapi.yaml`. Depois de alterá-lo, rode `make generate`; o CI falha se o código gerado estiver desatualizado.

## Destinos atendidos

Por enquanto só celulares brasileiros (MCC 724). A operadora (MNC) fica vazia até existir a consulta à base de portabilidade, e a tarifa usa o preço do país.

## Próximos passos

1. SDK oficial em Java (`com.binitech:sms-java`).

## Testes

```sh
make test     # go test -race ./...
make lint     # go vet + golangci-lint
```

Os testes de integração sobem um PostgreSQL 17 embutido (baixado na primeira execução) e um Kafka falso do franz-go, então rodam sem Docker. Com `TEST_DATABASE_URL` definida, usam esse banco no lugar do embutido.
