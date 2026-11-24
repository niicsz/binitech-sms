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

1. Runner de consumidor Kafka com commit manual, retry topics e DLQ.
2. `router-worker`, `route-adapter`, `status-engine`, `webhook-dispatcher` e `ledger-consumer`, um binário por vez em `cmd/`.

## Testes

```sh
make test     # go test -race ./...
make lint     # go vet + golangci-lint
```

Os testes de integração sobem um PostgreSQL 17 embutido (baixado na primeira execução) e um Kafka falso do franz-go, então rodam sem Docker. Com `TEST_DATABASE_URL` definida, usam esse banco no lugar do embutido.
