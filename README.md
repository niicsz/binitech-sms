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

- Domínio com testes: segmentação GSM-7/UCS-2 (153/67 por parte, sem partir extensão nem emoji), dinheiro em 4 casas, telefone E.164 e a máquina de estados da mensagem.
- `core-api`: `/healthz`, `/readyz` (Postgres e Kafka), `/metrics` e envelope `{code, message, data}` nas respostas de `/v1`.
- `outbox-relay`: lê o outbox com `FOR UPDATE SKIP LOCKED`, publica no Kafka com `acks=all` e produtor idempotente, e só marca como publicado depois da confirmação do broker.
- Migração inicial com todas as tabelas do planejamento.

## Próximos passos

1. `POST /v1/message/send`: autenticação, idempotência, reserva de saldo, outbox e publicação.
2. Runner de consumidor Kafka com commit manual, retry topics e DLQ.
3. `router-worker`, `route-adapter`, `status-engine`, `webhook-dispatcher` e `ledger-consumer`, um binário por vez em `cmd/`.

## Testes

```sh
make test     # go test -race ./...
make lint     # go vet + golangci-lint
```
