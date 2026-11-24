COMPOSE := docker compose -f deploy/local/docker-compose.yml --env-file .env
SERVICES := core-api outbox-relay router-worker route-adapter status-engine ledger-consumer webhook-dispatcher campaign-scheduler link-redirector smsctl

.PHONY: up down reset topics migrate generate test lint build run-api run-relay images

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

topics:
	$(COMPOSE) run --rm kafka-topics

migrate:
	$(COMPOSE) run --rm migrate

generate:
	go tool oapi-codegen -config api/oapi-codegen.yaml api/openapi.yaml

test:
	go test -race ./...

lint:
	go vet ./...
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0 run

build:
	@for s in $(SERVICES); do go build -o bin/$$s ./cmd/$$s || exit 1; done

run-api:
	set -a && . ./.env && set +a && go run ./cmd/core-api

run-relay:
	set -a && . ./.env && set +a && go run ./cmd/outbox-relay

images:
	@for s in $(SERVICES); do docker build --build-arg SERVICE=$$s -t binitech-sms/$$s:dev . || exit 1; done
