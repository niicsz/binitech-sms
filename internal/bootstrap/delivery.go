package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/niicsz/binitech-sms/internal/adapter/out/carrier/gsm"
	"github.com/niicsz/binitech-sms/internal/adapter/out/carrier/httproute"
	"github.com/niicsz/binitech-sms/internal/adapter/out/carrier/sandbox"
	"github.com/niicsz/binitech-sms/internal/adapter/out/carrier/smpp"
	kafkaadapter "github.com/niicsz/binitech-sms/internal/adapter/out/kafka"
	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/adapter/out/system"
	"github.com/niicsz/binitech-sms/internal/adapter/out/throttle"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/delivery"
	"github.com/niicsz/binitech-sms/internal/application/usecase/eventing"
	"github.com/niicsz/binitech-sms/internal/application/usecase/ledger"
	"github.com/niicsz/binitech-sms/internal/application/usecase/routing"
	"github.com/niicsz/binitech-sms/internal/application/usecase/status"
	"github.com/niicsz/binitech-sms/internal/application/usecase/webhook"
	"github.com/niicsz/binitech-sms/internal/domain/route"
)

func Emitter(pool *pgxpool.Pool, client *kgo.Client) *eventing.Emitter {
	clock := system.Clock{}
	var outbox out.Outbox
	if pool != nil {
		outbox = postgres.NewOutboxRepository(pool)
	}
	return eventing.NewEmitter(outbox, kafkaadapter.NewPublisher(client), clock, system.NewIDs(clock))
}

func Router(pool *pgxpool.Pool, client *kgo.Client, catalog *postgres.RouteCatalog) *routing.Router {
	return routing.New(postgres.NewTxManager(pool), postgres.NewProcessedOps(pool), postgres.NewDelivery(pool), catalog,
		Emitter(pool, client), rand.IntN)
}

func LedgerConsumer(pool *pgxpool.Pool) *ledger.Consumer {
	return ledger.New(postgres.NewTxManager(pool), postgres.NewProcessedOps(pool), postgres.NewBalances(pool))
}

func WebhookDispatcher(pool *pgxpool.Pool, client *kgo.Client, box out.SecretBox, sender out.WebhookSender) *webhook.Dispatcher {
	return webhook.NewDispatcher(postgres.NewWebhooks(pool), box, sender, Emitter(pool, client))
}

func StatusEngine(pool *pgxpool.Pool, client *kgo.Client) *status.Engine {
	return status.New(postgres.NewTxManager(pool), postgres.NewProcessedOps(pool), postgres.NewDelivery(pool), Emitter(pool, client))
}

type RouteSettings struct {
	Kind   string
	Config json.RawMessage
	TPS    float64
}

func LoadRoute(ctx context.Context, pool *pgxpool.Pool, id string) (RouteSettings, error) {
	if id == route.SandboxID {
		return RouteSettings{Kind: "sandbox", Config: json.RawMessage(`{}`)}, nil
	}
	var (
		s   RouteSettings
		cfg []byte
	)
	if err := pool.QueryRow(ctx, `SELECT kind, config FROM routes WHERE id = $1`, id).Scan(&s.Kind, &cfg); err != nil {
		return s, fmt.Errorf("rota %s: %w", id, err)
	}
	s.Config = cfg
	var extra struct {
		TPS float64 `json:"tps"`
	}
	_ = json.Unmarshal(cfg, &extra)
	s.TPS = extra.TPS
	return s, nil
}

type BuiltCarrier struct {
	Carrier  out.Carrier
	Callback *httproute.Carrier
	Throttle delivery.Throttle
}

func BuildCarrier(id string, settings RouteSettings, env func(string) string, sandboxDelay time.Duration) (BuiltCarrier, error) {
	built := BuiltCarrier{Throttle: throttle.New(settings.TPS, max(int(settings.TPS), 1))}
	switch settings.Kind {
	case "sandbox":
		built.Carrier = sandbox.New(sandboxDelay)
	case "http":
		c, err := httproute.New(id, settings.Config, env)
		if err != nil {
			return built, err
		}
		built.Carrier, built.Callback = c, c
	case "smpp":
		c, err := smpp.New(id, settings.Config, env)
		if err != nil {
			return built, err
		}
		c.Start()
		built.Carrier = c
	case "gsm":
		c, err := gsm.New(id, settings.Config, env)
		if err != nil {
			return built, err
		}
		built.Carrier = c
	default:
		return built, fmt.Errorf("rota %s: tipo %q ainda não suportado", id, settings.Kind)
	}
	return built, nil
}
