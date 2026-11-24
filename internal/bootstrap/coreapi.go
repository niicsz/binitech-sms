package bootstrap

import (
	"crypto/rand"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	httpadapter "github.com/niicsz/binitech-sms/internal/adapter/in/http"
	"github.com/niicsz/binitech-sms/internal/adapter/out/destination"
	kafkaadapter "github.com/niicsz/binitech-sms/internal/adapter/out/kafka"
	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/adapter/out/ratelimit"
	"github.com/niicsz/binitech-sms/internal/adapter/out/secret"
	"github.com/niicsz/binitech-sms/internal/adapter/out/system"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/auth"
	"github.com/niicsz/binitech-sms/internal/application/usecase/eventing"
	"github.com/niicsz/binitech-sms/internal/application/usecase/query"
	"github.com/niicsz/binitech-sms/internal/application/usecase/readiness"
	"github.com/niicsz/binitech-sms/internal/application/usecase/send"
	"github.com/niicsz/binitech-sms/internal/application/usecase/webhook"
)

type CoreAPI struct {
	Log        *slog.Logger
	Env        string
	Pool       *pgxpool.Pool
	Kafka      *kgo.Client
	Redis      *redis.Client
	Registry   *prometheus.Registry
	Clock      out.Clock
	Hasher     out.SecretHasher
	RatePerSec int
	RateBurst  int
	KeyCache   time.Duration
	SecretBox  out.SecretBox
}

func (c CoreAPI) Handler() http.Handler {
	clock := c.Clock
	if clock == nil {
		clock = system.Clock{}
	}
	hasher := c.Hasher
	if hasher == nil {
		hasher = secret.DefaultScrypt()
	}
	if c.KeyCache == 0 {
		c.KeyCache = 5 * time.Minute
	}
	accounts := postgres.NewAccounts(c.Pool)
	balances := postgres.NewBalances(c.Pool)
	catalog := postgres.NewCatalog(c.Pool)
	messages := postgres.NewMessages(c.Pool)
	checkers := []out.HealthChecker{postgres.NewHealthChecker(c.Pool), kafkaadapter.NewHealthChecker(c.Kafka)}

	var limiter in.RateLimiter
	if c.Redis != nil {
		limiter = auth.NewRateLimiter(ratelimit.NewGCRA(c.Redis, c.RatePerSec, c.RateBurst))
	}
	sender := send.New(send.Deps{
		Tx:           postgres.NewTxManager(c.Pool),
		Accounts:     accounts,
		Balances:     balances,
		Ledger:       balances,
		Destinations: destination.Brazil{},
		Tariffs:      catalog,
		Senders:      catalog,
		Messages:     messages,
		Idempotency:  postgres.NewIdempotency(c.Pool),
		Events:       eventing.NewEmitter(postgres.NewOutboxRepository(c.Pool), kafkaadapter.NewPublisher(c.Kafka), clock, system.NewIDs(clock)),
		Clock:        clock,
		IDs:          system.NewIDs(clock),
		Env:          c.Env,
	})
	return httpadapter.NewRouter(httpadapter.Deps{
		Log:           c.Log,
		Readiness:     readiness.New(2*time.Second, checkers...),
		Registry:      c.Registry,
		Authenticator: auth.NewAuthenticator(accounts, hasher, clock, c.KeyCache),
		RateLimiter:   limiter,
		Sender:        sender,
		Queries:       query.New(accounts, balances, messages),
		Webhooks:      webhook.NewManager(postgres.NewWebhooks(c.Pool), c.SecretBox, system.NewIDs(clock), clock, rand.Reader),
	})
}
