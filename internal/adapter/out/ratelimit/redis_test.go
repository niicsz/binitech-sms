package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/niicsz/binitech-sms/internal/adapter/out/ratelimit"
)

func TestBurstThenBlocksThenRecovers(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	limiter := ratelimit.NewGCRA(client, 10, 3)
	ratelimit.SetClock(limiter, func() time.Time { return now })
	ctx := context.Background()

	for i := range 3 {
		d, err := limiter.Allow(ctx, "account:a")
		if err != nil || !d.Allowed {
			t.Fatalf("requisição %d da rajada deveria passar: %+v %v", i+1, d, err)
		}
	}
	d, err := limiter.Allow(ctx, "account:a")
	if err != nil || d.Allowed || d.RetryAfter <= 0 || d.RetryAfter > 100*time.Millisecond {
		t.Fatalf("quarta deveria ser barrada com espera de até 100ms: %+v %v", d, err)
	}
	other, _ := limiter.Allow(ctx, "account:b")
	if !other.Allowed {
		t.Fatal("outra conta não pode ser afetada")
	}
	now = now.Add(100 * time.Millisecond)
	if d, _ := limiter.Allow(ctx, "account:a"); !d.Allowed {
		t.Fatalf("depois de 100ms deveria liberar uma: %+v", d)
	}
}
