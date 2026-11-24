package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

var gcra = redis.NewScript(`
local key = KEYS[1]
local emission = tonumber(ARGV[1])
local tolerance = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local tat = tonumber(redis.call('GET', key) or now)
if tat < now then tat = now end
local allow_at = tat - tolerance
if now < allow_at then
  return {0, allow_at - now, tat - now}
end
local new_tat = tat + emission
redis.call('SET', key, new_tat, 'PX', math.ceil((new_tat - now + emission) / 1000))
return {1, 0, new_tat - now}
`)

type GCRA struct {
	client *redis.Client
	rate   int
	burst  int
	now    func() time.Time
}

var _ out.RateLimiter = (*GCRA)(nil)

func NewGCRA(client *redis.Client, perSecond, burst int) *GCRA {
	return &GCRA{client: client, rate: perSecond, burst: burst, now: time.Now}
}

func (g *GCRA) Allow(ctx context.Context, key string) (out.RateDecision, error) {
	emission := 1e6 / float64(g.rate)
	tolerance := emission * float64(g.burst-1)
	now := g.now().UnixMicro()
	res, err := gcra.Run(ctx, g.client, []string{"rl:" + key}, emission, tolerance, now).Slice()
	if err != nil {
		return out.RateDecision{}, fmt.Errorf("ratelimit: %w", err)
	}
	allowed := toFloat(res[0]) == 1
	retry := time.Duration(toFloat(res[1]) * float64(time.Microsecond))
	ahead := toFloat(res[2])
	remaining := int((tolerance + emission - ahead) / emission)
	if remaining < 0 {
		remaining = 0
	}
	return out.RateDecision{
		Allowed:    allowed,
		Limit:      g.burst,
		Remaining:  remaining,
		RetryAfter: retry,
		ResetAfter: time.Duration(ahead * float64(time.Microsecond)),
	}, nil
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case float64:
		return n
	case string:
		var f float64
		_, _ = fmt.Sscan(n, &f)
		return f
	default:
		return 0
	}
}
