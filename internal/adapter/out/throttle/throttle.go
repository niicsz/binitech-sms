package throttle

import (
	"context"

	"golang.org/x/time/rate"
)

type TokenBucket struct {
	limiter *rate.Limiter
}

func New(perSecond float64, burst int) *TokenBucket {
	if perSecond <= 0 {
		return &TokenBucket{limiter: rate.NewLimiter(rate.Inf, 1)}
	}
	return &TokenBucket{limiter: rate.NewLimiter(rate.Limit(perSecond), max(burst, 1))}
}

func (t *TokenBucket) Wait(ctx context.Context) error {
	return t.limiter.Wait(ctx)
}
