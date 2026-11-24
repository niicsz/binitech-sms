package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/apikey"
)

const maxCached = 10000

type cached struct {
	principal in.Principal
	expires   time.Time
}

type Authenticator struct {
	keys   out.APIKeyStore
	hasher out.SecretHasher
	clock  out.Clock
	ttl    time.Duration

	mu    sync.Mutex
	cache map[[32]byte]cached
}

var _ in.Authenticator = (*Authenticator)(nil)

func NewAuthenticator(keys out.APIKeyStore, hasher out.SecretHasher, clock out.Clock, ttl time.Duration) *Authenticator {
	return &Authenticator{keys: keys, hasher: hasher, clock: clock, ttl: ttl, cache: map[[32]byte]cached{}}
}

func (a *Authenticator) Authenticate(ctx context.Context, rawKey string) (in.Principal, error) {
	unauthorized := apperr.New(apperr.Unauthorized, "Chave de API ausente, inválida ou revogada.")
	parsed, err := apikey.Parse(rawKey)
	if err != nil {
		return in.Principal{}, unauthorized
	}
	digest := sha256.Sum256([]byte(rawKey))
	now := a.clock.Now()
	if principal, ok := a.fromCache(digest, now); ok {
		return principal, nil
	}
	stored, err := a.keys.FindActive(ctx, parsed.ID)
	if errors.Is(err, out.ErrNotFound) {
		return in.Principal{}, unauthorized
	}
	if err != nil {
		return in.Principal{}, err
	}
	if stored.Env != parsed.Env || !a.hasher.Verify(parsed.Secret, stored.Hash, stored.Salt) {
		return in.Principal{}, unauthorized
	}
	if stored.AccountStatus != "active" {
		return in.Principal{}, apperr.New(apperr.Forbidden, "Conta suspensa ou encerrada.")
	}
	principal := in.Principal{AccountID: stored.AccountID, KeyID: stored.ID, Env: stored.Env}
	a.store(digest, principal, now)
	return principal, nil
}

func (a *Authenticator) fromCache(digest [32]byte, now time.Time) (in.Principal, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, ok := a.cache[digest]
	if !ok || now.After(entry.expires) {
		delete(a.cache, digest)
		return in.Principal{}, false
	}
	return entry.principal, true
}

func (a *Authenticator) store(digest [32]byte, principal in.Principal, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.cache) >= maxCached {
		clear(a.cache)
	}
	a.cache[digest] = cached{principal: principal, expires: now.Add(a.ttl)}
}

type RateLimiter struct {
	limiter out.RateLimiter
}

var _ in.RateLimiter = (*RateLimiter)(nil)

func NewRateLimiter(limiter out.RateLimiter) *RateLimiter {
	return &RateLimiter{limiter: limiter}
}

func (r *RateLimiter) Allow(ctx context.Context, principal in.Principal) (in.RateLimit, error) {
	decision, err := r.limiter.Allow(ctx, "account:"+principal.AccountID)
	if err != nil {
		return in.RateLimit{}, err
	}
	limit := in.RateLimit{Limit: decision.Limit, Remaining: decision.Remaining, ResetAfter: decision.ResetAfter}
	if !decision.Allowed {
		e := apperr.New(apperr.RateLimited, "Limite de requisições da conta atingido. Tente de novo em instantes.")
		e.RetryAfter = decision.RetryAfter
		return limit, e
	}
	return limit, nil
}
