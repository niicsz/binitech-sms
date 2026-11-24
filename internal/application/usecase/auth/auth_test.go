package auth_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/auth"
	"github.com/niicsz/binitech-sms/internal/domain/apikey"
)

type plainHasher struct{}

func (plainHasher) Hash(secret string) ([]byte, []byte, error) { return []byte(secret), nil, nil }
func (plainHasher) Verify(secret string, hash, _ []byte) bool  { return secret == string(hash) }

type keyStore struct {
	keys  map[string]out.StoredKey
	calls int
}

func (s *keyStore) FindActive(_ context.Context, id string) (out.StoredKey, error) {
	s.calls++
	k, ok := s.keys[id]
	if !ok {
		return out.StoredKey{}, out.ErrNotFound
	}
	return k, nil
}

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func setup(t *testing.T, env apikey.Environment, status string) (string, *keyStore, *clock, *auth.Authenticator) {
	t.Helper()
	raw, parsed, err := apikey.Generate(env, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	store := &keyStore{keys: map[string]out.StoredKey{parsed.ID: {
		ID: parsed.ID, AccountID: "acc_1", Env: env, Hash: []byte(parsed.Secret), AccountStatus: status,
	}}}
	c := &clock{now: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)}
	return raw, store, c, auth.NewAuthenticator(store, plainHasher{}, c, 5*time.Minute)
}

func TestCachesVerifiedKeyUntilTTL(t *testing.T) {
	raw, store, c, a := setup(t, apikey.Live, "active")
	for range 3 {
		p, err := a.Authenticate(context.Background(), raw)
		if err != nil || p.AccountID != "acc_1" || p.Env != apikey.Live {
			t.Fatalf("got %+v %v", p, err)
		}
	}
	if store.calls != 1 {
		t.Fatalf("deveria consultar o banco 1 vez, foram %d", store.calls)
	}
	c.now = c.now.Add(6 * time.Minute)
	delete(store.keys, mustID(t, raw))
	if _, err := a.Authenticate(context.Background(), raw); apperr.KindOf(err) != apperr.Unauthorized {
		t.Fatalf("depois do TTL a chave revogada deve cair: %v", err)
	}
}

func TestRejectsWrongSecretAndEnvironment(t *testing.T) {
	raw, store, _, a := setup(t, apikey.Live, "active")
	tampered := raw[:len(raw)-1] + "Z"
	if raw[len(raw)-1] == 'Z' {
		tampered = raw[:len(raw)-1] + "Y"
	}
	if _, err := a.Authenticate(context.Background(), tampered); apperr.KindOf(err) != apperr.Unauthorized {
		t.Fatalf("segredo trocado: %v", err)
	}
	k := store.keys[mustID(t, raw)]
	k.Env = apikey.Test
	store.keys[k.ID] = k
	if _, err := a.Authenticate(context.Background(), raw); apperr.KindOf(err) != apperr.Unauthorized {
		t.Fatalf("chave live gravada como test: %v", err)
	}
}

func TestSuspendedAccount(t *testing.T) {
	raw, _, _, a := setup(t, apikey.Live, "suspended")
	if _, err := a.Authenticate(context.Background(), raw); apperr.KindOf(err) != apperr.Forbidden {
		t.Fatalf("conta suspensa: %v", err)
	}
}

func mustID(t *testing.T, raw string) string {
	t.Helper()
	p, err := apikey.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return p.ID
}
