package apikey

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
)

type Environment string

const (
	Live Environment = "live"
	Test Environment = "test"
)

const (
	idLength    = 12
	secretBytes = 64
	minSecret   = 80
	maxSecret   = 90
	alphabet    = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

var ErrMalformed = errors.New("apikey: formato inválido")

type Parsed struct {
	Env    Environment
	ID     string
	Secret string
}

func Parse(raw string) (Parsed, error) {
	var env Environment
	switch {
	case strings.HasPrefix(raw, "sk_live_"):
		env = Live
	case strings.HasPrefix(raw, "sk_test_"):
		env = Test
	default:
		return Parsed{}, ErrMalformed
	}
	rest := raw[len("sk_live_"):]
	id, secret, ok := strings.Cut(rest, "_")
	if !ok || len(id) != idLength || len(secret) < minSecret || len(secret) > maxSecret || !base62(id) || !base62(secret) {
		return Parsed{}, ErrMalformed
	}
	return Parsed{Env: env, ID: id, Secret: secret}, nil
}

func Generate(env Environment, random io.Reader) (raw string, parsed Parsed, err error) {
	if env != Live && env != Test {
		return "", Parsed{}, fmt.Errorf("apikey: ambiente %q desconhecido", env)
	}
	idBytes := make([]byte, idLength)
	if _, err := io.ReadFull(random, idBytes); err != nil {
		return "", Parsed{}, err
	}
	id := make([]byte, idLength)
	for i, b := range idBytes {
		id[i] = alphabet[int(b)%len(alphabet)]
	}
	secretRaw := make([]byte, secretBytes)
	if _, err := io.ReadFull(random, secretRaw); err != nil {
		return "", Parsed{}, err
	}
	parsed = Parsed{Env: env, ID: string(id), Secret: encode(secretRaw)}
	return fmt.Sprintf("sk_%s_%s_%s", env, parsed.ID, parsed.Secret), parsed, nil
}

func encode(b []byte) string {
	n := new(big.Int).SetBytes(b)
	base := big.NewInt(int64(len(alphabet)))
	mod := new(big.Int)
	var out []byte
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		out = append(out, alphabet[mod.Int64()])
	}
	for len(out) < minSecret {
		out = append(out, alphabet[0])
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

func base62(s string) bool {
	for _, c := range s {
		if !strings.ContainsRune(alphabet, c) {
			return false
		}
	}
	return true
}
