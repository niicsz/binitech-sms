package apikey_test

import (
	"crypto/rand"
	"strings"
	"testing"

	"github.com/niicsz/binitech-sms/internal/domain/apikey"
)

func TestGenerateAndParse(t *testing.T) {
	for _, env := range []apikey.Environment{apikey.Live, apikey.Test} {
		raw, generated, err := apikey.Generate(env, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(raw, "sk_"+string(env)+"_") {
			t.Fatalf("prefixo errado: %s", raw)
		}
		parsed, err := apikey.Parse(raw)
		if err != nil {
			t.Fatalf("Parse(%s): %v", raw, err)
		}
		if parsed != generated {
			t.Fatalf("got %+v want %+v", parsed, generated)
		}
	}
}

func TestGeneratedKeysDiffer(t *testing.T) {
	a, _, _ := apikey.Generate(apikey.Live, rand.Reader)
	b, _, _ := apikey.Generate(apikey.Live, rand.Reader)
	if a == b {
		t.Fatal("duas chaves iguais")
	}
}

func TestParseRejects(t *testing.T) {
	valid, _, _ := apikey.Generate(apikey.Live, rand.Reader)
	cases := []string{
		"",
		"pk_live_abc",
		strings.Replace(valid, "sk_live_", "sk_prod_", 1),
		strings.Replace(valid, "_", "-", 3),
		valid[:len(valid)-20],
		valid + "!",
		"sk_live_short_" + strings.Repeat("a", 86),
	}
	for _, raw := range cases {
		if _, err := apikey.Parse(raw); err == nil {
			t.Fatalf("Parse(%q) deveria falhar", raw)
		}
	}
}

func TestRejectsUnknownEnvironment(t *testing.T) {
	if _, _, err := apikey.Generate("prod", rand.Reader); err == nil {
		t.Fatal("ambiente desconhecido deveria falhar")
	}
}
