package secretbox_test

import (
	"bytes"
	"testing"

	"github.com/niicsz/binitech-sms/internal/adapter/out/secretbox"
)

const key = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="

func TestSealOpen(t *testing.T) {
	box, err := secretbox.FromBase64(key)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := box.Seal([]byte("whsec_abc"))
	b, _ := box.Seal([]byte("whsec_abc"))
	if bytes.Equal(a, b) {
		t.Fatal("cada cifragem precisa de nonce próprio")
	}
	plain, err := box.Open(a)
	if err != nil || string(plain) != "whsec_abc" {
		t.Fatalf("got %q %v", plain, err)
	}
	a[len(a)-1] ^= 1
	if _, err := box.Open(a); err == nil {
		t.Fatal("conteúdo adulterado deveria falhar")
	}
}

func TestRejectsBadKey(t *testing.T) {
	for _, k := range []string{"", "curta", "MDEyMzQ1Njc4OWFiY2RlZg=="} {
		if _, err := secretbox.FromBase64(k); err == nil {
			t.Fatalf("chave %q deveria ser recusada", k)
		}
	}
}
