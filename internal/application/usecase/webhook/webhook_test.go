package webhook_test

import (
	"testing"

	"github.com/niicsz/binitech-sms/internal/application/usecase/webhook"
)

func TestSignKnownVector(t *testing.T) {
	got := webhook.Sign([]byte("whsec_test"), 1763564404, []byte(`{"id":"evt_1"}`))
	want := "t=1763564404,v1=83a486f944e8bccdb12bcaa3b76e3ff446ca0db7aa82c3d301a4454364f2ea56"
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
	if webhook.Sign([]byte("whsec_test"), 1763564404, []byte(`{"id":"evt_1"}`)) != got {
		t.Fatal("assinatura precisa ser determinística")
	}
	if webhook.Sign([]byte("outro"), 1763564404, []byte(`{"id":"evt_1"}`)) == got {
		t.Fatal("segredo diferente precisa gerar assinatura diferente")
	}
}

func TestEventFor(t *testing.T) {
	cases := map[string]string{"submitted": "message.submitted", "delivered": "message.delivered", "undelivered": "message.failed", "rejected": "message.failed", "expired": "message.failed", "routed": ""}
	for status, want := range cases {
		if got := webhook.EventFor(status); got != want {
			t.Fatalf("%s: got %q want %q", status, got, want)
		}
	}
}
