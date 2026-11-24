package webhookhttp_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/niicsz/binitech-sms/internal/adapter/out/webhookhttp"
)

func TestBlockedAddresses(t *testing.T) {
	blocked := []string{"127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.0.10", "169.254.169.254", "100.64.0.1", "::1", "fe80::1", "0.0.0.0", "::ffff:127.0.0.1"}
	for _, ip := range blocked {
		if !webhookhttp.Blocked(netip.MustParseAddr(ip)) {
			t.Errorf("%s deveria ser bloqueado", ip)
		}
	}
	for _, ip := range []string{"8.8.8.8", "200.160.2.3", "2001:4860:4860::8888"} {
		if webhookhttp.Blocked(netip.MustParseAddr(ip)) {
			t.Errorf("%s é público", ip)
		}
	}
}

func TestRefusesLoopbackByDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()
	_, err := webhookhttp.New(2*time.Second, false).Post(context.Background(), srv.URL, []byte("{}"), nil)
	if !errors.Is(err, webhookhttp.ErrBlockedAddress) {
		t.Fatalf("deveria recusar endereço interno: %v", err)
	}
}

func TestDoesNotFollowRedirects(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirect.Close()
	status, err := webhookhttp.New(2*time.Second, true).Post(context.Background(), redirect.URL, []byte("{}"), nil)
	if err != nil || status != http.StatusFound {
		t.Fatalf("redirecionamento deve contar como resposta, não ser seguido: %d %v", status, err)
	}
}
