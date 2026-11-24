package status_test

import (
	"testing"

	"github.com/niicsz/binitech-sms/internal/application/usecase/status"
)

func TestOptOutKeyword(t *testing.T) {
	yes := []string{"SAIR", "sair", " Sair. ", "PARAR", "stop", "Sair por favor", "descadastrar"}
	no := []string{"", "oi", "quero sair", "sairr", "STOPPED"}
	for _, text := range yes {
		if _, ok := status.OptOutKeyword(text); !ok {
			t.Errorf("%q deveria ser opt-out", text)
		}
	}
	for _, text := range no {
		if _, ok := status.OptOutKeyword(text); ok {
			t.Errorf("%q não é opt-out", text)
		}
	}
}
