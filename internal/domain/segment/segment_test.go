package segment_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/niicsz/binitech-sms/internal/domain/segment"
)

func TestAnalyze(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		encoding segment.Encoding
		units    int
		segments int
	}{
		{"ó fora do alfabeto GSM força UCS-2", "Seu código é 482913", segment.UCS2, 19, 1},
		{"sem acento fica em GSM-7", "Seu codigo e 482913", segment.GSM7, 19, 1},
		{"ê e á forçam UCS-2", "Você está à vontade? é", segment.UCS2, 22, 1},
		{"160 caracteres cabem em 1", strings.Repeat("a", 160), segment.GSM7, 160, 1},
		{"161 caracteres viram 2", strings.Repeat("a", 161), segment.GSM7, 161, 2},
		{"306 caracteres cabem em 2", strings.Repeat("a", 306), segment.GSM7, 306, 2},
		{"307 caracteres viram 3", strings.Repeat("a", 307), segment.GSM7, 307, 3},
		{"extensão conta 2", "{}", segment.GSM7, 4, 1},
		{"80 chaves ocupam 160 unidades", strings.Repeat("{", 80), segment.GSM7, 160, 1},
		{"extensão não é partida entre segmentos", strings.Repeat("a", 152) + "€" + strings.Repeat("a", 152), segment.GSM7, 306, 3},
		{"emoji força UCS-2", "Oi 👋", segment.UCS2, 5, 1},
		{"70 unidades UCS-2 cabem em 1", strings.Repeat("ç", 70), segment.UCS2, 70, 1},
		{"71 unidades UCS-2 viram 2", strings.Repeat("ç", 71), segment.UCS2, 71, 2},
		{"134 unidades UCS-2 cabem em 2", strings.Repeat("ç", 134), segment.UCS2, 134, 2},
		{"par substituto não é partido", strings.Repeat("ç", 66) + "👋" + strings.Repeat("ç", 66), segment.UCS2, 134, 3},
		{"aspas tipográficas forçam UCS-2", "“oi”", segment.UCS2, 4, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := segment.Analyze(tt.text)
			if err != nil {
				t.Fatalf("erro inesperado: %v", err)
			}
			if got.Encoding != tt.encoding || got.Units != tt.units || got.Segments != tt.segments {
				t.Fatalf("got %+v, want encoding=%s units=%d segments=%d", got, tt.encoding, tt.units, tt.segments)
			}
		})
	}
}

func TestAnalyzeLimits(t *testing.T) {
	if _, err := segment.Analyze(""); !errors.Is(err, segment.ErrEmpty) {
		t.Fatalf("texto vazio: got %v", err)
	}
	if _, err := segment.Analyze(string([]byte{0xff, 0xfe})); !errors.Is(err, segment.ErrInvalid) {
		t.Fatalf("UTF-8 inválido: got %v", err)
	}
	if _, err := segment.Analyze(strings.Repeat("a", 153*10)); err != nil {
		t.Fatalf("10 segmentos deveria passar: %v", err)
	}
	if _, err := segment.Analyze(strings.Repeat("a", 153*10+1)); !errors.Is(err, segment.ErrTooLong) {
		t.Fatalf("11 segmentos: got %v", err)
	}
}

func TestNormalizeKeepsGSM7(t *testing.T) {
	got := segment.Normalize("“Pedido” pronto – retire…")
	if got != "\"Pedido\" pronto - retire..." {
		t.Fatalf("got %q", got)
	}
	res, err := segment.Analyze(got)
	if err != nil || res.Encoding != segment.GSM7 {
		t.Fatalf("normalizado deveria ser GSM-7: %+v %v", res, err)
	}
}
