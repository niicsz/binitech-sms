package phone_test

import (
	"testing"

	"github.com/niicsz/binitech-sms/internal/domain/phone"
)

func TestParse(t *testing.T) {
	tests := map[string]string{
		"+5511987654321":      "+5511987654321",
		"+55 (11) 98765-4321": "+5511987654321",
		"5511987654321":       "+5511987654321",
		"+1 415 555 0100":     "+14155550100",
	}
	for in, want := range tests {
		got, err := phone.Parse(in)
		if err != nil || got.String() != want {
			t.Fatalf("Parse(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
}

func TestParseRejects(t *testing.T) {
	for _, in := range []string{"", "123", "+0123456789", "11 9876-abcd", "55+11987654321", "+12345678901234567"} {
		if _, err := phone.Parse(in); err == nil {
			t.Fatalf("Parse(%q) deveria falhar", in)
		}
	}
}
