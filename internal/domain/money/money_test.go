package money_test

import (
	"encoding/json"
	"testing"

	"github.com/niicsz/binitech-sms/internal/domain/money"
)

func TestParseAndString(t *testing.T) {
	tests := map[string]string{
		"0.08":    "0.0800",
		"0.0800":  "0.0800",
		"12":      "12.0000",
		"-1.5":    "-1.5000",
		"1000.01": "1000.0100",
	}
	for in, want := range tests {
		got, err := money.Parse(in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", in, err)
		}
		if got.String() != want {
			t.Fatalf("Parse(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestParseRejects(t *testing.T) {
	for _, in := range []string{"", "abc", "1.23456", ".5", "1,5", "1.2.3"} {
		if _, err := money.Parse(in); err == nil {
			t.Fatalf("Parse(%q) deveria falhar", in)
		}
	}
}

func TestTimesKeepsExactCents(t *testing.T) {
	price, _ := money.Parse("0.0800")
	if got := price.Times(3).String(); got != "0.2400" {
		t.Fatalf("got %s", got)
	}
}

func TestJSONAsText(t *testing.T) {
	type payload struct {
		Cost money.Amount `json:"cost"`
	}
	raw, err := json.Marshal(payload{Cost: 800})
	if err != nil || string(raw) != `{"cost":"0.0800"}` {
		t.Fatalf("marshal: %s %v", raw, err)
	}
	var back payload
	if err := json.Unmarshal(raw, &back); err != nil || back.Cost != 800 {
		t.Fatalf("unmarshal: %+v %v", back, err)
	}
}
