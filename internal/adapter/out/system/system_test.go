package system_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/niicsz/binitech-sms/internal/adapter/out/system"
)

type at time.Time

func (a at) Now() time.Time { return time.Time(a) }

func TestMessageIDIsSortableByTime(t *testing.T) {
	earlier := system.NewIDs(at(time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC))).MessageID()
	later := system.NewIDs(at(time.Date(2026, 11, 19, 12, 0, 1, 0, time.UTC))).MessageID()
	if !regexp.MustCompile(`^msg_[0-9A-HJKMNP-TV-Z]{26}$`).MatchString(earlier) {
		t.Fatalf("formato inesperado: %s", earlier)
	}
	if earlier[:14] >= later[:14] {
		t.Fatalf("ids fora de ordem: %s >= %s", earlier, later)
	}
}

func TestMessageIDsAreUnique(t *testing.T) {
	ids := system.NewIDs(system.Clock{})
	seen := map[string]bool{}
	for range 1000 {
		id := ids.MessageID()
		if seen[id] {
			t.Fatalf("id repetido: %s", id)
		}
		seen[id] = true
	}
}

func TestOpIDIsUUIDv4(t *testing.T) {
	id := system.NewIDs(system.Clock{}).OpID()
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(id) {
		t.Fatalf("não é UUID v4: %s", id)
	}
}
