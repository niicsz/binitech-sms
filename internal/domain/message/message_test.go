package message_test

import (
	"errors"
	"testing"

	"github.com/niicsz/binitech-sms/internal/domain/message"
	"github.com/niicsz/binitech-sms/internal/domain/money"
)

func TestHappyPath(t *testing.T) {
	m := message.Message{Status: message.Accepted}
	for _, s := range []message.Status{message.Routed, message.Submitted, message.Delivered} {
		if err := m.MoveTo(s); err != nil {
			t.Fatalf("MoveTo(%s): %v", s, err)
		}
	}
	if !m.Status.Final() {
		t.Fatal("delivered deveria ser final")
	}
}

func TestRejectsInvalidTransition(t *testing.T) {
	m := message.Message{Status: message.Delivered}
	if err := m.MoveTo(message.Failed); !errors.Is(err, message.ErrTransition) {
		t.Fatalf("got %v", err)
	}
	m = message.Message{Status: message.Accepted}
	if err := m.MoveTo(message.Delivered); !errors.Is(err, message.ErrTransition) {
		t.Fatalf("accepted não pula para delivered: got %v", err)
	}
}

func TestPriceBySegments(t *testing.T) {
	m := message.Message{Segments: 3}
	unit, _ := money.Parse("0.0800")
	m.Price(unit)
	if m.Cost.String() != "0.2400" {
		t.Fatalf("got %s", m.Cost)
	}
}
