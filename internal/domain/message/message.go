package message

import (
	"errors"
	"fmt"
	"time"

	"github.com/niicsz/binitech-sms/internal/domain/money"
	"github.com/niicsz/binitech-sms/internal/domain/phone"
	"github.com/niicsz/binitech-sms/internal/domain/segment"
)

type Status string

const (
	Accepted    Status = "accepted"
	Routed      Status = "routed"
	Submitted   Status = "submitted"
	Delivered   Status = "delivered"
	Undelivered Status = "undelivered"
	Expired     Status = "expired"
	Rejected    Status = "rejected"
	Failed      Status = "failed"
)

var ErrTransition = errors.New("message: mudança de status inválida")

var next = map[Status][]Status{
	Accepted:  {Routed, Rejected, Expired, Failed},
	Routed:    {Submitted, Rejected, Expired, Failed},
	Submitted: {Delivered, Undelivered, Expired},
}

type Message struct {
	ID         string
	AccountID  string
	To         phone.Number
	Sender     string
	Body       string
	Encoding   segment.Encoding
	Segments   int
	UnitPrice  money.Amount
	Cost       money.Amount
	Status     Status
	WebhookRef string
	ExpiresAt  time.Time
	CreatedAt  time.Time
}

func (s Status) Final() bool {
	_, open := next[s]
	return !open
}

func (s Status) CanMoveTo(target Status) bool {
	for _, allowed := range next[s] {
		if allowed == target {
			return true
		}
	}
	return false
}

func (m *Message) MoveTo(target Status) error {
	if !m.Status.CanMoveTo(target) {
		return fmt.Errorf("%w: %s para %s", ErrTransition, m.Status, target)
	}
	m.Status = target
	return nil
}

func (m *Message) Price(unit money.Amount) {
	m.UnitPrice = unit
	m.Cost = unit.Times(m.Segments)
}
