package out

import (
	"context"
	"errors"
	"time"

	"github.com/niicsz/binitech-sms/internal/domain/message"
	"github.com/niicsz/binitech-sms/internal/domain/route"
)

var ErrCarrierUnavailable = errors.New("rota indisponível")

type RouteCatalog interface {
	Routes(ctx context.Context) ([]route.Route, error)
}

type RoutingStore interface {
	MarkRouted(ctx context.Context, messageID, routeID string) (bool, error)
}

type OutboundSMS struct {
	MessageID string
	AccountID string
	To        string
	From      string
	Text      string
	Encoding  string
	Segments  int
	ExpiresAt time.Time
}

type CarrierResult struct {
	Accepted    bool
	ProviderIDs []string
	ErrorCode   string
}

type DeliveryReport struct {
	RouteID    string
	ProviderID string
	MessageID  string
	Status     string
	ErrorCode  string
	DoneAt     time.Time
	Raw        map[string]string
}

type Carrier interface {
	ID() string
	Send(ctx context.Context, sms OutboundSMS) (CarrierResult, error)
	Reports() <-chan DeliveryReport
	Healthy(ctx context.Context) error
	Close() error
}

type RouteHealthStore interface {
	Report(ctx context.Context, routeID string, healthy bool, detail string) error
}

type Transition struct {
	MessageID string
	From      []message.Status
	To        message.Status
	ErrorCode string
	At        time.Time
}

type PartStatus struct {
	Part   int
	Status string
}

type StatusStore interface {
	Transition(ctx context.Context, t Transition) (StoredMessage, bool, error)
	SaveParts(ctx context.Context, messageID, routeID string, providerIDs []string) error
	FindPart(ctx context.Context, routeID, providerID string) (messageID string, part int, err error)
	UpdatePart(ctx context.Context, messageID string, part int, status, errorCode string) ([]PartStatus, error)
	RecordDLR(ctx context.Context, messageID, routeID, providerStatus, mappedStatus, errorCode, opID string, raw map[string]string) error
	Get(ctx context.Context, messageID string) (StoredMessage, error)
}
