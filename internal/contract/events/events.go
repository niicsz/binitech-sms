package events

import (
	"time"

	"github.com/niicsz/binitech-sms/internal/domain/money"
)

const (
	SchemaV1 = "v1"

	HeaderOperation   = "operation"
	HeaderOpID        = "op_id"
	HeaderSchema      = "schema"
	HeaderTraceParent = "traceparent"
	HeaderAccountID   = "account_id"
	HeaderProducedAt  = "produced_at"
	HeaderAttempt     = "attempt"
	HeaderError       = "error"

	OpMessageSubmit   = "message.submit"
	OpMessageDispatch = "message.dispatch"
	OpMessageDLR      = "message.dlr"
	OpWebhookDeliver  = "webhook.deliver"
	OpLedgerDebit     = "ledger.debit"
	OpLedgerRelease   = "ledger.release"
)

type Envelope struct {
	Topic   string
	Key     string
	Headers map[string]string
	Payload []byte
}

type MessageSubmitted struct {
	MessageID   string       `json:"message_id"`
	AccountID   string       `json:"account_id"`
	To          string       `json:"to"`
	From        string       `json:"from"`
	Text        string       `json:"text"`
	Encoding    string       `json:"encoding"`
	Segments    int          `json:"segments"`
	MCC         string       `json:"mcc,omitempty"`
	MNC         string       `json:"mnc,omitempty"`
	UnitPrice   money.Amount `json:"unit_price"`
	ExpiresAt   time.Time    `json:"expires_at"`
	CampaignID  *string      `json:"campaign_id"`
	SubmittedAt time.Time    `json:"submitted_at"`
}

type MessageDispatched struct {
	MessageID    string    `json:"message_id"`
	AccountID    string    `json:"account_id"`
	RouteID      string    `json:"route_id"`
	RouteReason  string    `json:"route_reason"`
	Attempt      int       `json:"attempt"`
	DispatchedAt time.Time `json:"dispatched_at"`
}

type DlrReceived struct {
	MessageID      string    `json:"message_id"`
	RouteID        string    `json:"route_id"`
	ProviderMsgID  string    `json:"provider_msg_id"`
	ProviderStatus string    `json:"provider_status"`
	MappedStatus   string    `json:"mapped_status"`
	ErrorCode      string    `json:"error_code"`
	DoneAt         time.Time `json:"done_at"`
	ReceivedAt     time.Time `json:"received_at"`
}

type WebhookDelivered struct {
	WebhookID   string    `json:"webhook_id"`
	Event       string    `json:"event"`
	MessageID   string    `json:"message_id"`
	Attempt     int       `json:"attempt"`
	HTTPStatus  int       `json:"http_status"`
	LatencyMS   int64     `json:"latency_ms"`
	DeliveredAt time.Time `json:"delivered_at"`
}

type LedgerDebited struct {
	OpID      string       `json:"op_id"`
	AccountID string       `json:"account_id"`
	MessageID string       `json:"message_id"`
	Amount    money.Amount `json:"amount"`
	Currency  string       `json:"currency"`
	Reason    string       `json:"reason"`
	DebitedAt time.Time    `json:"debited_at"`
}
