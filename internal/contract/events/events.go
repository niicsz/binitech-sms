package events

import (
	"encoding/json"
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
	OpMessageStatus   = "message.status"
	OpCarrierAccepted = "message.carrier_accepted"
	OpCarrierRejected = "message.carrier_rejected"
	OpStatusChanged   = "message.status_changed"
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
	Test        bool         `json:"test,omitempty"`
	SubmittedAt time.Time    `json:"submitted_at"`
}

type MessageDispatched struct {
	MessageID    string    `json:"message_id"`
	AccountID    string    `json:"account_id"`
	RouteID      string    `json:"route_id"`
	RouteReason  string    `json:"route_reason"`
	To           string    `json:"to"`
	From         string    `json:"from"`
	Text         string    `json:"text"`
	Encoding     string    `json:"encoding"`
	Segments     int       `json:"segments"`
	ExpiresAt    time.Time `json:"expires_at"`
	Test         bool      `json:"test,omitempty"`
	Attempt      int       `json:"attempt"`
	DispatchedAt time.Time `json:"dispatched_at"`
}

type MessageStatus struct {
	MessageID string    `json:"message_id"`
	AccountID string    `json:"account_id"`
	Status    string    `json:"status"`
	ErrorCode string    `json:"error_code,omitempty"`
	At        time.Time `json:"at"`
}

type CarrierAccepted struct {
	MessageID      string    `json:"message_id"`
	AccountID      string    `json:"account_id"`
	RouteID        string    `json:"route_id"`
	ProviderMsgIDs []string  `json:"provider_msg_ids"`
	AcceptedAt     time.Time `json:"accepted_at"`
}

type CarrierRejected struct {
	MessageID  string    `json:"message_id"`
	AccountID  string    `json:"account_id"`
	RouteID    string    `json:"route_id"`
	ErrorCode  string    `json:"error_code"`
	RejectedAt time.Time `json:"rejected_at"`
}

type MessageStatusChanged struct {
	MessageID  string    `json:"message_id"`
	AccountID  string    `json:"account_id"`
	Status     string    `json:"status"`
	ErrorCode  string    `json:"error_code,omitempty"`
	WebhookRef string    `json:"webhook_ref,omitempty"`
	To         string    `json:"to"`
	Segments   int       `json:"segments"`
	Cost       string    `json:"cost"`
	Test       bool      `json:"test,omitempty"`
	ChangedAt  time.Time `json:"changed_at"`
}

type LedgerRequest struct {
	OpID      string       `json:"op_id"`
	AccountID string       `json:"account_id"`
	MessageID string       `json:"message_id"`
	Kind      string       `json:"kind"`
	Amount    money.Amount `json:"amount"`
	Reason    string       `json:"reason"`
	At        time.Time    `json:"at"`
}

type DlrReceived struct {
	MessageID      string    `json:"message_id,omitempty"`
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

type WebhookTask struct {
	DeliveryID string          `json:"delivery_id"`
	WebhookID  string          `json:"webhook_id"`
	AccountID  string          `json:"account_id"`
	Event      string          `json:"event"`
	Body       json.RawMessage `json:"body"`
	CreatedAt  time.Time       `json:"created_at"`
}

type WebhookBody struct {
	ID        string               `json:"id"`
	Event     string               `json:"event"`
	CreatedAt time.Time            `json:"created_at"`
	Data      MessageStatusChanged `json:"data"`
}
