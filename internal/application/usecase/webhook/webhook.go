package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/eventing"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

const (
	EventSubmitted = "message.submitted"
	EventDelivered = "message.delivered"
	EventFailed    = "message.failed"

	maxPerAccount  = 5
	maxDescription = 200
	disableAfter   = 50
	SignatureName  = "X-BiniTech-Signature"
)

var allowedEvents = []string{EventSubmitted, EventDelivered, EventFailed}

func EventFor(status string) string {
	switch status {
	case "submitted":
		return EventSubmitted
	case "delivered":
		return EventDelivered
	case "undelivered", "rejected", "expired", "failed":
		return EventFailed
	default:
		return ""
	}
}

func Sign(secret []byte, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", timestamp, hex.EncodeToString(mac.Sum(nil)))
}

type Manager struct {
	store  out.WebhookStore
	box    out.SecretBox
	ids    out.IDGenerator
	random io.Reader
	clock  out.Clock
}

var _ in.WebhookManager = (*Manager)(nil)

func NewManager(store out.WebhookStore, box out.SecretBox, ids out.IDGenerator, clock out.Clock, random io.Reader) *Manager {
	return &Manager{store: store, box: box, ids: ids, clock: clock, random: random}
}

func (m *Manager) Create(ctx context.Context, p in.Principal, input in.WebhookInput) (in.WebhookView, string, error) {
	if err := validate(input); err != nil {
		return in.WebhookView{}, "", err
	}
	count, err := m.store.Count(ctx, p.AccountID)
	if err != nil {
		return in.WebhookView{}, "", err
	}
	if count >= maxPerAccount {
		return in.WebhookView{}, "", apperr.New(apperr.Forbidden, fmt.Sprintf("Limite de %d webhooks por conta.", maxPerAccount))
	}
	raw := make([]byte, 32)
	if _, err := io.ReadFull(m.random, raw); err != nil {
		return in.WebhookView{}, "", err
	}
	secret := "whsec_" + base64.RawURLEncoding.EncodeToString(raw)
	sealed, err := m.box.Seal([]byte(secret))
	if err != nil {
		return in.WebhookView{}, "", err
	}
	now := m.clock.Now()
	w := out.Webhook{
		ID: "whk_" + strings.ToLower(strings.TrimPrefix(m.ids.MessageID(), "msg_"))[:16], AccountID: p.AccountID,
		URL: input.URL, Events: input.Events, Description: input.Description, Secret: sealed, CreatedAt: now, UpdatedAt: now,
	}
	if err := m.store.Create(ctx, w); err != nil {
		return in.WebhookView{}, "", err
	}
	return view(w), secret, nil
}

func (m *Manager) List(ctx context.Context, p in.Principal) ([]in.WebhookView, error) {
	list, err := m.store.List(ctx, p.AccountID)
	if err != nil {
		return nil, err
	}
	views := make([]in.WebhookView, len(list))
	for i, w := range list {
		views[i] = view(w)
	}
	return views, nil
}

func (m *Manager) Get(ctx context.Context, p in.Principal, id string) (in.WebhookView, error) {
	w, err := m.find(ctx, p, id)
	if err != nil {
		return in.WebhookView{}, err
	}
	return view(w), nil
}

func (m *Manager) Update(ctx context.Context, p in.Principal, id string, input in.WebhookInput, enable bool) (in.WebhookView, error) {
	if err := validate(input); err != nil {
		return in.WebhookView{}, err
	}
	w, err := m.find(ctx, p, id)
	if err != nil {
		return in.WebhookView{}, err
	}
	w.URL, w.Events, w.Description, w.UpdatedAt = input.URL, input.Events, input.Description, m.clock.Now()
	if enable {
		w.Disabled, w.Failures = false, 0
	}
	if err := m.store.Update(ctx, w); err != nil {
		return in.WebhookView{}, err
	}
	return view(w), nil
}

func (m *Manager) Delete(ctx context.Context, p in.Principal, id string) error {
	if _, err := m.find(ctx, p, id); err != nil {
		return err
	}
	return m.store.Delete(ctx, p.AccountID, id)
}

func (m *Manager) find(ctx context.Context, p in.Principal, id string) (out.Webhook, error) {
	w, err := m.store.Get(ctx, p.AccountID, id)
	if errors.Is(err, out.ErrNotFound) {
		return out.Webhook{}, apperr.New(apperr.NotFound, "Webhook não encontrado.")
	}
	return w, err
}

func validate(input in.WebhookInput) error {
	var fields []apperr.FieldError
	u, err := url.Parse(input.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		fields = append(fields, apperr.FieldError{Field: "url", Message: "Use uma URL https sem usuário e senha."})
	}
	if len(input.Events) == 0 {
		fields = append(fields, apperr.FieldError{Field: "events", Message: "Escolha ao menos um evento."})
	}
	for _, e := range input.Events {
		if !slices.Contains(allowedEvents, e) {
			fields = append(fields, apperr.FieldError{Field: "events", Message: "Evento desconhecido: " + e + "."})
		}
	}
	if len([]rune(input.Description)) > maxDescription {
		fields = append(fields, apperr.FieldError{Field: "description", Message: "Use até 200 caracteres."})
	}
	if len(fields) > 0 {
		return apperr.Validation(fields)
	}
	return nil
}

func view(w out.Webhook) in.WebhookView {
	return in.WebhookView{
		ID: w.ID, URL: w.URL, Events: w.Events, Description: w.Description, Enabled: !w.Disabled,
		Failures: w.Failures, LastStatus: w.LastStatus, CreatedAt: w.CreatedAt, UpdatedAt: w.UpdatedAt,
	}
}

type Dispatcher struct {
	store  out.WebhookStore
	box    out.SecretBox
	sender out.WebhookSender
	events *eventing.Emitter
}

var _ in.EventHandler = (*Dispatcher)(nil)

func NewDispatcher(store out.WebhookStore, box out.SecretBox, sender out.WebhookSender, emitter *eventing.Emitter) *Dispatcher {
	return &Dispatcher{store: store, box: box, sender: sender, events: emitter}
}

func (d *Dispatcher) Handle(ctx context.Context, e in.Event) error {
	switch e.Headers[events.HeaderOperation] {
	case events.OpStatusChanged:
		return d.fanOut(ctx, e)
	case events.OpWebhookDeliver:
		return d.deliver(ctx, e)
	default:
		return nil
	}
}

func (d *Dispatcher) fanOut(ctx context.Context, e in.Event) error {
	var changed events.MessageStatusChanged
	if err := json.Unmarshal(e.Payload, &changed); err != nil {
		return in.Permanent(err)
	}
	name := EventFor(changed.Status)
	if name == "" {
		return nil
	}
	hooks, err := d.store.Subscribed(ctx, changed.AccountID, name)
	if err != nil || len(hooks) == 0 {
		return err
	}
	opID := e.Headers[events.HeaderOpID]
	body, err := json.Marshal(events.WebhookBody{ID: "evt_" + opID, Event: name, CreatedAt: changed.ChangedAt, Data: changed})
	if err != nil {
		return in.Permanent(err)
	}
	envelopes := make([]events.Envelope, 0, len(hooks))
	for _, h := range hooks {
		envelope, err := d.events.Envelope(events.TopicWebhookOutbound, h.ID, events.OpWebhookDeliver, changed.AccountID, events.WebhookTask{
			DeliveryID: opID + ":" + h.ID, WebhookID: h.ID, AccountID: changed.AccountID, Event: name, Body: body, CreatedAt: d.events.Now(),
		})
		if err != nil {
			return in.Permanent(err)
		}
		envelopes = append(envelopes, envelope)
	}
	return d.events.Publish(ctx, envelopes...)
}

func (d *Dispatcher) deliver(ctx context.Context, e in.Event) error {
	var task events.WebhookTask
	if err := json.Unmarshal(e.Payload, &task); err != nil {
		return in.Permanent(err)
	}
	hook, err := d.store.GetActive(ctx, task.WebhookID)
	if errors.Is(err, out.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	secret, err := d.box.Open(hook.Secret)
	if err != nil {
		return in.Permanent(fmt.Errorf("webhook %s: segredo: %w", hook.ID, err))
	}
	headers := map[string]string{
		"Content-Type":        "application/json",
		"User-Agent":          "BiniTech-Webhooks/1",
		SignatureName:         Sign(secret, d.events.Now().Unix(), task.Body),
		"X-BiniTech-Event":    task.Event,
		"X-BiniTech-Delivery": task.DeliveryID,
	}
	status, postErr := d.sender.Post(ctx, hook.URL, task.Body, headers)
	ok := postErr == nil && status >= 200 && status < 300
	disabled, err := d.store.RecordAttempt(ctx, hook.ID, status, ok, disableAfter)
	if err != nil {
		return err
	}
	if ok || disabled {
		return nil
	}
	if postErr != nil {
		return fmt.Errorf("webhook %s: %w", hook.ID, postErr)
	}
	return fmt.Errorf("webhook %s: status %d", hook.ID, status)
}
