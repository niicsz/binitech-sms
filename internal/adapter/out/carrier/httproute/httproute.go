package httproute

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

const maxCallbackBytes = 64 << 10

type DLRMapping struct {
	IDField     string `json:"id_field"`
	StatusField string `json:"status_field"`
	ErrorField  string `json:"error_field"`
	DoneField   string `json:"done_field"`
}

type Config struct {
	URL             string            `json:"url"`
	Method          string            `json:"method"`
	AuthHeader      string            `json:"auth_header"`
	AuthEnv         string            `json:"auth_env"`
	Body            map[string]string `json:"body"`
	IDField         string            `json:"id_field"`
	SuccessStatuses []int             `json:"success_statuses"`
	CallbackToken   string            `json:"callback_token_env"`
	DLR             DLRMapping        `json:"dlr"`
	TimeoutSeconds  int               `json:"timeout_seconds"`
	HealthURL       string            `json:"health_url"`
}

type Carrier struct {
	id      string
	cfg     Config
	secret  string
	token   string
	client  *http.Client
	reports chan out.DeliveryReport
	now     func() time.Time
	closed  chan struct{}
}

var _ out.Carrier = (*Carrier)(nil)

func New(id string, raw []byte, env func(string) string) (*Carrier, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("httproute %s: config: %w", id, err)
	}
	if !strings.HasPrefix(cfg.URL, "https://") && !strings.HasPrefix(cfg.URL, "http://127.0.0.1") && !strings.HasPrefix(cfg.URL, "http://localhost") {
		return nil, fmt.Errorf("httproute %s: use https na url do fornecedor", id)
	}
	if cfg.Method == "" {
		cfg.Method = http.MethodPost
	}
	if len(cfg.SuccessStatuses) == 0 {
		cfg.SuccessStatuses = []int{http.StatusOK, http.StatusCreated, http.StatusAccepted}
	}
	if cfg.IDField == "" {
		return nil, fmt.Errorf("httproute %s: informe id_field", id)
	}
	timeout := time.Duration(max(cfg.TimeoutSeconds, 1)) * time.Second
	if cfg.TimeoutSeconds == 0 {
		timeout = 10 * time.Second
	}
	c := &Carrier{
		id: id, cfg: cfg, client: &http.Client{Timeout: timeout},
		reports: make(chan out.DeliveryReport, 1024), now: time.Now, closed: make(chan struct{}),
	}
	if cfg.AuthEnv != "" {
		if c.secret = env(cfg.AuthEnv); c.secret == "" {
			return nil, fmt.Errorf("httproute %s: variável %s vazia", id, cfg.AuthEnv)
		}
	}
	if cfg.CallbackToken != "" {
		if c.token = env(cfg.CallbackToken); c.token == "" {
			return nil, fmt.Errorf("httproute %s: variável %s vazia", id, cfg.CallbackToken)
		}
	}
	return c, nil
}

func (c *Carrier) ID() string { return c.id }

func (c *Carrier) Send(ctx context.Context, sms out.OutboundSMS) (out.CarrierResult, error) {
	values := map[string]string{
		"to": sms.To, "from": sms.From, "text": sms.Text, "message_id": sms.MessageID,
		"encoding": sms.Encoding, "to_digits": strings.TrimPrefix(sms.To, "+"),
	}
	body := make(map[string]string, len(c.cfg.Body))
	for field, tpl := range c.cfg.Body {
		body[field] = render(tpl, values)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return out.CarrierResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, c.cfg.Method, c.cfg.URL, bytes.NewReader(payload))
	if err != nil {
		return out.CarrierResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.AuthHeader != "" {
		req.Header.Set(c.cfg.AuthHeader, c.secret)
	}
	res, err := c.client.Do(req)
	if err != nil {
		return out.CarrierResult{}, fmt.Errorf("%w: %w", out.ErrCarrierUnavailable, err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, maxCallbackBytes))
	switch {
	case c.success(res.StatusCode):
	case res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500:
		return out.CarrierResult{}, fmt.Errorf("%w: status %d", out.ErrCarrierUnavailable, res.StatusCode)
	default:
		return out.CarrierResult{Accepted: false, ErrorCode: fmt.Sprintf("http_%d", res.StatusCode)}, nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return out.CarrierResult{}, fmt.Errorf("%w: resposta sem JSON", out.ErrCarrierUnavailable)
	}
	id := lookup(decoded, c.cfg.IDField)
	if id == "" {
		return out.CarrierResult{}, fmt.Errorf("%w: resposta sem %s", out.ErrCarrierUnavailable, c.cfg.IDField)
	}
	return out.CarrierResult{Accepted: true, ProviderIDs: []string{id}}, nil
}

func (c *Carrier) CallbackHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if c.token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Callback-Token")), []byte(c.token)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var decoded map[string]any
		if err := json.NewDecoder(io.LimitReader(r.Body, maxCallbackBytes)).Decode(&decoded); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m := c.cfg.DLR
		id := lookup(decoded, m.IDField)
		status := lookup(decoded, m.StatusField)
		if id == "" || status == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		done := c.now()
		if raw := lookup(decoded, m.DoneField); raw != "" {
			if t, err := time.Parse(time.RFC3339, raw); err == nil {
				done = t
			}
		}
		report := out.DeliveryReport{RouteID: c.id, ProviderID: id, Status: status, ErrorCode: lookup(decoded, m.ErrorField), DoneAt: done}
		select {
		case c.reports <- report:
			w.WriteHeader(http.StatusNoContent)
		case <-c.closed:
			w.WriteHeader(http.StatusServiceUnavailable)
		case <-r.Context().Done():
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
}

func (c *Carrier) Reports() <-chan out.DeliveryReport { return c.reports }

func (c *Carrier) Healthy(ctx context.Context) error {
	if c.cfg.HealthURL == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.HealthURL, nil)
	if err != nil {
		return err
	}
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode >= 300 {
		return fmt.Errorf("saúde: status %d", res.StatusCode)
	}
	return nil
}

func (c *Carrier) Close() error {
	select {
	case <-c.closed:
		return errors.New("httproute: já fechado")
	default:
		close(c.closed)
	}
	return nil
}

func (c *Carrier) success(status int) bool {
	for _, s := range c.cfg.SuccessStatuses {
		if s == status {
			return true
		}
	}
	return false
}

func render(tpl string, values map[string]string) string {
	for key, value := range values {
		tpl = strings.ReplaceAll(tpl, "{{"+key+"}}", value)
	}
	return tpl
}

func lookup(m map[string]any, path string) string {
	if path == "" {
		return ""
	}
	var current any = m
	for _, part := range strings.Split(path, ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = obj[part]
	}
	switch v := current.(type) {
	case string:
		return v
	case float64:
		return strings.TrimSuffix(strings.TrimRight(fmt.Sprintf("%f", v), "0"), ".")
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}
