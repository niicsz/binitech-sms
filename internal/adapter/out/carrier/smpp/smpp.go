package smpp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	fsmpp "github.com/fiorix/go-smpp/smpp"
	"github.com/fiorix/go-smpp/smpp/pdu"
	"github.com/fiorix/go-smpp/smpp/pdu/pdufield"
	"github.com/fiorix/go-smpp/smpp/pdu/pdutext"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

const (
	esmDeliveryReceipt  = 0x04
	statusThrottled     = 0x58
	statusQueueFull     = 0x14
	statusSystemError   = 0x08
	defaultEnquireLink  = 10 * time.Second
	defaultRespTimeout  = 5 * time.Second
	defaultBindInterval = 5 * time.Second
)

type Config struct {
	Addr               string `json:"addr"`
	SystemID           string `json:"system_id"`
	PasswordEnv        string `json:"password_env"`
	SystemType         string `json:"system_type"`
	TLS                bool   `json:"tls"`
	EnquireLinkSeconds int    `json:"enquire_link_seconds"`
	RespTimeoutSeconds int    `json:"resp_timeout_seconds"`
	Window             uint   `json:"window"`
	AlphaTON           uint8  `json:"alpha_ton"`
	AlphaNPI           uint8  `json:"alpha_npi"`
	NumericTON         uint8  `json:"numeric_ton"`
	NumericNPI         uint8  `json:"numeric_npi"`
	DestTON            uint8  `json:"dest_ton"`
	DestNPI            uint8  `json:"dest_npi"`
	DLRIDFormat        string `json:"dlr_id_format"`
}

type Carrier struct {
	id        string
	cfg       Config
	tx        *fsmpp.Transceiver
	reports   chan out.DeliveryReport
	connected atomic.Bool
	lastErr   atomic.Value
	now       func() time.Time
	done      chan struct{}
}

var _ out.Carrier = (*Carrier)(nil)

func New(id string, raw []byte, env func(string) string) (*Carrier, error) {
	cfg := Config{AlphaTON: 5, AlphaNPI: 0, NumericTON: 1, NumericNPI: 1, DestTON: 1, DestNPI: 1, Window: 10}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("smpp %s: config: %w", id, err)
	}
	if cfg.Addr == "" || cfg.SystemID == "" {
		return nil, fmt.Errorf("smpp %s: informe addr e system_id", id)
	}
	password := ""
	if cfg.PasswordEnv != "" {
		if password = env(cfg.PasswordEnv); password == "" {
			return nil, fmt.Errorf("smpp %s: variável %s vazia", id, cfg.PasswordEnv)
		}
	}
	switch cfg.DLRIDFormat {
	case "", "same", "hex_to_dec", "dec_to_hex":
	default:
		return nil, fmt.Errorf("smpp %s: dlr_id_format %q desconhecido", id, cfg.DLRIDFormat)
	}
	c := &Carrier{id: id, cfg: cfg, reports: make(chan out.DeliveryReport, 4096), now: time.Now, done: make(chan struct{})}
	c.tx = &fsmpp.Transceiver{
		Addr:               cfg.Addr,
		User:               cfg.SystemID,
		Passwd:             password,
		SystemType:         cfg.SystemType,
		EnquireLink:        seconds(cfg.EnquireLinkSeconds, defaultEnquireLink),
		EnquireLinkTimeout: 3 * seconds(cfg.EnquireLinkSeconds, defaultEnquireLink),
		RespTimeout:        seconds(cfg.RespTimeoutSeconds, defaultRespTimeout),
		BindInterval:       defaultBindInterval,
		WindowSize:         cfg.Window,
		Handler:            c.handle,
	}
	if cfg.TLS {
		c.tx.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return c, nil
}

func (c *Carrier) Start() {
	statuses := c.tx.Bind()
	go func() {
		for status := range statuses {
			c.connected.Store(status.Status() == fsmpp.Connected)
			if err := status.Error(); err != nil {
				c.lastErr.Store(err.Error())
			}
		}
	}()
}

func (c *Carrier) ID() string { return c.id }

func (c *Carrier) Send(_ context.Context, sms out.OutboundSMS) (out.CarrierResult, error) {
	if !c.connected.Load() {
		return out.CarrierResult{}, fmt.Errorf("%w: bind não conectado", out.ErrCarrierUnavailable)
	}
	var codec pdutext.Codec = pdutext.GSM7(sms.Text)
	if sms.Encoding == "UCS2" {
		codec = pdutext.UCS2(sms.Text)
	}
	ton, npi := c.cfg.NumericTON, c.cfg.NumericNPI
	if hasLetter(sms.From) {
		ton, npi = c.cfg.AlphaTON, c.cfg.AlphaNPI
	}
	msg := &fsmpp.ShortMessage{
		Src: sms.From, Dst: strings.TrimPrefix(sms.To, "+"), Text: codec,
		Register:      pdufield.FinalDeliveryReceipt,
		SourceAddrTON: ton, SourceAddrNPI: npi, DestAddrTON: c.cfg.DestTON, DestAddrNPI: c.cfg.DestNPI,
	}
	if v := time.Until(sms.ExpiresAt); v > time.Minute {
		msg.Validity = v.Truncate(time.Minute)
	}
	var ids []string
	if sms.Segments > 1 {
		parts, err := c.tx.SubmitLongMsg(msg)
		if err != nil {
			return classify(err)
		}
		for i := range parts {
			ids = append(ids, parts[i].RespID())
		}
	} else {
		sent, err := c.tx.Submit(msg)
		if err != nil {
			return classify(err)
		}
		ids = []string{sent.RespID()}
	}
	for _, id := range ids {
		if id == "" {
			return out.CarrierResult{}, fmt.Errorf("%w: submit_sm_resp sem message_id", out.ErrCarrierUnavailable)
		}
	}
	return out.CarrierResult{Accepted: true, ProviderIDs: ids}, nil
}

func classify(err error) (out.CarrierResult, error) {
	var status pdu.Status
	if errors.As(err, &status) {
		switch uint32(status) {
		case statusThrottled, statusQueueFull, statusSystemError:
			return out.CarrierResult{}, fmt.Errorf("%w: status 0x%02X", out.ErrCarrierUnavailable, uint32(status))
		default:
			return out.CarrierResult{Accepted: false, ErrorCode: fmt.Sprintf("smpp_0x%02X", uint32(status))}, nil
		}
	}
	return out.CarrierResult{}, fmt.Errorf("%w: %w", out.ErrCarrierUnavailable, err)
}

func (c *Carrier) handle(p pdu.Body) {
	if p.Header().ID != pdu.DeliverSMID {
		return
	}
	fields := p.Fields()
	var esm uint8
	if f := fields[pdufield.ESMClass]; f != nil {
		if v, ok := f.Raw().(uint8); ok {
			esm = v
		}
	}
	text := ""
	if f := fields[pdufield.ShortMessage]; f != nil {
		text = f.String()
	}
	if esm&esmDeliveryReceipt == 0 {
		from, to := "", ""
		if f := fields[pdufield.SourceAddr]; f != nil {
			from = f.String()
		}
		if f := fields[pdufield.DestinationAddr]; f != nil {
			to = f.String()
		}
		select {
		case c.reports <- out.DeliveryReport{RouteID: c.id, Status: out.StatusMO, DoneAt: c.now(), Raw: map[string]string{"from": from, "to": to, "text": text}}:
		case <-c.done:
		}
		return
	}
	receipt := ParseReceipt(text)
	if receipt.ID == "" || receipt.Status == "" {
		return
	}
	report := out.DeliveryReport{
		RouteID: c.id, ProviderID: c.normalizeID(receipt.ID), Status: receipt.Status, ErrorCode: receipt.Err,
		DoneAt: c.now(), Raw: map[string]string{"short_message": text},
	}
	select {
	case c.reports <- report:
	case <-c.done:
	}
}

func (c *Carrier) normalizeID(id string) string {
	switch c.cfg.DLRIDFormat {
	case "hex_to_dec":
		if n, ok := new(big.Int).SetString(id, 16); ok {
			return n.String()
		}
	case "dec_to_hex":
		if n, ok := new(big.Int).SetString(id, 10); ok {
			return strings.ToUpper(n.Text(16))
		}
	}
	return id
}

func (c *Carrier) Reports() <-chan out.DeliveryReport { return c.reports }

func (c *Carrier) Healthy(context.Context) error {
	if c.connected.Load() {
		return nil
	}
	if msg, ok := c.lastErr.Load().(string); ok && msg != "" {
		return fmt.Errorf("bind desconectado: %s", msg)
	}
	return errors.New("bind desconectado")
}

func (c *Carrier) Close() error {
	select {
	case <-c.done:
		return nil
	default:
		close(c.done)
	}
	return c.tx.Close()
}

type Receipt struct {
	ID     string
	Status string
	Err    string
}

func ParseReceipt(text string) Receipt {
	var r Receipt
	lower := strings.ToLower(text)
	value := func(key string) string {
		i := strings.Index(lower, key+":")
		if i < 0 {
			return ""
		}
		rest := text[i+len(key)+1:]
		if j := strings.IndexByte(rest, ' '); j >= 0 {
			rest = rest[:j]
		}
		return strings.TrimSpace(rest)
	}
	r.ID = value("id")
	r.Status = strings.ToUpper(value("stat"))
	r.Err = value("err")
	return r
}

func hasLetter(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) {
			return true
		}
	}
	return false
}

func seconds(v int, fallback time.Duration) time.Duration {
	if v <= 0 {
		return fallback
	}
	return time.Duration(v) * time.Second
}
