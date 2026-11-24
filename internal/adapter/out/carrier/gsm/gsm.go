package gsm

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"go.bug.st/serial"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

const (
	ctrlZ          = "\x1a"
	commandTimeout = 10 * time.Second
	sendTimeout    = 60 * time.Second
)

type Config struct {
	Device string `json:"device"`
	Baud   int    `json:"baud"`
	PINEnv string `json:"pin_env"`
}

type Carrier struct {
	id      string
	port    io.ReadWriteCloser
	reports chan out.DeliveryReport
	now     func() time.Time

	cmd   sync.Mutex
	lines chan string
	done  chan struct{}

	refs  sync.Mutex
	byRef map[int]string
}

var _ out.Carrier = (*Carrier)(nil)

func New(id string, raw []byte, env func(string) string) (*Carrier, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("gsm %s: config: %w", id, err)
	}
	if cfg.Device == "" {
		return nil, fmt.Errorf("gsm %s: informe device (ex.: /dev/ttyUSB0 ou COM3)", id)
	}
	if cfg.Baud == 0 {
		cfg.Baud = 115200
	}
	port, err := serial.Open(cfg.Device, &serial.Mode{BaudRate: cfg.Baud})
	if err != nil {
		return nil, fmt.Errorf("gsm %s: abrir %s: %w", id, cfg.Device, err)
	}
	pin := ""
	if cfg.PINEnv != "" {
		pin = env(cfg.PINEnv)
	}
	return NewWithPort(id, port, pin)
}

func NewWithPort(id string, port io.ReadWriteCloser, pin string) (*Carrier, error) {
	c := &Carrier{
		id: id, port: port, reports: make(chan out.DeliveryReport, 256), now: time.Now,
		lines: make(chan string, 64), done: make(chan struct{}), byRef: map[int]string{},
	}
	go c.read()
	setup := []string{"AT", "ATE0", "AT+CMEE=1"}
	if pin != "" {
		setup = append(setup, `AT+CPIN="`+pin+`"`)
	}
	setup = append(setup, "AT+CMGF=1", "AT+CNMI=2,1,0,1,0")
	for _, command := range setup {
		if _, err := c.command(command, commandTimeout); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("gsm %s: %s: %w", id, command, err)
		}
	}
	return c, nil
}

func (c *Carrier) ID() string { return c.id }

func (c *Carrier) Send(ctx context.Context, sms out.OutboundSMS) (out.CarrierResult, error) {
	if sms.Segments > 1 {
		return out.CarrierResult{Accepted: false, ErrorCode: "gsm_multipart_unsupported"}, nil
	}
	c.cmd.Lock()
	defer c.cmd.Unlock()
	dcs, charset, dest, text := 0, "GSM", strings.TrimPrefix(sms.To, "+"), sms.Text
	if sms.Encoding == "UCS2" {
		dcs, charset, dest, text = 8, "UCS2", ucs2Hex("+"+dest), ucs2Hex(text)
	} else {
		dest = "+" + dest
	}
	for _, command := range []string{
		fmt.Sprintf("AT+CSMP=49,167,0,%d", dcs),
		fmt.Sprintf(`AT+CSCS="%s"`, charset),
	} {
		if _, err := c.locked(command, commandTimeout); err != nil {
			return out.CarrierResult{}, fmt.Errorf("%w: %w", out.ErrCarrierUnavailable, err)
		}
	}
	if err := c.write(fmt.Sprintf(`AT+CMGS="%s"`+"\r", dest)); err != nil {
		return out.CarrierResult{}, fmt.Errorf("%w: %w", out.ErrCarrierUnavailable, err)
	}
	if err := c.waitPrompt(ctx); err != nil {
		return out.CarrierResult{}, fmt.Errorf("%w: %w", out.ErrCarrierUnavailable, err)
	}
	if err := c.write(text + ctrlZ); err != nil {
		return out.CarrierResult{}, fmt.Errorf("%w: %w", out.ErrCarrierUnavailable, err)
	}
	lines, err := c.collect(sendTimeout)
	if err != nil {
		if strings.Contains(err.Error(), "CMS ERROR") {
			return out.CarrierResult{Accepted: false, ErrorCode: "gsm_" + strings.ReplaceAll(strings.TrimSpace(strings.TrimPrefix(err.Error(), "+CMS ERROR:")), " ", "")}, nil
		}
		return out.CarrierResult{}, fmt.Errorf("%w: %w", out.ErrCarrierUnavailable, err)
	}
	ref := -1
	for _, line := range lines {
		if rest, ok := strings.CutPrefix(line, "+CMGS:"); ok {
			ref, _ = strconv.Atoi(strings.TrimSpace(strings.Split(rest, ",")[0]))
		}
	}
	if ref < 0 {
		return out.CarrierResult{}, fmt.Errorf("%w: modem não devolveu +CMGS", out.ErrCarrierUnavailable)
	}
	id := fmt.Sprintf("%d-%d", c.now().UnixNano(), ref)
	c.refs.Lock()
	c.byRef[ref] = id
	c.refs.Unlock()
	return out.CarrierResult{Accepted: true, ProviderIDs: []string{id}}, nil
}

func (c *Carrier) Reports() <-chan out.DeliveryReport { return c.reports }

func (c *Carrier) Healthy(context.Context) error {
	if _, err := c.command("AT", commandTimeout); err != nil {
		return err
	}
	return nil
}

func (c *Carrier) Close() error {
	select {
	case <-c.done:
		return nil
	default:
		close(c.done)
	}
	return c.port.Close()
}

func (c *Carrier) command(command string, timeout time.Duration) ([]string, error) {
	c.cmd.Lock()
	defer c.cmd.Unlock()
	return c.locked(command, timeout)
}

func (c *Carrier) locked(command string, timeout time.Duration) ([]string, error) {
	if err := c.write(command + "\r"); err != nil {
		return nil, err
	}
	return c.collect(timeout)
}

func (c *Carrier) write(s string) error {
	_, err := io.WriteString(c.port, s)
	return err
}

func (c *Carrier) collect(timeout time.Duration) ([]string, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var lines []string
	for {
		select {
		case line := <-c.lines:
			switch {
			case line == "OK":
				return lines, nil
			case line == "ERROR", strings.HasPrefix(line, "+CME ERROR"), strings.HasPrefix(line, "+CMS ERROR"):
				return lines, errors.New(line)
			case line == ">":
			default:
				lines = append(lines, line)
			}
		case <-timer.C:
			return lines, errors.New("tempo esgotado esperando o modem")
		case <-c.done:
			return lines, errors.New("modem fechado")
		}
	}
}

func (c *Carrier) waitPrompt(ctx context.Context) error {
	timer := time.NewTimer(commandTimeout)
	defer timer.Stop()
	for {
		select {
		case line := <-c.lines:
			if line == ">" {
				return nil
			}
			if line == "ERROR" || strings.HasPrefix(line, "+CMS ERROR") || strings.HasPrefix(line, "+CME ERROR") {
				return errors.New(line)
			}
		case <-timer.C:
			return errors.New("modem não abriu o prompt de texto")
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return errors.New("modem fechado")
		}
	}
}

func (c *Carrier) read() {
	reader := bufio.NewReader(c.port)
	var buf strings.Builder
	for {
		b, err := reader.ReadByte()
		if err != nil {
			c.emit(buf.String())
			return
		}
		switch b {
		case '\n':
			c.emit(buf.String())
			buf.Reset()
		case '\r':
		default:
			buf.WriteByte(b)
			if strings.TrimSpace(buf.String()) == ">" && b == ' ' {
				c.push(">")
				buf.Reset()
			}
		}
	}
}

func (c *Carrier) emit(line string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return
	}
	if rest, ok := strings.CutPrefix(trimmed, "+CDS:"); ok {
		c.statusReport(rest)
		return
	}
	c.push(trimmed)
}

func (c *Carrier) push(line string) {
	select {
	case c.lines <- line:
	case <-c.done:
	}
}

func (c *Carrier) statusReport(rest string) {
	fields := strings.Split(rest, ",")
	if len(fields) < 2 {
		return
	}
	ref, err := strconv.Atoi(strings.TrimSpace(fields[1]))
	if err != nil {
		return
	}
	st, err := strconv.Atoi(strings.TrimSpace(fields[len(fields)-1]))
	if err != nil {
		return
	}
	status := ""
	switch {
	case st <= 31:
		status = "DELIVRD"
	case st <= 63:
		return
	default:
		status = "UNDELIV"
	}
	c.refs.Lock()
	id, ok := c.byRef[ref]
	delete(c.byRef, ref)
	c.refs.Unlock()
	if !ok {
		return
	}
	report := out.DeliveryReport{RouteID: c.id, ProviderID: id, Status: status, ErrorCode: strconv.Itoa(st), DoneAt: c.now()}
	select {
	case c.reports <- report:
	case <-c.done:
	}
}

func ucs2Hex(s string) string {
	units := utf16.Encode([]rune(s))
	buf := make([]byte, len(units)*2)
	for i, u := range units {
		binary.BigEndian.PutUint16(buf[i*2:], u)
	}
	return strings.ToUpper(hex.EncodeToString(buf))
}
