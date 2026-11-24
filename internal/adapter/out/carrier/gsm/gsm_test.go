package gsm_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/niicsz/binitech-sms/internal/adapter/out/carrier/gsm"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

type modem struct {
	hostR *io.PipeReader
	hostW *io.PipeWriter
	devR  *io.PipeReader
	devW  *io.PipeWriter

	mu       sync.Mutex
	commands []string
	texts    []string
	ref      int
	cmsError string
}

type port struct {
	io.Reader
	io.Writer
	closer func() error
}

func (p port) Close() error { return p.closer() }

func newModem(t *testing.T) (*modem, io.ReadWriteCloser) {
	t.Helper()
	m := &modem{}
	m.hostR, m.devW = io.Pipe()
	m.devR, m.hostW = io.Pipe()
	go m.run()
	t.Cleanup(func() { _ = m.devW.Close(); _ = m.devR.Close() })
	return m, port{Reader: m.hostR, Writer: m.hostW, closer: func() error { _ = m.hostW.Close(); return m.hostR.Close() }}
}

func (m *modem) say(lines ...string) {
	for _, l := range lines {
		_, _ = io.WriteString(m.devW, "\r\n"+l+"\r\n")
	}
}

func (m *modem) run() {
	r := bufio.NewReader(m.devR)
	for {
		line, err := r.ReadString('\r')
		if err != nil {
			return
		}
		cmd := strings.TrimSpace(line)
		m.mu.Lock()
		m.commands = append(m.commands, cmd)
		m.mu.Unlock()
		if strings.HasPrefix(cmd, "AT+CMGS=") {
			_, _ = io.WriteString(m.devW, "\r\n> ")
			text, err := r.ReadString('\x1a')
			if err != nil {
				return
			}
			m.mu.Lock()
			m.texts = append(m.texts, strings.TrimSuffix(text, "\x1a"))
			m.ref++
			ref, cms := m.ref, m.cmsError
			m.mu.Unlock()
			if cms != "" {
				m.say("+CMS ERROR: " + cms)
				continue
			}
			m.say(fmt.Sprintf("+CMGS: %d", ref), "OK")
			go func() {
				time.Sleep(20 * time.Millisecond)
				m.say(fmt.Sprintf("+CDS: 6,%d,\"+5511987654321\",145,\"26/11/24,12:00:00-12\",\"26/11/24,12:00:05-12\",0", ref))
			}()
			continue
		}
		m.say("OK")
	}
}

func TestSendGSMTextAndStatusReport(t *testing.T) {
	m, p := newModem(t)
	c, err := gsm.NewWithPort("modem-1", p, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	res, err := c.Send(context.Background(), out.OutboundSMS{To: "+5511987654321", Text: "Seu codigo e 482913", Encoding: "GSM7", Segments: 1})
	if err != nil || !res.Accepted || len(res.ProviderIDs) != 1 {
		t.Fatalf("got %+v %v", res, err)
	}
	select {
	case report := <-c.Reports():
		if report.ProviderID != res.ProviderIDs[0] || report.Status != "DELIVRD" {
			t.Fatalf("relatório: %+v", report)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("relatório de entrega não chegou")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	joined := strings.Join(m.commands, "|")
	for _, want := range []string{"AT+CMGF=1", "AT+CNMI=2,1,0,1,0", "AT+CSMP=49,167,0,0", `AT+CSCS="GSM"`, `AT+CMGS="+5511987654321"`} {
		if !strings.Contains(joined, want) {
			t.Fatalf("comando %s não enviado: %s", want, joined)
		}
	}
	if m.texts[0] != "Seu codigo e 482913" {
		t.Fatalf("texto: %q", m.texts[0])
	}
}

func TestUCS2EncodesNumberAndText(t *testing.T) {
	m, p := newModem(t)
	c, err := gsm.NewWithPort("modem-1", p, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Send(context.Background(), out.OutboundSMS{To: "+55", Text: "ç", Encoding: "UCS2", Segments: 1}); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	joined := strings.Join(m.commands, "|")
	if !strings.Contains(joined, `AT+CMGS="002B00350035"`) || !strings.Contains(joined, "AT+CSMP=49,167,0,8") || m.texts[0] != "00E7" {
		t.Fatalf("UCS2: %s texto=%q", joined, m.texts[0])
	}
}

func TestMultipartAndCMSError(t *testing.T) {
	m, p := newModem(t)
	c, err := gsm.NewWithPort("modem-1", p, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	res, _ := c.Send(context.Background(), out.OutboundSMS{To: "+5511987654321", Text: strings.Repeat("a", 200), Encoding: "GSM7", Segments: 2})
	if res.Accepted || res.ErrorCode != "gsm_multipart_unsupported" {
		t.Fatalf("multipart: %+v", res)
	}
	m.mu.Lock()
	m.cmsError = "21"
	m.mu.Unlock()
	res, err = c.Send(context.Background(), out.OutboundSMS{To: "+5511987654321", Text: "oi", Encoding: "GSM7", Segments: 1})
	if err != nil || res.Accepted || res.ErrorCode != "gsm_21" {
		t.Fatalf("CMS ERROR deveria virar recusa: %+v %v", res, err)
	}
}
