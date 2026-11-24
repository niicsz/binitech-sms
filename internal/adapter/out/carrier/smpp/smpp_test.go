package smpp_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fiorix/go-smpp/smpp/pdu"
	"github.com/fiorix/go-smpp/smpp/pdu/pdufield"
	"github.com/fiorix/go-smpp/smpp/smpptest"

	"github.com/niicsz/binitech-sms/internal/adapter/out/carrier/smpp"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

type smsc struct {
	server   *smpptest.Server
	next     atomic.Int64
	reject   atomic.Uint32
	mu       sync.Mutex
	received []pdu.Body
	wmu      sync.Mutex
}

func (s *smsc) write(c smpptest.Conn, p pdu.Body) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_ = c.Write(p)
}

func newSMSC(t *testing.T) *smsc {
	t.Helper()
	s := &smsc{}
	s.server = smpptest.NewUnstartedServer()
	s.server.Handler = func(c smpptest.Conn, m pdu.Body) {
		switch m.Header().ID {
		case pdu.SubmitSMID:
			s.mu.Lock()
			s.received = append(s.received, m)
			s.mu.Unlock()
			resp := pdu.NewSubmitSMResp()
			resp.Header().Seq = m.Header().Seq
			if status := s.reject.Load(); status != 0 {
				resp.Header().Status = pdu.Status(status)
				s.write(c, resp)
				return
			}
			id := fmt.Sprintf("%X", 0xA0+s.next.Add(1))
			_ = resp.Fields().Set(pdufield.MessageID, id)
			s.write(c, resp)
			go func() {
				time.Sleep(20 * time.Millisecond)
				dlr := pdu.NewDeliverSM()
				_ = dlr.Fields().Set(pdufield.ESMClass, uint8(0x04))
				_ = dlr.Fields().Set(pdufield.ShortMessage, "id:"+id+" sub:001 dlvrd:001 submit date:2611241200 done date:2611241201 stat:DELIVRD err:000 text:")
				s.write(c, dlr)
			}()
		default:
			s.wmu.Lock()
			smpptest.EchoHandler(c, m)
			s.wmu.Unlock()
		}
	}
	s.server.Start()
	t.Cleanup(s.server.Close)
	return s
}

func connect(t *testing.T, s *smsc) *smpp.Carrier {
	t.Helper()
	cfg := fmt.Sprintf(`{"addr":%q,"system_id":%q,"password_env":"PW","resp_timeout_seconds":2}`, s.server.Addr(), smpptest.DefaultUser)
	c, err := smpp.New("op-a", []byte(cfg), func(string) string { return smpptest.DefaultPasswd })
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	t.Cleanup(func() { _ = c.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for c.Healthy(context.Background()) != nil {
		if time.Now().After(deadline) {
			t.Fatalf("bind não conectou: %v", c.Healthy(context.Background()))
		}
		time.Sleep(20 * time.Millisecond)
	}
	return c
}

func TestSubmitAndReceipt(t *testing.T) {
	s := newSMSC(t)
	c := connect(t, s)
	res, err := c.Send(context.Background(), out.OutboundSMS{
		MessageID: "msg_1", To: "+5511987654321", From: "BINITECH", Text: "Seu codigo e 482913", Encoding: "GSM7", Segments: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil || !res.Accepted || len(res.ProviderIDs) != 1 {
		t.Fatalf("got %+v %v", res, err)
	}
	select {
	case report := <-c.Reports():
		if report.ProviderID != res.ProviderIDs[0] || report.Status != "DELIVRD" || report.RouteID != "op-a" {
			t.Fatalf("recibo: %+v", report)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recibo de entrega não chegou")
	}
	s.mu.Lock()
	sent := s.received[0]
	s.mu.Unlock()
	f := sent.Fields()
	if f[pdufield.DestinationAddr].String() != "5511987654321" || f[pdufield.SourceAddr].String() != "BINITECH" {
		t.Fatalf("endereços: %v %v", f[pdufield.SourceAddr], f[pdufield.DestinationAddr])
	}
	if ton := f[pdufield.SourceAddrTON].Raw(); ton != uint8(5) {
		t.Fatalf("remetente alfanumérico precisa de TON 5, veio %v", ton)
	}
}

func TestLongMessageSendsParts(t *testing.T) {
	s := newSMSC(t)
	c := connect(t, s)
	res, err := c.Send(context.Background(), out.OutboundSMS{
		MessageID: "msg_2", To: "+5511987654321", From: "29292", Text: strings.Repeat("a", 200), Encoding: "GSM7", Segments: 2,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil || len(res.ProviderIDs) != 2 || res.ProviderIDs[0] == res.ProviderIDs[1] {
		t.Fatalf("esperava 2 ids diferentes: %+v %v", res, err)
	}
}

func TestRejectionAndThrottling(t *testing.T) {
	s := newSMSC(t)
	c := connect(t, s)
	sms := out.OutboundSMS{MessageID: "m", To: "+5511987654321", From: "X", Text: "oi", Encoding: "GSM7", Segments: 1}

	s.reject.Store(0x0B)
	res, err := c.Send(context.Background(), sms)
	if err != nil || res.Accepted || res.ErrorCode != "smpp_0x0B" {
		t.Fatalf("destino inválido deveria ser recusa definitiva: %+v %v", res, err)
	}
	s.reject.Store(0x58)
	if _, err := c.Send(context.Background(), sms); err == nil {
		t.Fatal("throttling deveria voltar como erro repetível")
	}
}

func TestSendWhileDisconnected(t *testing.T) {
	c, err := smpp.New("op-b", []byte(`{"addr":"127.0.0.1:1","system_id":"x"}`), func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Send(context.Background(), out.OutboundSMS{To: "+5511987654321"}); err == nil {
		t.Fatal("sem bind deveria dar erro repetível")
	}
	if c.Healthy(context.Background()) == nil {
		t.Fatal("sem bind não está saudável")
	}
}

func TestParseReceipt(t *testing.T) {
	r := smpp.ParseReceipt("id:0A1B2C sub:001 dlvrd:000 submit date:2611241200 done date:2611241205 stat:UNDELIV err:005 Text:oi")
	if r.ID != "0A1B2C" || r.Status != "UNDELIV" || r.Err != "005" {
		t.Fatalf("got %+v", r)
	}
}
