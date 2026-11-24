package sandbox

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/route"
)

const (
	FailSuffix    = "0000"
	RejectSuffix  = "9999"
	defaultDelay  = time.Second
	reportsBuffer = 1024
)

type Carrier struct {
	delay   time.Duration
	reports chan out.DeliveryReport
	now     func() time.Time

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

var _ out.Carrier = (*Carrier)(nil)

func New(delay time.Duration) *Carrier {
	if delay <= 0 {
		delay = defaultDelay
	}
	return &Carrier{delay: delay, reports: make(chan out.DeliveryReport, reportsBuffer), now: time.Now}
}

func (c *Carrier) ID() string { return route.SandboxID }

func (c *Carrier) Send(_ context.Context, sms out.OutboundSMS) (out.CarrierResult, error) {
	if strings.HasSuffix(sms.To, RejectSuffix) {
		return out.CarrierResult{Accepted: false, ErrorCode: "sandbox_rejected"}, nil
	}
	parts := max(sms.Segments, 1)
	ids := make([]string, parts)
	for i := range ids {
		ids[i] = fmt.Sprintf("sbx-%s-%d", sms.MessageID, i+1)
	}
	status, code := "DELIVRD", "000"
	if strings.HasSuffix(sms.To, FailSuffix) {
		status, code = "UNDELIV", "001"
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return out.CarrierResult{}, out.ErrCarrierUnavailable
	}
	c.wg.Go(func() {
		time.Sleep(c.delay)
		for _, id := range ids {
			c.reports <- out.DeliveryReport{
				RouteID: route.SandboxID, ProviderID: id, MessageID: sms.MessageID,
				Status: status, ErrorCode: code, DoneAt: c.now(),
			}
		}
	})
	return out.CarrierResult{Accepted: true, ProviderIDs: ids}, nil
}

func (c *Carrier) Reports() <-chan out.DeliveryReport { return c.reports }

func (c *Carrier) Healthy(context.Context) error { return nil }

func (c *Carrier) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.wg.Wait()
	close(c.reports)
	return nil
}
