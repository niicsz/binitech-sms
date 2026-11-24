package campaign

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/apikey"
	"github.com/niicsz/binitech-sms/internal/domain/phone"
)

const (
	MaxRecipients   = 100000
	ChunkSize       = 1000
	maxScheduleDays = 30
	maxName         = 120
	defaultZone     = "America/Sao_Paulo"
)

type Service struct {
	store    out.CampaignStore
	accounts out.AccountRepository
	bulk     in.BulkSender
	ids      out.IDGenerator
	clock    out.Clock
}

var (
	_ in.CampaignManager = (*Service)(nil)
	_ in.CampaignRunner  = (*Service)(nil)
)

func New(store out.CampaignStore, accounts out.AccountRepository, bulk in.BulkSender, ids out.IDGenerator, clock out.Clock) *Service {
	return &Service{store: store, accounts: accounts, bulk: bulk, ids: ids, clock: clock}
}

func (s *Service) Create(ctx context.Context, p in.Principal, input in.CampaignInput) (in.CampaignSummary, error) {
	now := s.clock.Now()
	zone := input.TimeZone
	if zone == "" {
		zone = defaultZone
	}
	var fields []apperr.FieldError
	add := func(f, m string) { fields = append(fields, apperr.FieldError{Field: f, Message: m}) }
	name := strings.TrimSpace(input.Name)
	if name == "" || len([]rune(name)) > maxName {
		add("name", "Informe um nome de até 120 caracteres.")
	}
	if strings.TrimSpace(input.From) == "" {
		add("from", "Informe um remetente aprovado.")
	}
	if strings.TrimSpace(input.Text) == "" {
		add("text", "Informe o texto da campanha.")
	}
	if input.ScheduledAt.IsZero() || input.ScheduledAt.After(now.AddDate(0, 0, maxScheduleDays)) {
		add("scheduled_at", "Agende para no máximo 30 dias à frente.")
	}
	if _, err := time.LoadLocation(zone); err != nil {
		add("time_zone", "Fuso desconhecido; use um nome IANA como America/Sao_Paulo.")
	}
	if (input.WindowStart == "") != (input.WindowEnd == "") {
		add("window_start", "Informe início e fim da janela, ou nenhum dos dois.")
	} else if input.WindowStart != "" {
		if _, err := clock(input.WindowStart); err != nil {
			add("window_start", "Use o formato HH:MM.")
		}
		if _, err := clock(input.WindowEnd); err != nil {
			add("window_end", "Use o formato HH:MM.")
		}
	}
	if len(input.Recipients) == 0 || len(input.Recipients) > MaxRecipients {
		add("recipients", fmt.Sprintf("Envie de 1 a %d destinatários.", MaxRecipients))
	}
	recipients := make([]out.Recipient, 0, len(input.Recipients))
	for i, r := range input.Recipients {
		number, err := phone.Parse(r.To)
		if err != nil {
			add(fmt.Sprintf("recipients[%d].to", i), "Use o formato E.164.")
			if len(fields) > 20 {
				break
			}
			continue
		}
		recipients = append(recipients, out.Recipient{Position: i, To: number.String(), Vars: r.Vars})
	}
	if len(fields) > 0 {
		return in.CampaignSummary{}, apperr.Validation(fields)
	}
	c := out.Campaign{
		ID: "cmp_" + strings.ToLower(strings.TrimPrefix(s.ids.MessageID(), "msg_"))[:16], AccountID: p.AccountID,
		Name: name, Sender: strings.TrimSpace(input.From), Body: input.Text, Normalize: input.Normalize,
		ValidityMinutes: input.ValidityMinutes, ScheduledAt: input.ScheduledAt, WindowStart: input.WindowStart,
		WindowEnd: input.WindowEnd, TimeZone: zone, Test: p.Env == apikey.Test, Status: "scheduled",
		Total: len(recipients), IdempotencyKey: input.IdempotencyKey, CreatedAt: now,
	}
	id, err := s.store.Create(ctx, c, recipients)
	if err != nil {
		return in.CampaignSummary{}, err
	}
	return s.Summary(ctx, p, id)
}

func (s *Service) Summary(ctx context.Context, p in.Principal, id string) (in.CampaignSummary, error) {
	c, err := s.store.Get(ctx, p.AccountID, id)
	if errors.Is(err, out.ErrNotFound) {
		return in.CampaignSummary{}, apperr.New(apperr.NotFound, "Campanha não encontrada.")
	}
	if err != nil {
		return in.CampaignSummary{}, err
	}
	stats, err := s.store.Stats(ctx, id)
	if err != nil {
		return in.CampaignSummary{}, err
	}
	account, err := s.accounts.Get(ctx, p.AccountID)
	if err != nil {
		return in.CampaignSummary{}, err
	}
	return in.CampaignSummary{
		ID: c.ID, Name: c.Name, Status: c.Status, ScheduledAt: c.ScheduledAt, StartedAt: c.StartedAt, FinishedAt: c.FinishedAt,
		Total: c.Total, Pending: stats.Pending, Submitted: stats.Submitted, Rejected: stats.Rejected,
		Delivered: stats.Delivered, Failed: stats.Failed, InFlight: stats.InFlight, Cost: stats.Cost,
		Currency: account.Currency, Test: c.Test,
	}, nil
}

func (s *Service) Tick(ctx context.Context) (bool, error) {
	unlock, ok, err := s.store.TryLock(ctx)
	if err != nil || !ok {
		return false, err
	}
	defer unlock()
	now := s.clock.Now()
	var skip []string
	for {
		c, err := s.store.NextDue(ctx, now, skip)
		if errors.Is(err, out.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if !InWindow(c, now) {
			skip = append(skip, c.ID)
			continue
		}
		return true, s.runChunk(ctx, c, now)
	}
}

func (s *Service) runChunk(ctx context.Context, c out.Campaign, now time.Time) error {
	if c.Status == "scheduled" {
		if err := s.store.MarkStatus(ctx, c.ID, "running", now); err != nil {
			return err
		}
	}
	chunk, err := s.store.PendingChunk(ctx, c.ID, ChunkSize)
	if err != nil {
		return err
	}
	if len(chunk) == 0 {
		return s.store.MarkStatus(ctx, c.ID, "done", now)
	}
	result := out.ChunkResult{Accepted: map[int]string{}, Rejected: map[int]string{}}
	items := make([]in.BulkItem, 0, len(chunk))
	positions := make([]int, 0, len(chunk))
	for _, r := range chunk {
		text, ok := render(c.Body, r.Vars)
		if !ok {
			result.Rejected[r.Position] = "Variável sem valor para este destinatário."
			continue
		}
		items = append(items, in.BulkItem{To: r.To, Text: text})
		positions = append(positions, r.Position)
	}
	if len(items) > 0 {
		env := apikey.Live
		if c.Test {
			env = apikey.Test
		}
		res, err := s.bulk.SendBulk(ctx, in.BulkCommand{
			Principal:       in.Principal{AccountID: c.AccountID, Env: env},
			IdempotencyKey:  fmt.Sprintf("campaign:%s:%d", c.ID, positions[0]),
			From:            c.Sender,
			Normalize:       c.Normalize,
			ValidityMinutes: c.ValidityMinutes,
			Items:           items,
			CampaignID:      c.ID,
		})
		if apperr.KindOf(err) == apperr.InsufficientBalance {
			return s.store.MarkStatus(ctx, c.ID, "paused", now)
		}
		if err != nil {
			return err
		}
		for _, a := range res.Accepted {
			result.Accepted[positions[a.Index]] = a.MessageID
		}
		for _, r := range res.Rejected {
			result.Rejected[positions[r.Index]] = r.Message
		}
	}
	return s.store.RecordChunk(ctx, c.ID, result)
}

func render(body string, vars map[string]string) (string, bool) {
	for k, v := range vars {
		body = strings.ReplaceAll(body, "{{"+k+"}}", v)
	}
	return body, !strings.Contains(body, "{{")
}

func InWindow(c out.Campaign, now time.Time) bool {
	if c.WindowStart == "" {
		return true
	}
	loc, err := time.LoadLocation(c.TimeZone)
	if err != nil {
		return false
	}
	start, err1 := clock(c.WindowStart)
	end, err2 := clock(c.WindowEnd)
	if err1 != nil || err2 != nil {
		return false
	}
	local := now.In(loc)
	minute := local.Hour()*60 + local.Minute()
	if start <= end {
		return minute >= start && minute < end
	}
	return minute >= start || minute < end
}

func clock(hhmm string) (int, error) {
	t, err := time.Parse("15:04", hhmm)
	if err != nil {
		return 0, err
	}
	return t.Hour()*60 + t.Minute(), nil
}
