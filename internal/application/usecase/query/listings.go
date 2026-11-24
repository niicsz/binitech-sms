package query

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

const (
	defaultLimit = 50
	maxLimit     = 100
)

type Listings struct {
	accounts   out.AccountRepository
	messages   out.MessageQuery
	alphanames out.AlphanameRepository
}

var _ in.Listings = (*Listings)(nil)

func NewListings(accounts out.AccountRepository, messages out.MessageQuery, alphanames out.AlphanameRepository) *Listings {
	return &Listings{accounts: accounts, messages: messages, alphanames: alphanames}
}

func (l *Listings) Messages(ctx context.Context, p in.Principal, f in.MessageFilter) (in.MessagePage, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	filter := out.MessageFilter{From: f.From, To: f.To, Status: f.Status, CampaignID: f.CampaignID, Limit: limit + 1}
	if f.Cursor != "" {
		created, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return in.MessagePage{}, apperr.Validation([]apperr.FieldError{{Field: "cursor", Message: "Cursor inválido; use o next_cursor da página anterior."}})
		}
		filter.AfterCreated, filter.AfterID = &created, id
	}
	account, err := l.accounts.Get(ctx, p.AccountID)
	if err != nil {
		return in.MessagePage{}, fmt.Errorf("conta: %w", err)
	}
	rows, err := l.messages.List(ctx, p.AccountID, filter)
	if err != nil {
		return in.MessagePage{}, err
	}
	page := in.MessagePage{Items: []in.MessageView{}}
	if len(rows) > limit {
		last := rows[limit-1]
		page.NextCursor = encodeCursor(last.CreatedAt, last.ID)
		rows = rows[:limit]
	}
	for _, m := range rows {
		page.Items = append(page.Items, in.MessageView{
			ID: m.ID, To: m.To.String(), From: m.Sender, Status: string(m.Status), Segments: m.Segments,
			Encoding: string(m.Encoding), Cost: m.Cost, Currency: account.Currency, ErrorCode: m.ErrorCode,
			WebhookRef: m.WebhookRef, Test: m.Test, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt, ExpiresAt: m.ExpiresAt,
		})
	}
	return page, nil
}

func (l *Listings) Alphanames(ctx context.Context, p in.Principal) ([]in.AlphanameView, error) {
	list, err := l.alphanames.ListAlphanames(ctx, p.AccountID)
	if err != nil {
		return nil, err
	}
	views := make([]in.AlphanameView, len(list))
	for i, a := range list {
		views[i] = in.AlphanameView(a)
	}
	return views, nil
}

func encodeCursor(created time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(created.UnixMicro(), 10) + "|" + id))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", err
	}
	stamp, id, ok := strings.Cut(string(raw), "|")
	if !ok || id == "" {
		return time.Time{}, "", fmt.Errorf("cursor sem id")
	}
	micros, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil {
		return time.Time{}, "", err
	}
	return time.UnixMicro(micros).UTC(), id, nil
}
