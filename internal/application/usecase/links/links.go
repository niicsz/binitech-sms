package links

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

const (
	codeLength = 7
	maxURL     = 2048
	alphabet   = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

type Service struct {
	store     out.LinkStore
	campaigns out.CampaignStore
	ids       out.IDGenerator
	clock     out.Clock
	random    io.Reader
	base      string
}

var (
	_ in.LinkShortener = (*Service)(nil)
	_ in.LinkResolver  = (*Service)(nil)
)

func New(store out.LinkStore, campaigns out.CampaignStore, ids out.IDGenerator, clock out.Clock, random io.Reader, base string) *Service {
	return &Service{store: store, campaigns: campaigns, ids: ids, clock: clock, random: random, base: strings.TrimRight(base, "/")}
}

func (s *Service) Shorten(ctx context.Context, p in.Principal, raw, campaignID, messageID string) (in.ShortLink, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || len(raw) > maxURL || u.User != nil {
		return in.ShortLink{}, apperr.Validation([]apperr.FieldError{{Field: "url", Message: "Use uma URL http ou https de até 2048 caracteres, sem usuário e senha."}})
	}
	if campaignID != "" {
		if _, err := s.campaigns.Get(ctx, p.AccountID, campaignID); errors.Is(err, out.ErrNotFound) {
			return in.ShortLink{}, apperr.Validation([]apperr.FieldError{{Field: "campaign_id", Message: "Campanha não encontrada."}})
		} else if err != nil {
			return in.ShortLink{}, err
		}
	}
	for range 5 {
		code, err := s.code()
		if err != nil {
			return in.ShortLink{}, err
		}
		link := out.Link{
			ID: "lnk_" + strings.ToLower(strings.TrimPrefix(s.ids.MessageID(), "msg_"))[:16], AccountID: p.AccountID,
			Code: code, URL: raw, CampaignID: campaignID, MessageID: messageID, CreatedAt: s.clock.Now(),
		}
		err = s.store.CreateLink(ctx, link)
		if errors.Is(err, out.ErrConflict) {
			continue
		}
		if err != nil {
			return in.ShortLink{}, err
		}
		return in.ShortLink{ID: link.ID, Code: code, ShortURL: s.base + "/" + code, URL: raw}, nil
	}
	return in.ShortLink{}, errors.New("links: não foi possível gerar um código livre")
}

func (s *Service) Resolve(ctx context.Context, click in.Click) (string, error) {
	link, err := s.store.ByCode(ctx, click.Code)
	if errors.Is(err, out.ErrNotFound) {
		return "", apperr.New(apperr.NotFound, "Link não encontrado.")
	}
	if err != nil {
		return "", err
	}
	var ipHash []byte
	if click.IP != "" {
		sum := sha256.Sum256([]byte(click.IP))
		ipHash = sum[:]
	}
	agent := click.UserAgent
	if len(agent) > 300 {
		agent = agent[:300]
	}
	if err := s.store.RecordClick(ctx, link.ID, ipHash, agent, s.clock.Now()); err != nil {
		return "", err
	}
	return link.URL, nil
}

func (s *Service) code() (string, error) {
	buf := make([]byte, 1)
	out := make([]byte, 0, codeLength)
	for len(out) < codeLength {
		if _, err := io.ReadFull(s.random, buf); err != nil {
			return "", fmt.Errorf("links: aleatoriedade: %w", err)
		}
		if buf[0] >= 248 {
			continue
		}
		out = append(out, alphabet[int(buf[0])%len(alphabet)])
	}
	return string(out), nil
}
