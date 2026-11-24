package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

type Service struct {
	accounts out.AccountRepository
	balances out.BalanceRepository
	messages out.MessageRepository
}

var _ in.AccountQueries = (*Service)(nil)

func New(accounts out.AccountRepository, balances out.BalanceRepository, messages out.MessageRepository) *Service {
	return &Service{accounts: accounts, balances: balances, messages: messages}
}

func (s *Service) Message(ctx context.Context, principal in.Principal, id string) (in.MessageView, error) {
	msg, err := s.messages.FindByID(ctx, principal.AccountID, id)
	if errors.Is(err, out.ErrNotFound) {
		return in.MessageView{}, apperr.New(apperr.NotFound, "Mensagem não encontrada.")
	}
	if err != nil {
		return in.MessageView{}, fmt.Errorf("mensagem: %w", err)
	}
	account, err := s.accounts.Get(ctx, principal.AccountID)
	if err != nil {
		return in.MessageView{}, fmt.Errorf("conta: %w", err)
	}
	return in.MessageView{
		ID:         msg.ID,
		To:         msg.To.String(),
		From:       msg.Sender,
		Status:     string(msg.Status),
		Segments:   msg.Segments,
		Encoding:   string(msg.Encoding),
		Cost:       msg.Cost,
		Currency:   account.Currency,
		ErrorCode:  msg.ErrorCode,
		WebhookRef: msg.WebhookRef,
		Test:       msg.Test,
		CreatedAt:  msg.CreatedAt,
		UpdatedAt:  msg.UpdatedAt,
		ExpiresAt:  msg.ExpiresAt,
	}, nil
}

func (s *Service) Balance(ctx context.Context, principal in.Principal) (in.BalanceView, error) {
	account, err := s.accounts.Get(ctx, principal.AccountID)
	if err != nil {
		return in.BalanceView{}, fmt.Errorf("conta: %w", err)
	}
	balance, err := s.balances.Get(ctx, principal.AccountID)
	if errors.Is(err, out.ErrNotFound) {
		return in.BalanceView{Currency: account.Currency}, nil
	}
	if err != nil {
		return in.BalanceView{}, fmt.Errorf("saldo: %w", err)
	}
	return in.BalanceView{
		Available: balance.Available,
		Reserved:  balance.Reserved,
		Spendable: balance.Available - balance.Reserved,
		Currency:  account.Currency,
	}, nil
}
