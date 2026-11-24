package admin

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/apikey"
	"github.com/niicsz/binitech-sms/internal/domain/money"
)

type Service struct {
	tx     out.TxManager
	store  out.AdminStore
	ledger out.LedgerRepository
	hasher out.SecretHasher
	ids    out.IDGenerator
	clock  out.Clock
	random io.Reader
}

func New(tx out.TxManager, store out.AdminStore, ledger out.LedgerRepository, hasher out.SecretHasher, ids out.IDGenerator, clock out.Clock, random io.Reader) *Service {
	return &Service{tx: tx, store: store, ledger: ledger, hasher: hasher, ids: ids, clock: clock, random: random}
}

func (s *Service) CreateAccount(ctx context.Context, name, plan, currency string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", apperr.Validation([]apperr.FieldError{{Field: "name", Message: "Informe o nome da conta."}})
	}
	if plan == "" {
		plan = "default"
	}
	if currency == "" {
		currency = "BRL"
	}
	id := "acc_" + strings.ToLower(strings.TrimPrefix(s.ids.MessageID(), "msg_"))[:16]
	if err := s.store.InsertAccount(ctx, out.NewAccount{ID: id, Name: name, Plan: plan, Currency: currency}); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Service) IssueKey(ctx context.Context, accountID string, env apikey.Environment, label string) (string, error) {
	raw, parsed, err := apikey.Generate(env, s.random)
	if err != nil {
		return "", err
	}
	hash, salt, err := s.hasher.Hash(parsed.Secret)
	if err != nil {
		return "", err
	}
	err = s.store.InsertKey(ctx, out.NewAPIKey{
		StoredKey: out.StoredKey{ID: parsed.ID, AccountID: accountID, Env: env, Hash: hash, Salt: salt},
		Label:     label,
	})
	if err != nil {
		return "", err
	}
	return raw, nil
}

func (s *Service) RevokeKey(ctx context.Context, accountID, keyID string) error {
	return s.store.RevokeKey(ctx, accountID, keyID, s.clock.Now())
}

func (s *Service) Credit(ctx context.Context, accountID string, amount money.Amount) (out.Balance, error) {
	if amount <= 0 {
		return out.Balance{}, apperr.Validation([]apperr.FieldError{{Field: "amount", Message: "O crédito precisa ser positivo."}})
	}
	var balance out.Balance
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		var err error
		if balance, err = s.store.Credit(ctx, accountID, amount); err != nil {
			return err
		}
		return s.ledger.Append(ctx, out.LedgerEntry{
			AccountID: accountID, Kind: "credit", Amount: amount, OpID: s.ids.OpID(), BalanceAfter: balance.Available,
		})
	})
	return balance, err
}

func (s *Service) SetTariff(ctx context.Context, plan, mcc, mnc string, price money.Amount, validFrom time.Time) error {
	if len(mcc) != 3 || price < 0 {
		return apperr.Validation([]apperr.FieldError{{Field: "tariff", Message: "Use MCC de 3 dígitos e preço não negativo."}})
	}
	if validFrom.IsZero() {
		validFrom = s.clock.Now()
	}
	return s.store.InsertTariff(ctx, out.NewTariff{Plan: plan, MCC: mcc, MNC: mnc, Price: price, ValidFrom: validFrom})
}

func (s *Service) ApproveSender(ctx context.Context, accountID, sender, kind string, countries []string) error {
	sender = strings.TrimSpace(sender)
	if sender == "" || len([]rune(sender)) > 11 || len(countries) == 0 {
		return apperr.Validation([]apperr.FieldError{{Field: "sender", Message: "Use até 11 caracteres e ao menos um país."}})
	}
	switch kind {
	case "alphanumeric", "shortcode", "longcode":
	default:
		return fmt.Errorf("admin: tipo de remetente %q desconhecido", kind)
	}
	id := "snd_" + strings.ToLower(strings.TrimPrefix(s.ids.MessageID(), "msg_"))[:16]
	return s.store.ApproveSender(ctx, out.NewSender{ID: id, AccountID: accountID, Sender: sender, Kind: kind, Countries: countries})
}
