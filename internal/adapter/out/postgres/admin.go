package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/money"
)

type Admin struct {
	pool *pgxpool.Pool
}

var _ out.AdminStore = (*Admin)(nil)

func NewAdmin(pool *pgxpool.Pool) *Admin {
	return &Admin{pool: pool}
}

func (r *Admin) InsertAccount(ctx context.Context, a out.NewAccount) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO accounts (id, name, status, plan, currency) VALUES ($1, $2, 'active', $3, $4)`,
		a.ID, a.Name, a.Plan, a.Currency)
	if err != nil {
		return fmt.Errorf("admin: conta: %w", err)
	}
	return nil
}

func (r *Admin) InsertKey(ctx context.Context, k out.NewAPIKey) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO api_keys (id, account_id, environment, secret_hash, salt, label) VALUES ($1, $2, $3, $4, $5, $6)`,
		k.ID, k.AccountID, string(k.Env), k.Hash, k.Salt, nullable(k.Label))
	if err != nil {
		return fmt.Errorf("admin: chave: %w", err)
	}
	return nil
}

func (r *Admin) RevokeKey(ctx context.Context, accountID, keyID string, at time.Time) error {
	tag, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE api_keys SET revoked_at = $3 WHERE account_id = $1 AND id = $2 AND revoked_at IS NULL`,
		accountID, keyID, at)
	if err != nil {
		return fmt.Errorf("admin: revogar: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return out.ErrNotFound
	}
	return nil
}

func (r *Admin) Credit(ctx context.Context, accountID string, amount money.Amount) (out.Balance, error) {
	var available, reserved string
	err := conn(ctx, r.pool).QueryRow(ctx,
		`INSERT INTO balances (account_id, available) VALUES ($1, $2::numeric)
		 ON CONFLICT (account_id) DO UPDATE
		   SET available = balances.available + EXCLUDED.available, version = balances.version + 1, updated_at = now()
		 RETURNING available::text, reserved::text`,
		accountID, amount.String(),
	).Scan(&available, &reserved)
	if err != nil {
		return out.Balance{}, fmt.Errorf("admin: crédito: %w", err)
	}
	return parseBalance(available, reserved)
}

func (r *Admin) InsertTariff(ctx context.Context, t out.NewTariff) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO tariffs (plan, mcc, mnc, price, valid_from) VALUES ($1, $2, $3, $4::numeric, $5)`,
		t.Plan, t.MCC, nullable(t.MNC), t.Price.String(), t.ValidFrom)
	if err != nil {
		return fmt.Errorf("admin: tarifa: %w", err)
	}
	return nil
}

func (r *Admin) ApproveSender(ctx context.Context, s out.NewSender) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO alphanames (id, account_id, sender, kind, countries, status)
		 VALUES ($1, $2, $3, $4, $5, 'approved')
		 ON CONFLICT (account_id, sender) DO UPDATE
		   SET status = 'approved', kind = EXCLUDED.kind, countries = EXCLUDED.countries`,
		s.ID, s.AccountID, s.Sender, s.Kind, s.Countries)
	if err != nil {
		return fmt.Errorf("admin: remetente: %w", err)
	}
	return nil
}
