package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/money"
)

var _ out.LedgerStore = (*Balances)(nil)

func (r *Balances) Debit(ctx context.Context, accountID string, amount money.Amount) (out.Balance, error) {
	return r.settle(ctx, accountID, amount, true)
}

func (r *Balances) Release(ctx context.Context, accountID string, amount money.Amount) (out.Balance, error) {
	return r.settle(ctx, accountID, amount, false)
}

func (r *Balances) settle(ctx context.Context, accountID string, amount money.Amount, debit bool) (out.Balance, error) {
	var available, reserved string
	err := conn(ctx, r.pool).QueryRow(ctx,
		`UPDATE balances
		    SET reserved = reserved - $2::numeric,
		        available = CASE WHEN $3 THEN available - $2::numeric ELSE available END,
		        version = version + 1, updated_at = now()
		  WHERE account_id = $1 AND reserved >= $2::numeric
		  RETURNING available::text, reserved::text`,
		accountID, amount.String(), debit,
	).Scan(&available, &reserved)
	if errors.Is(err, pgx.ErrNoRows) {
		return out.Balance{}, out.ErrLedgerInconsistent
	}
	if err != nil {
		return out.Balance{}, fmt.Errorf("balances: liquidar: %w", err)
	}
	return parseBalance(available, reserved)
}
