package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/apikey"
	"github.com/niicsz/binitech-sms/internal/domain/money"
)

type Accounts struct {
	pool *pgxpool.Pool
}

type Balances struct {
	pool *pgxpool.Pool
}

type Catalog struct {
	pool *pgxpool.Pool
}

var (
	_ out.AccountRepository = (*Accounts)(nil)
	_ out.APIKeyStore       = (*Accounts)(nil)
	_ out.BalanceRepository = (*Balances)(nil)
	_ out.LedgerRepository  = (*Balances)(nil)
	_ out.TariffRepository  = (*Catalog)(nil)
	_ out.SenderRepository  = (*Catalog)(nil)
)

func NewAccounts(pool *pgxpool.Pool) *Accounts {
	return &Accounts{pool: pool}
}

func NewBalances(pool *pgxpool.Pool) *Balances {
	return &Balances{pool: pool}
}

func NewCatalog(pool *pgxpool.Pool) *Catalog {
	return &Catalog{pool: pool}
}

func (r *Accounts) Get(ctx context.Context, id string) (out.Account, error) {
	var a out.Account
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT id, plan, status, currency, high_volume FROM accounts WHERE id = $1`, id,
	).Scan(&a.ID, &a.Plan, &a.Status, &a.Currency, &a.HighVolume)
	if errors.Is(err, pgx.ErrNoRows) {
		return out.Account{}, out.ErrNotFound
	}
	if err != nil {
		return out.Account{}, fmt.Errorf("accounts: %w", err)
	}
	return a, nil
}

func (r *Accounts) FindActive(ctx context.Context, id string) (out.StoredKey, error) {
	var (
		k   out.StoredKey
		env string
	)
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT k.id, k.account_id, k.environment, k.secret_hash, k.salt, a.status
		   FROM api_keys k JOIN accounts a ON a.id = k.account_id
		  WHERE k.id = $1 AND k.revoked_at IS NULL`, id,
	).Scan(&k.ID, &k.AccountID, &env, &k.Hash, &k.Salt, &k.AccountStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return out.StoredKey{}, out.ErrNotFound
	}
	if err != nil {
		return out.StoredKey{}, fmt.Errorf("api_keys: %w", err)
	}
	k.Env = apikey.Environment(env)
	return k, nil
}

func (r *Balances) Get(ctx context.Context, accountID string) (out.Balance, error) {
	var available, reserved string
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT available::text, reserved::text FROM balances WHERE account_id = $1`, accountID,
	).Scan(&available, &reserved)
	if errors.Is(err, pgx.ErrNoRows) {
		return out.Balance{}, out.ErrNotFound
	}
	if err != nil {
		return out.Balance{}, fmt.Errorf("balances: %w", err)
	}
	return parseBalance(available, reserved)
}

func (r *Balances) Reserve(ctx context.Context, accountID string, amount money.Amount) (out.Balance, error) {
	var available, reserved string
	err := conn(ctx, r.pool).QueryRow(ctx,
		`UPDATE balances
		    SET reserved = reserved + $2::numeric, version = version + 1, updated_at = now()
		  WHERE account_id = $1 AND available - reserved >= $2::numeric
		  RETURNING available::text, reserved::text`,
		accountID, amount.String(),
	).Scan(&available, &reserved)
	if errors.Is(err, pgx.ErrNoRows) {
		return out.Balance{}, out.ErrInsufficientBalance
	}
	if err != nil {
		return out.Balance{}, fmt.Errorf("balances: reserve: %w", err)
	}
	return parseBalance(available, reserved)
}

func (r *Balances) Append(ctx context.Context, e out.LedgerEntry) error {
	var messageID any
	if e.MessageID != "" {
		messageID = e.MessageID
	}
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO ledger_entries (account_id, kind, amount, message_id, op_id, balance_after)
		 VALUES ($1, $2, $3::numeric, $4, $5, $6::numeric)`,
		e.AccountID, e.Kind, e.Amount.String(), messageID, e.OpID, e.BalanceAfter.String())
	if err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	return nil
}

func (r *Catalog) UnitPrice(ctx context.Context, plan, mcc, mnc string, at time.Time) (money.Amount, error) {
	var price string
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT price::text FROM tariffs
		  WHERE plan = $1 AND mcc = $2 AND (mnc = $3 OR mnc IS NULL)
		    AND valid_from <= $4 AND (valid_to IS NULL OR valid_to > $4)
		  ORDER BY mnc IS NULL, valid_from DESC
		  LIMIT 1`,
		plan, mcc, mnc, at,
	).Scan(&price)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, out.ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("tariffs: %w", err)
	}
	return money.Parse(price)
}

func (r *Catalog) IsApproved(ctx context.Context, accountID, sender, country string) (bool, error) {
	var ok bool
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM alphanames
		  WHERE account_id = $1 AND sender = $2 AND status = 'approved' AND $3 = ANY(countries))`,
		accountID, sender, country,
	).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("alphanames: %w", err)
	}
	return ok, nil
}

func parseBalance(available, reserved string) (out.Balance, error) {
	a, err := money.Parse(available)
	if err != nil {
		return out.Balance{}, err
	}
	r, err := money.Parse(reserved)
	if err != nil {
		return out.Balance{}, err
	}
	return out.Balance{Available: a, Reserved: r}, nil
}
