package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

const consumer = "ledger-consumer"

type Consumer struct {
	tx    out.TxManager
	ops   out.ProcessedOps
	store out.LedgerStore
}

var _ in.EventHandler = (*Consumer)(nil)

func New(tx out.TxManager, ops out.ProcessedOps, store out.LedgerStore) *Consumer {
	return &Consumer{tx: tx, ops: ops, store: store}
}

func (c *Consumer) Handle(ctx context.Context, e in.Event) error {
	op := e.Headers[events.HeaderOperation]
	if op != events.OpLedgerDebit && op != events.OpLedgerRelease {
		return nil
	}
	opID := e.Headers[events.HeaderOpID]
	if opID == "" {
		return in.Permanent(errors.New("ledger: evento sem op_id"))
	}
	var req events.LedgerRequest
	if err := json.Unmarshal(e.Payload, &req); err != nil {
		return in.Permanent(fmt.Errorf("ledger: payload: %w", err))
	}
	if req.Amount <= 0 || req.OpID == "" {
		return in.Permanent(fmt.Errorf("ledger: lançamento inválido %+v", req))
	}
	return c.tx.WithinTx(ctx, func(ctx context.Context) error {
		first, err := c.ops.MarkOnce(ctx, consumer, opID)
		if err != nil || !first {
			return err
		}
		var balance out.Balance
		switch req.Kind {
		case "debit":
			balance, err = c.store.Debit(ctx, req.AccountID, req.Amount)
		case "release":
			balance, err = c.store.Release(ctx, req.AccountID, req.Amount)
		default:
			return in.Permanent(fmt.Errorf("ledger: tipo %q desconhecido", req.Kind))
		}
		if errors.Is(err, out.ErrLedgerInconsistent) {
			return in.Permanent(fmt.Errorf("conta %s, mensagem %s: %w", req.AccountID, req.MessageID, err))
		}
		if err != nil {
			return err
		}
		return c.store.Append(ctx, out.LedgerEntry{
			AccountID: req.AccountID, Kind: req.Kind, Amount: req.Amount, MessageID: req.MessageID,
			OpID: req.OpID, BalanceAfter: balance.Available,
		})
	})
}
