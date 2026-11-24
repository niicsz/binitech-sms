package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/testsupport/pgtest"
)

func TestMarkOnce(t *testing.T) {
	pool := pgtest.New(t)
	ops := postgres.NewProcessedOps(pool)
	tx := postgres.NewTxManager(pool)
	ctx := context.Background()
	op := "1b6f0c9e-7d0e-4e1f-9a52-3c1f0c7b8a21"

	first, err := ops.MarkOnce(ctx, "status-engine", op)
	if err != nil || !first {
		t.Fatalf("primeira vez: %v %v", first, err)
	}
	again, _ := ops.MarkOnce(ctx, "status-engine", op)
	if again {
		t.Fatal("repetição não pode ser marcada de novo")
	}
	other, _ := ops.MarkOnce(ctx, "ledger-consumer", op)
	if !other {
		t.Fatal("outro consumidor tem a própria marcação")
	}

	op2 := "2b6f0c9e-7d0e-4e1f-9a52-3c1f0c7b8a21"
	_ = tx.WithinTx(ctx, func(ctx context.Context) error {
		_, _ = ops.MarkOnce(ctx, "status-engine", op2)
		return errors.New("efeito falhou")
	})
	retry, _ := ops.MarkOnce(ctx, "status-engine", op2)
	if !retry {
		t.Fatal("se o efeito desfez a transação, a marcação também precisa sumir")
	}
}
