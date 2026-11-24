package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/testsupport/pgtest"
)

func TestOutboxClaimAndMark(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()
	tx := postgres.NewTxManager(pool)
	repo := postgres.NewOutboxRepository(pool)

	for _, key := range []string{"acc_1", "acc_2"} {
		if _, err := repo.Enqueue(ctx, events.Envelope{
			Topic: "sms.submit.test", Key: key,
			Headers: map[string]string{events.HeaderSchema: events.SchemaV1},
			Payload: []byte(`{"ok":true}`),
		}); err != nil {
			t.Fatal(err)
		}
	}

	var claimed int
	err := tx.WithinTx(ctx, func(ctx context.Context) error {
		records, err := repo.ClaimPending(ctx, 0, 10)
		if err != nil {
			return err
		}
		claimed = len(records)
		if records[0].Envelope.Headers[events.HeaderSchema] != events.SchemaV1 || string(records[0].Envelope.Payload) != `{"ok":true}` {
			t.Errorf("registro lido diferente do gravado: %+v", records[0])
		}
		ids := []int64{records[0].ID, records[1].ID}
		return repo.MarkPublished(ctx, ids, time.Now())
	})
	if err != nil || claimed != 2 {
		t.Fatalf("claimed=%d err=%v", claimed, err)
	}

	err = tx.WithinTx(ctx, func(ctx context.Context) error {
		records, err := repo.ClaimPending(ctx, 0, 10)
		if len(records) != 0 {
			t.Errorf("publicados não deveriam voltar: %d", len(records))
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOutboxGraceAndSkipLocked(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()
	tx := postgres.NewTxManager(pool)
	repo := postgres.NewOutboxRepository(pool)
	if _, err := repo.Enqueue(ctx, events.Envelope{Topic: "t", Key: "k", Headers: map[string]string{}, Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	_ = tx.WithinTx(ctx, func(ctx context.Context) error {
		records, _ := repo.ClaimPending(ctx, time.Hour, 10)
		if len(records) != 0 {
			t.Errorf("registro recente não deveria ser pego dentro da carência")
		}
		return nil
	})

	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- tx.WithinTx(ctx, func(ctx context.Context) error {
			records, err := repo.ClaimPending(ctx, 0, 10)
			if err != nil || len(records) != 1 {
				return errors.Join(err, errors.New("primeiro relay deveria pegar 1"))
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	_ = tx.WithinTx(ctx, func(ctx context.Context) error {
		records, _ := repo.ClaimPending(ctx, 0, 10)
		if len(records) != 0 {
			t.Errorf("segundo relay pegou linha travada: %d", len(records))
		}
		return nil
	})
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTxRollsBackOnError(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()
	tx := postgres.NewTxManager(pool)
	repo := postgres.NewOutboxRepository(pool)

	boom := errors.New("falhou depois de gravar")
	err := tx.WithinTx(ctx, func(ctx context.Context) error {
		if _, err := repo.Enqueue(ctx, events.Envelope{Topic: "t", Key: "k", Headers: map[string]string{}, Payload: []byte("x")}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&count); err != nil || count != 0 {
		t.Fatalf("rollback falhou: count=%d err=%v", count, err)
	}
}
