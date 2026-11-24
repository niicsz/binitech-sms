package in

import (
	"context"
	"errors"
	"time"
)

type Event struct {
	Topic     string
	Key       string
	Headers   map[string]string
	Payload   []byte
	Partition int32
	Offset    int64
	Timestamp time.Time
}

type EventHandler interface {
	Handle(ctx context.Context, event Event) error
}

type EventHandlerFunc func(ctx context.Context, event Event) error

func (f EventHandlerFunc) Handle(ctx context.Context, event Event) error {
	return f(ctx, event)
}

type permanentError struct {
	err error
}

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
