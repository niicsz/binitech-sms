package apperr

import (
	"errors"
	"time"
)

type Kind int

const (
	Internal Kind = iota
	Invalid
	Unauthorized
	InsufficientBalance
	Forbidden
	NotFound
	Conflict
	RateLimited
)

type FieldError struct {
	Field   string
	Message string
}

type Error struct {
	Kind       Kind
	Message    string
	Fields     []FieldError
	RetryAfter time.Duration
	Cause      error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Cause }

func New(kind Kind, message string) *Error {
	return &Error{Kind: kind, Message: message}
}

func Validation(fields []FieldError) *Error {
	return &Error{Kind: Invalid, Message: "Requisição inválida.", Fields: fields}
}

func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return Internal
}
