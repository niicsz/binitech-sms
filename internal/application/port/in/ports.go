package in

import "context"

type RelayResult struct {
	Published int
}

type OutboxRelay interface {
	RelayOnce(ctx context.Context) (RelayResult, error)
}

type CheckResult struct {
	Name  string
	Error error
}

type Readiness interface {
	Check(ctx context.Context) []CheckResult
}
