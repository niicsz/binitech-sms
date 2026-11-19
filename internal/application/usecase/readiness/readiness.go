package readiness

import (
	"context"
	"sync"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

type Service struct {
	checkers []out.HealthChecker
	timeout  time.Duration
}

var _ in.Readiness = (*Service)(nil)

func New(timeout time.Duration, checkers ...out.HealthChecker) *Service {
	return &Service{checkers: checkers, timeout: timeout}
}

func (s *Service) Check(ctx context.Context) []in.CheckResult {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	results := make([]in.CheckResult, len(s.checkers))
	var wg sync.WaitGroup
	for i, checker := range s.checkers {
		wg.Go(func() {
			results[i] = in.CheckResult{Name: checker.Name(), Error: checker.Check(ctx)}
		})
	}
	wg.Wait()
	return results
}
