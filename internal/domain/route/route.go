package route

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/niicsz/binitech-sms/internal/domain/money"
)

const SandboxID = "sandbox"

var ErrNoRoute = errors.New("route: nenhuma rota disponível para o destino")

type Route struct {
	ID       string
	Kind     string
	Covers   []string
	Cost     money.Amount
	Priority int
	Weight   int
	Enabled  bool
	Healthy  bool
}

type Choice struct {
	Route  Route
	Reason string
}

func (r Route) Serves(mcc, mnc string) bool {
	for _, pattern := range r.Covers {
		switch {
		case pattern == mcc+"*":
			return true
		case mnc != "" && pattern == mcc+mnc:
			return true
		}
	}
	return false
}

func Choose(routes []Route, mcc, mnc string, pick func(n int) int) (Choice, error) {
	var candidates []Route
	for _, r := range routes {
		if r.Enabled && r.Healthy && r.ID != SandboxID && r.Serves(mcc, mnc) {
			candidates = append(candidates, r)
		}
	}
	if len(candidates) == 0 {
		return Choice{}, ErrNoRoute
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority < candidates[j].Priority
		}
		return candidates[i].Cost < candidates[j].Cost
	})
	best := candidates[0]
	tied := candidates[:0:0]
	total := 0
	for _, r := range candidates {
		if r.Priority != best.Priority || r.Cost != best.Cost {
			break
		}
		r.Weight = max(r.Weight, 1)
		total += r.Weight
		tied = append(tied, r)
	}
	chosen := tied[0]
	if len(tied) > 1 {
		ticket := pick(total)
		for _, r := range tied {
			if ticket < r.Weight {
				chosen = r
				break
			}
			ticket -= r.Weight
		}
	}
	reason := fmt.Sprintf("lcr:priority=%d,cost=%s,candidates=%d", chosen.Priority, chosen.Cost, len(candidates))
	return Choice{Route: chosen, Reason: reason}, nil
}

func Sandbox() Route {
	return Route{ID: SandboxID, Kind: "sandbox", Enabled: true, Healthy: true}
}

func NormalizeDLRStatus(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "DELIVRD", "DELIVERED":
		return "delivered"
	case "EXPIRED":
		return "expired"
	case "ACCEPTD", "ENROUTE":
		return ""
	default:
		return "undelivered"
	}
}
