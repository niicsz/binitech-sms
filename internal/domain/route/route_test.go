package route_test

import (
	"errors"
	"testing"

	"github.com/niicsz/binitech-sms/internal/domain/money"
	"github.com/niicsz/binitech-sms/internal/domain/route"
)

func rt(id string, priority int, cost string, weight int, covers ...string) route.Route {
	c, _ := money.Parse(cost)
	return route.Route{ID: id, Kind: "http", Covers: covers, Cost: c, Priority: priority, Weight: weight, Enabled: true, Healthy: true}
}

func first(int) int { return 0 }

func TestServes(t *testing.T) {
	country := route.Route{Covers: []string{"724*"}}
	if !country.Serves("724", "") || !country.Serves("724", "05") || country.Serves("722", "") {
		t.Fatal("padrão por país")
	}
	carrier := route.Route{Covers: []string{"72405"}}
	if !carrier.Serves("724", "05") || carrier.Serves("724", "06") || carrier.Serves("724", "") {
		t.Fatal("padrão por operadora")
	}
}

func TestChoosePriorityThenCost(t *testing.T) {
	routes := []route.Route{
		rt("cara", 100, "0.0500", 1, "724*"),
		rt("barata", 100, "0.0300", 1, "724*"),
		rt("reserva", 200, "0.0100", 1, "724*"),
	}
	got, err := route.Choose(routes, "724", "", first)
	if err != nil || got.Route.ID != "barata" {
		t.Fatalf("got %+v %v", got, err)
	}
}

func TestChooseSkipsUnhealthyDisabledAndOutOfCoverage(t *testing.T) {
	sick := rt("doente", 1, "0.0100", 1, "724*")
	sick.Healthy = false
	off := rt("desligada", 1, "0.0100", 1, "724*")
	off.Enabled = false
	other := rt("argentina", 1, "0.0100", 1, "722*")
	ok := rt("ok", 500, "0.0900", 1, "724*")
	got, err := route.Choose([]route.Route{sick, off, other, ok}, "724", "", first)
	if err != nil || got.Route.ID != "ok" {
		t.Fatalf("got %+v %v", got, err)
	}
}

func TestChooseNeverPicksSandboxForRealTraffic(t *testing.T) {
	sb := route.Sandbox()
	sb.Covers = []string{"724*"}
	if _, err := route.Choose([]route.Route{sb}, "724", "", first); !errors.Is(err, route.ErrNoRoute) {
		t.Fatalf("got %v", err)
	}
}

func TestChooseWeightedAmongTies(t *testing.T) {
	routes := []route.Route{rt("a", 1, "0.0300", 1, "724*"), rt("b", 1, "0.0300", 3, "724*")}
	counts := map[string]int{}
	for ticket := range 4 {
		got, _ := route.Choose(routes, "724", "", func(n int) int {
			if n != 4 {
				t.Fatalf("total de pesos = %d", n)
			}
			return ticket
		})
		counts[got.Route.ID]++
	}
	if counts["a"] != 1 || counts["b"] != 3 {
		t.Fatalf("distribuição por peso: %v", counts)
	}
}

func TestNormalizeDLRStatus(t *testing.T) {
	cases := map[string]string{"DELIVRD": "delivered", "delivered": "delivered", "EXPIRED": "expired", "UNDELIV": "undelivered", "REJECTD": "undelivered", "ENROUTE": ""}
	for in, want := range cases {
		if got := route.NormalizeDLRStatus(in); got != want {
			t.Fatalf("%s: got %q want %q", in, got, want)
		}
	}
}
