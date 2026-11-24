package campaign_test

import (
	"testing"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/campaign"
)

func at(hhmm string) time.Time {
	loc, _ := time.LoadLocation("America/Sao_Paulo")
	t, _ := time.ParseInLocation("2006-01-02 15:04", "2026-11-24 "+hhmm, loc)
	return t.UTC()
}

func TestInWindow(t *testing.T) {
	day := out.Campaign{WindowStart: "08:00", WindowEnd: "20:00", TimeZone: "America/Sao_Paulo"}
	night := out.Campaign{WindowStart: "22:00", WindowEnd: "02:00", TimeZone: "America/Sao_Paulo"}
	cases := []struct {
		c    out.Campaign
		at   string
		want bool
	}{
		{day, "07:59", false},
		{day, "08:00", true},
		{day, "19:59", true},
		{day, "20:00", false},
		{night, "23:30", true},
		{night, "01:00", true},
		{night, "12:00", false},
		{out.Campaign{}, "03:00", true},
	}
	for _, c := range cases {
		if got := campaign.InWindow(c.c, at(c.at)); got != c.want {
			t.Errorf("janela %s-%s às %s: got %v", c.c.WindowStart, c.c.WindowEnd, c.at, got)
		}
	}
}
