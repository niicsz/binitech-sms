package ratelimit

import "time"

func SetClock(g *GCRA, now func() time.Time) {
	g.now = now
}
