// Package backoff provides backoff functionality
package backoff

import (
	"math"
	"math/rand"
	"time"
)

// Do is a function x^e multiplied by a factor of 0.1 second.
// Result is limited to 2 minute.
// Original source: github.com/micro/go-micro/v3/util/backoff/backoff.go
func Do(attempts int) time.Duration {
	if attempts > 13 {
		return 2 * time.Minute
	}

	return time.Duration(math.Pow(float64(attempts), math.E)) * time.Millisecond * 100
}

// Sleep where no data to sleep rnd * 2 + sec time.Duration
func Sleep(sec int) {
	rnd := rand.Intn(2)
	time.Sleep(Do(2*rnd + sec))
}
