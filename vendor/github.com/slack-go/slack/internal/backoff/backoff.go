package backoff

import (
	"math/rand"
	"time"
)

// This one was ripped from https://github.com/jpillora/backoff/blob/master/backoff.go

// Backoff is a time.Duration counter. It starts at Min.  After every
// call to Duration() it is multiplied by Factor.  It is capped at
// Max. It returns to Min on every call to Reset().  Used in
// conjunction with the time package.
type Backoff struct {
	attempts int
	// Initial value to scale out
	Initial time.Duration
	// Jitter value randomizes an additional delay between 0 and Jitter
	Jitter time.Duration
	// Max maximum values of the backoff
	Max time.Duration
}

// Returns the current value of the counter and then multiplies it
// Factor
func (b *Backoff) Duration() (dur time.Duration) {
	// Zero-values are nonsensical, so we use
	// them to apply defaults
	if b.Max == 0 {
		b.Max = 10 * time.Second
	}

	if b.Initial == 0 {
		b.Initial = 100 * time.Millisecond
	}

	// calculate this duration
	if dur = time.Duration(1 << uint(b.attempts)); dur > 0 {
		dur = dur * b.Initial
	} else {
		dur = b.Max
	}

	if b.Jitter > 0 {
		dur = dur + time.Duration(rand.Intn(int(b.Jitter)))
	}

	// bump attempts count
	b.attempts++

	return dur
}

//Resets the current value of the counter back to Min
func (b *Backoff) Reset() {
	b.attempts = 0
}

// Attempts returns the number of attempts that we had done so far
func (b *Backoff) Attempts() int {
	return b.attempts
}
