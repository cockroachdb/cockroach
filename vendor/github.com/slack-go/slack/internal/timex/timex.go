package timex

import "time"

// Max returns the maximum duration
func Max(values ...time.Duration) time.Duration {
	var (
		max time.Duration
	)

	for _, v := range values {
		if v > max {
			max = v
		}
	}

	return max
}
