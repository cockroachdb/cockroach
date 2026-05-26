// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slidingwindow

import "time"

// NewMaxSwag returns a sliding window aggregator with a maximum aggregator.
func NewMaxSwag(now time.Time, interval time.Duration, size int) *Swag {
	return NewSwag(
		now,
		interval,
		size,
		func(acc, val float64) float64 {
			if acc > val {
				return acc
			}
			return val
		},
	)
}
