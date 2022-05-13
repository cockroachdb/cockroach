// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
