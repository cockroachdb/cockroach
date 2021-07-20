// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"math/rand"
	"time"
)

// Jitter returns a jitter adjusted duration using fraction.
// The returned duration is a random duration between
// (1-jitter)*d and (1+jitter)*d
func Jitter(d time.Duration, fraction float64) time.Duration {
	jitterFraction := 1 + (2*rand.Float64()-1)*fraction
	return time.Duration(float64(d) * jitterFraction)
}
