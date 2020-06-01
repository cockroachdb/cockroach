// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package limit

import (
	"context"
	"math"
	"testing"

	"golang.org/x/time/rate"
)

func TestLimiterBurstDisabled(t *testing.T) {
	limiter := NewLimiter(100)

	if err := limiter.WaitN(context.Background(), 101); err != nil {
		t.Fatal(err)
	}

	limiter = NewLimiter(rate.Limit(math.MaxFloat64))
	if limiter.limiter.Burst() != maxInt {
		t.Errorf("expected '%v', got '%v'", limiter.limiter.Burst(), maxInt)
	}
}
