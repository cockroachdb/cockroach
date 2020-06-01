// Copyright 2016 The Cockroach Authors.
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
	"testing"
)

func TestLimiterBurstDisabled(t *testing.T) {
	limiter := NewLimiter(100)

	if err := limiter.WaitN(context.Background(), 101); err != nil {
		t.Fatal(err)
	}

	if err := limiter.WaitN(context.Background(), 100*burstFactor+100); err != nil {
		t.Fatal(err)
	}
}
