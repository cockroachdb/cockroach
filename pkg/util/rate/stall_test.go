// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rate

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestStallPrevention(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Construct a counter and add enough values to exceed the threshold.
	const threshold = 100
	var ctr Counter
	ctr.Add(threshold)

	// Construct a context that will be canceled once the counter
	// rate decays before the minimum rate.
	ctx, cancel := WithStallPrevention(context.Background(),
		OptionCheckPeriod(time.Second),
		OptionMaxFailures(2),
		OptionMinRate(NewRate(threshold-1, time.Second)),
		OptionRater(&ctr),
	)
	defer cancel()

	// Now, wait for the context to be canceled or time out.
	select {
	case <-ctx.Done():
		switch err := ctx.Err().(type) {
		case *StallDetectedError:
			// OK
		default:
			t.Error("unexpected ctx.Err()", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("context was not canceled in time")
	}
}
