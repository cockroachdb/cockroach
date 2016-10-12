// Copyright 2016 The Cockroach Authors.
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

package syncutil_test // because of log import

import (
	"fmt"
	"math"
	"regexp"
	"testing"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"

	"golang.org/x/net/context"
)

func TestTimedMutex(t *testing.T) {
	var msgs []string

	printf := func(ctx context.Context, innerMsg string, args ...interface{}) {
		formatted := fmt.Sprintf(innerMsg, args...)
		msgs = append(msgs, formatted)
	}
	var numMeasurements int
	record := func(time.Duration) { numMeasurements++ }

	{
		cb := syncutil.ThresholdLogger(
			context.Background(), time.Nanosecond, printf, record,
		)

		// Should fire.
		tm := syncutil.MakeTimedMutex(cb)
		tm.Lock()
		time.Sleep(2 * time.Nanosecond)
		tm.Unlock()

		re := regexp.MustCompile(`mutex held by .*TestTimedMutex for .* \(\>1ns\):`)
		if len(msgs) != 1 || !re.MatchString(msgs[0]) {
			t.Fatalf("mutex did not warn as expected: %+v", msgs)
		}
		if numMeasurements != 1 {
			t.Fatalf("expected one measurement, not %d", numMeasurements)
		}
	}

	numMeasurements = 0
	msgs = nil

	{
		cb := syncutil.ThresholdLogger(
			context.Background(), time.Duration(math.MaxInt64), printf, record,
		)
		tm := syncutil.MakeTimedMutex(cb)

		const num = 10
		for i := 0; i < num; i++ {
			tm.Lock()
			// Avoid staticcheck complaining about empty critical section.
			time.Sleep(time.Nanosecond)
			tm.Unlock()
		}

		if len(msgs) != 0 {
			t.Fatalf("mutex warned erroneously: %+v", msgs)
		}
		if numMeasurements != num {
			t.Fatalf("expected %d measurements not %d", num, numMeasurements)
		}

	}
}
