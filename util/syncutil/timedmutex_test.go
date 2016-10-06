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

	_ "github.com/cockroachdb/cockroach/util/log" // for flags
	"github.com/cockroachdb/cockroach/util/syncutil"

	"golang.org/x/net/context"
)

func TestTimedMutex(t *testing.T) {
	var msgs []string

	print := func(ctx context.Context, innerMsg string, args ...interface{}) {
		formatted := fmt.Sprintf(innerMsg, args...)
		msgs = append(msgs, formatted)
	}
	measure := func(time.Duration) {}

	{
		cb := syncutil.ThresholdLogger(
			context.Background(), time.Nanosecond, print, measure,
		)

		// Should fire.
		tm := syncutil.MakeTimedMutex(context.Background(), cb)
		tm.Lock()
		time.Sleep(2 * time.Nanosecond)
		tm.Unlock()

		re := regexp.MustCompile(`mutex held by .*TestTimedMutex for .* \(\>1ns\):`)
		if len(msgs) != 1 || !re.MatchString(msgs[0]) {
			t.Fatalf("mutex did not warn as expected: %+v", msgs)
		}
	}

	{
		cb := syncutil.ThresholdLogger(
			context.Background(), time.Duration(math.MaxInt64), print, measure,
		)
		tm := syncutil.MakeTimedMutex(context.Background(), cb)
		tm.Lock()
		// Avoid staticcheck complaining about empty critical section.
		time.Sleep(time.Nanosecond)
		tm.Unlock()

		if len(msgs) != 1 {
			t.Fatalf("mutex warned erroneously: %+v", msgs[1:])
		}
	}
}
