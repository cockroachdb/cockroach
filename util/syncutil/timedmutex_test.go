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

package syncutil

import (
	"math"
	"testing"
	"time"

	"golang.org/x/net/context"
)

func TestTimedMutex(t *testing.T) {
	var msgs []string
	SetLogger(func(ctx context.Context, innerMsg string, args ...interface{}) {
		msgs = append(msgs, innerMsg)
	})

	{
		// Should fire.
		tm := MakeTimedMutex(context.Background(), time.Nanosecond)
		tm.Lock()
		time.Sleep(2 * time.Nanosecond)
		tm.Unlock()

		if len(msgs) != 1 {
			t.Fatalf("mutex did not warn: %+v", msgs)
		}
	}

	{
		tm := MakeTimedMutex(context.Background(), time.Duration(math.MaxInt64))
		tm.Lock()
		// Avoid staticcheck complaining about empty critical section.
		time.Sleep(time.Nanosecond)
		tm.Unlock()

		if len(msgs) != 1 {
			t.Fatalf("mutex warned erroneously: %+v", msgs[1:])
		}
	}
}
