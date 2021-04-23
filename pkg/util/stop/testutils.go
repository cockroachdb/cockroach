// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stop

import (
	"runtime/debug"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

func init() {
	leaktest.PrintLeakedStoppers = PrintLeakedStoppers
}

// PrintLeakedStoppers prints (using `t`) the creation site of each Stopper
// for which `.Stop()` has not yet been called.
func PrintLeakedStoppers(t testing.TB) {
	trackedStoppers.Lock()
	defer trackedStoppers.Unlock()
	for _, tracked := range trackedStoppers.stoppers {
		t.Errorf("leaked stopper, created at:\n%s", tracked.createdAt)
	}
}

var trackedStoppers struct {
	syncutil.Mutex
	stoppers []stopperWithStack
}

func register(s *Stopper) {
	trackedStoppers.Lock()
	trackedStoppers.stoppers = append(trackedStoppers.stoppers,
		stopperWithStack{s: s, createdAt: string(debug.Stack())})
	trackedStoppers.Unlock()
}

func unregister(s *Stopper) {
	trackedStoppers.Lock()
	defer trackedStoppers.Unlock()
	sl := trackedStoppers.stoppers
	for i, tracked := range sl {
		if tracked.s == s {
			trackedStoppers.stoppers = sl[:i+copy(sl[i:], sl[i+1:])]
			return
		}
	}
	panic(errors.AssertionFailedf("attempt to unregister untracked stopper"))
}
