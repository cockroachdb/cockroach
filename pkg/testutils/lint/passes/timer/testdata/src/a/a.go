// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import "github.com/cockroachdb/cockroach/pkg/util/timeutil"

func testReadCheck() {
	timer := timeutil.NewTimer()
	for {
		timer.Reset(0)
		select {
		case <-timer.C:
			timer.Read = true
		}
	}
	for {
		timer.Reset(0)
		select {
		case <-timer.C: // want `must set timer.Read = true after reading from timer.C \(see timeutil/timer.go\)`
		}
	}
}

func testNewCheck() {
	var t1 timeutil.Timer // want `use timeutil.NewTimer\(\) to pool timer allocations`
	t1.Stop()
	t2 := timeutil.Timer{} // want `use timeutil.NewTimer\(\) to pool timer allocations`
	t2.Stop()
	t3 := &timeutil.Timer{} // want `use timeutil.NewTimer\(\) to pool timer allocations`
	t3.Stop()
	t4 := new(timeutil.Timer) // want `use timeutil.NewTimer\(\) to pool timer allocations`
	t4.Stop()
	t5 := timeutil.NewTimer()
	t5.Stop()
	var t6 = timeutil.NewTimer()
	t6.Stop()
	var t7 *timeutil.Timer
	if true {
		t7 = timeutil.NewTimer()
	}
	t7.Stop()
}
