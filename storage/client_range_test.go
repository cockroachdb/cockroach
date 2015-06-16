// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package storage_test

import (
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestRangeCommandClockUpdate verifies that followers update their
// clocks when executing a command, even if the leader's clock is far
// in the future.
func TestRangeCommandClockUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)

	const numNodes = 3
	var manuals []*hlc.ManualClock
	var clocks []*hlc.Clock
	for i := 0; i < numNodes; i++ {
		manuals = append(manuals, hlc.NewManualClock(1))
		clocks = append(clocks, hlc.NewClock(manuals[i].UnixNano))
		clocks[i].SetMaxOffset(100 * time.Millisecond)
	}
	mtc := multiTestContext{
		clocks: clocks,
	}
	mtc.Start(t, numNodes)
	defer mtc.Stop()
	mtc.replicateRange(1, 0, 1, 2)

	// Advance the leader's clock ahead of the followers (by more than
	// MaxOffset but less than the leader lease) and execute a command.
	manuals[0].Increment(int64(500 * time.Millisecond))
	incArgs, incResp := incrementArgs([]byte("a"), 5, 1, mtc.stores[0].StoreID())
	incArgs.Timestamp = clocks[0].Now()
	if err := mtc.stores[0].ExecuteCmd(context.Background(), client.Call{Args: incArgs, Reply: incResp}); err != nil {
		t.Fatal(err)
	}

	// Wait for that command to execute on all the followers.
	util.SucceedsWithin(t, 50*time.Millisecond, func() error {
		values := []int64{}
		for _, eng := range mtc.engines {
			val, err := engine.MVCCGet(eng, proto.Key("a"), clocks[0].Now(), true, nil)
			if err != nil {
				return err
			}
			values = append(values, val.GetInteger())
		}
		if !reflect.DeepEqual(values, []int64{5, 5, 5}) {
			return util.Errorf("expected (5, 5, 5), got %v", values)
		}
		return nil
	})

	// Verify that all the followers have accepted the clock update from
	// node 0 even though it comes from outside the usual max offset.
	now := clocks[0].Now()
	for i, clock := range clocks {
		// Only compare the WallTimes: it's normal for clock 0 to be a few logical ticks ahead.
		if clock.Now().WallTime < now.WallTime {
			t.Errorf("clock %d is behind clock 0: %s vs %s", i, clock.Now(), now)
		}
	}
}

// TestRejectFutureCommand verifies that leaders reject commands that
// would cause a large time jump.
func TestRejectFutureCommand(t *testing.T) {
	defer leaktest.AfterTest(t)

	const maxOffset = 100 * time.Millisecond
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxOffset)
	mtc := multiTestContext{
		clock: clock,
	}
	mtc.Start(t, 1)
	defer mtc.Stop()

	// First do a write. The first write will advance the clock by MaxOffset
	// because of the read cache's low water mark.
	getArgs, getResp := putArgs([]byte("b"), []byte("b"), 1, mtc.stores[0].StoreID())
	if err := mtc.stores[0].ExecuteCmd(context.Background(), client.Call{Args: getArgs, Reply: getResp}); err != nil {
		t.Fatal(err)
	}
	if now := clock.Now(); now.WallTime != int64(maxOffset) {
		t.Fatalf("expected clock to advance to 100ms; got %s", now)
	}
	// The logical clock has advanced past the physical clock; increment
	// the "physical" clock to catch up.
	manual.Increment(int64(maxOffset))

	startTime := manual.UnixNano()

	// Commands with a future timestamp that is within the MaxOffset
	// bound will be accepted and will cause the clock to advance.
	for i := int64(0); i < 3; i++ {
		incArgs, incResp := incrementArgs([]byte("a"), 5, 1, mtc.stores[0].StoreID())
		incArgs.Timestamp.WallTime = startTime + ((i+1)*30)*int64(time.Millisecond)
		if err := mtc.stores[0].ExecuteCmd(context.Background(), client.Call{Args: incArgs, Reply: incResp}); err != nil {
			t.Fatal(err)
		}
	}
	if now := clock.Now(); now.WallTime != int64(190*time.Millisecond) {
		t.Fatalf("expected clock to advance to 190ms; got %s", now)
	}

	// Once the accumulated offset reaches MaxOffset, commands will be rejected.
	incArgs, incResp := incrementArgs([]byte("a"), 11, 1, mtc.stores[0].StoreID())
	incArgs.Timestamp.WallTime = int64((time.Duration(startTime) + maxOffset + 1) * time.Millisecond)
	if err := mtc.stores[0].ExecuteCmd(context.Background(), client.Call{Args: incArgs, Reply: incResp}); err == nil {
		t.Fatalf("expected clock offset error but got nil")
	}

	// The clock remained at 190ms and the final command was not executed.
	if now := clock.Now(); now.WallTime != int64(190*time.Millisecond) {
		t.Errorf("expected clock to advance to 190ms; got %s", now)
	}
	val, err := engine.MVCCGet(mtc.engines[0], proto.Key("a"), clock.Now(), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := val.GetInteger(); v != 15 {
		t.Errorf("expected 15, got %v", v)
	}
}
