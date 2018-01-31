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
// permissions and limitations under the License.

package storage

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/kr/pretty"
)

// initialStats are the stats for a Replica which has been created through
// bootstrapRangeOnly. These stats are not empty because we call
// writeInitialState().
func initialStats() enginepb.MVCCStats {
	return enginepb.MVCCStats{
		SysBytes: 188,
		SysCount: 6,
	}
}
func TestRangeStatsEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	ms := tc.repl.GetMVCCStats()
	if exp := initialStats(); !reflect.DeepEqual(ms, exp) {
		t.Errorf("unexpected stats diff(exp, actual):\n%s", pretty.Diff(exp, ms))
	}
}

func TestRangeStatsInit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	ms := enginepb.MVCCStats{
		LiveBytes:       1,
		KeyBytes:        2,
		ValBytes:        3,
		IntentBytes:     4,
		LiveCount:       5,
		KeyCount:        6,
		ValCount:        7,
		IntentCount:     8,
		IntentAge:       9,
		GCBytesAge:      10,
		LastUpdateNanos: 11,
	}
	rsl := stateloader.Make(nil /* st */, tc.repl.RangeID)
	if err := rsl.SetMVCCStats(context.Background(), tc.engine, &ms); err != nil {
		t.Fatal(err)
	}
	loadMS, err := rsl.LoadMVCCStats(context.Background(), tc.engine)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ms, loadMS) {
		t.Errorf("mvcc stats mismatch %+v != %+v", ms, loadMS)
	}
}
