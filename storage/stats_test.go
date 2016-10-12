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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// initialStats are the stats for a Replica which has been created through
// bootstrapRangeOnly. These stats are not empty because we call
// writeInitialState().
func initialStats() enginepb.MVCCStats {
	return enginepb.MVCCStats{
		SysBytes: 183,
		SysCount: 6,
	}
}
func TestRangeStatsEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()

	ms := tc.rng.GetMVCCStats()
	if exp := initialStats(); !reflect.DeepEqual(ms, exp) {
		t.Errorf("expected stats %+v; got %+v", exp, ms)
	}
}
