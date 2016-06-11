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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestRangeStatsEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()

	ms := tc.rng.GetMVCCStats()
	if !reflect.DeepEqual(ms, enginepb.MVCCStats{}) {
		t.Errorf("expected empty stats; got %+v", ms)
	}
}

func TestRangeStatsInit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
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
	if err := engine.MVCCSetRangeStats(context.Background(), tc.engine, 1, &ms); err != nil {
		t.Fatal(err)
	}
	loadMS, err := loadMVCCStats(tc.engine, tc.rng.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ms, loadMS) {
		t.Errorf("mvcc stats mismatch %+v != %+v", ms, loadMS)
	}
}
