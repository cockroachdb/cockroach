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
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/pkg/errors"
)

func TestComputeStatsForKeySpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// Create a number of ranges using splits.
	splitKeys := []string{"a", "c", "e", "g", "i"}
	for _, k := range splitKeys {
		key := []byte(k)
		repl := mtc.stores[0].LookupReplica(key, roachpb.RKeyMin)
		args := adminSplitArgs(key, key)
		header := roachpb.Header{
			RangeID: repl.RangeID,
		}
		if _, err := client.SendWrappedWith(mtc.stores[0], nil, header, &args); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for splits to finish.
	util.SucceedsSoon(t, func() error {
		repl := mtc.stores[0].LookupReplica(roachpb.RKey("z"), nil)
		if actualRSpan := repl.Desc().RSpan(); !actualRSpan.Key.Equal(roachpb.RKey("i")) {
			return errors.Errorf("expected range %s to begin at key 'i'", repl)
		}
		return nil
	})

	// Create some keys across the ranges.
	incKeys := []string{"b", "bb", "bbb", "d", "dd", "h"}
	for _, k := range incKeys {
		if _, err := mtc.dbs[0].Inc([]byte(k), 5); err != nil {
			t.Fatal(err)
		}
	}

	// Verify stats across different spans.
	for _, tcase := range []struct {
		startKey       string
		endKey         string
		expectedRanges int
		expectedKeys   int64
	}{
		{"a", "i", 4, 6},
		{"a", "c", 1, 3},
		{"b", "e", 2, 5},
		{"e", "i", 2, 1},
	} {
		start, end := tcase.startKey, tcase.endKey
		stats, count := mtc.stores[0].ComputeStatsForKeySpan(
			roachpb.RKey(start), roachpb.RKey(end))
		if a, e := count, tcase.expectedRanges; a != e {
			t.Errorf("Expected %d ranges in span [%s - %s], found %d", e, start, end, a)
		}
		if a, e := stats.LiveCount, tcase.expectedKeys; a != e {
			t.Errorf("Expected %d keys in span [%s - %s], found %d", e, start, end, a)
		}
	}
}
