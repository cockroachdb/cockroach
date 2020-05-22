// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

func TestComputeStatsForKeySpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Create a number of ranges using splits.
	splitKeys := []string{"a", "c", "e", "g", "i"}
	for _, k := range splitKeys {
		key := roachpb.Key(k)
		repl := mtc.stores[0].LookupReplica(roachpb.RKey(key))
		args := adminSplitArgs(key)
		header := roachpb.Header{
			RangeID: repl.RangeID,
		}
		if _, err := kv.SendWrappedWith(context.Background(), mtc.stores[0], header, args); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for splits to finish.
	testutils.SucceedsSoon(t, func() error {
		repl := mtc.stores[0].LookupReplica(roachpb.RKey("z"))
		if actualRSpan := repl.Desc().RSpan(); !actualRSpan.Key.Equal(roachpb.RKey("i")) {
			return errors.Errorf("expected range %s to begin at key 'i'", repl)
		}
		return nil
	})

	// Create some keys across the ranges.
	incKeys := []string{"b", "bb", "bbb", "d", "dd", "h"}
	for _, k := range incKeys {
		if _, err := mtc.dbs[0].Inc(context.Background(), []byte(k), 5); err != nil {
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
		result, err := mtc.stores[0].ComputeStatsForKeySpan(
			roachpb.RKey(start), roachpb.RKey(end))
		if err != nil {
			t.Fatal(err)
		}
		if a, e := result.ReplicaCount, tcase.expectedRanges; a != e {
			t.Errorf("Expected %d ranges in span [%s - %s], found %d", e, start, end, a)
		}
		if a, e := result.MVCC.LiveCount, tcase.expectedKeys; a != e {
			t.Errorf("Expected %d keys in span [%s - %s], found %d", e, start, end, a)
		}
	}
}
