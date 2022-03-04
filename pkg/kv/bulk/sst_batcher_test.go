// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func TestAddBatched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("batch=default", func(t *testing.T) {
		runTestImport(t, 32<<20)
	})
	t.Run("batch=1", func(t *testing.T) {
		runTestImport(t, 1)
	})
}

func runTestImport(t *testing.T, batchSizeValue int64) {

	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	batchSize := func() int64 { return batchSizeValue }

	const split1, split2 = 3, 5

	// Each test case consists of some number of batches of keys, represented as
	// ints [0, 8). Splits are at 3 and 5.
	for i, testCase := range [][][]int{
		// Simple cases, no spanning splits, try first, last, middle, etc in each.
		// r1
		{{0}},
		{{1}},
		{{2}},
		{{0, 1, 2}},
		{{0}, {1}, {2}},

		// r2
		{{3}},
		{{4}},
		{{3, 4}},
		{{3}, {4}},

		// r3
		{{5}},
		{{5, 6, 7}},
		{{6}},

		// batches exactly matching spans.
		{{0, 1, 2}, {3, 4}, {5, 6, 7}},

		// every key, in its own batch.
		{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}},

		// every key in one big batch.
		{{0, 1, 2, 3, 4, 5, 6, 7}},

		// Look for off-by-ones on and around the splits.
		{{2, 3}},
		{{1, 3}},
		{{2, 4}},
		{{1, 4}},
		{{1, 5}},
		{{2, 5}},

		// Mixture of split-aligned and non-aligned batches.
		{{1}, {5}, {6}},
		{{1, 2, 3}, {4, 5}, {6, 7}},
		{{0}, {2, 3, 5}, {7}},
		{{0, 4}, {5, 7}},
		{{0, 3}, {4}},
	} {
		t.Run(fmt.Sprintf("%d-%v", i, testCase), func(t *testing.T) {
			prefix := keys.SystemSQLCodec.IndexPrefix(uint32(100+i), 1)
			key := func(i int) roachpb.Key {
				return encoding.EncodeStringAscending(append([]byte{}, prefix...), fmt.Sprintf("k%d", i))
			}

			if err := kvDB.AdminSplit(ctx, key(split1), hlc.MaxTimestamp /* expirationTime */); err != nil {
				t.Fatal(err)
			}
			if err := kvDB.AdminSplit(ctx, key(split2), hlc.MaxTimestamp /* expirationTime */); err != nil {
				t.Fatal(err)
			}

			// We want to make sure our range-aware batching knows about one of our
			// splits to exercise that codepath, but we also want to make sure we
			// still handle an unexpected split, so we make our own range cache and
			// only populate it with one of our two splits.
			mockCache := rangecache.NewRangeCache(s.ClusterSettings(), nil,
				func() int64 { return 2 << 10 }, s.Stopper(), s.TracerI().(*tracing.Tracer))
			addr, err := keys.Addr(key(0))
			if err != nil {
				t.Fatal(err)
			}
			tok, err := s.DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache().LookupWithEvictionToken(
				ctx, addr, rangecache.EvictionToken{}, false)
			if err != nil {
				t.Fatal(err)
			}
			r := roachpb.RangeInfo{
				Desc: *tok.Desc(),
			}
			mockCache.Insert(ctx, r)

			ts := hlc.Timestamp{WallTime: 100}
			b, err := bulk.MakeBulkAdder(
				ctx, kvDB, mockCache, s.ClusterSettings(), ts, kvserverbase.BulkAdderOptions{MinBufferSize: batchSize()}, nil, /* bulkMon */
			)
			if err != nil {
				t.Fatal(err)
			}

			defer b.Close(ctx)

			var expected []kv.KeyValue

			// Since the batcher automatically handles any retries due to spanning the
			// range-bounds internally, it can be difficult to observe from outside if
			// we correctly split on the first attempt to avoid those retires.
			// However we log an event when forced to retry (in case we need to debug)
			// slow requests or something, so we can inspect the trace in the test to
			// determine if requests required the expected number of retries.
			tr := s.TracerI().(*tracing.Tracer)
			addCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "add")
			defer getRecAndFinish()
			expectedSplitRetries := 0
			for _, batch := range testCase {
				for idx, x := range batch {
					k := key(x)
					// if our adds is batching multiple keys and we've previously added
					// a key prior to split2 and are now adding one after split2, then we
					// should expect this batch to span split2 and thus cause a retry.
					if batchSize() > 1 && idx > 0 && batch[idx-1] < split2 && batch[idx-1] >= split1 && batch[idx] >= split2 {
						expectedSplitRetries = 1
					}
					v := roachpb.MakeValueFromString(fmt.Sprintf("value-%d", x))
					v.Timestamp = ts
					v.InitChecksum(k)
					t.Logf("adding: %v", k)

					if err := b.Add(addCtx, k, v.RawBytes); err != nil {
						t.Fatal(err)
					}
					expected = append(expected, kv.KeyValue{Key: k, Value: &v})
				}
				if err := b.Flush(addCtx); err != nil {
					t.Fatal(err)
				}
			}
			var splitRetries int
			for _, sp := range getRecAndFinish() {
				splitRetries += tracing.CountLogMessages(sp, "SSTable cannot be added spanning range bounds")
			}
			if splitRetries != expectedSplitRetries {
				t.Fatalf("expected %d split-caused retries, got %d", expectedSplitRetries, splitRetries)
			}

			added := b.GetSummary()
			t.Logf("Wrote %d total", added.DataSize)

			got, err := kvDB.Scan(ctx, key(0), key(8), 0)
			if err != nil {
				t.Fatalf("%+v", err)
			}

			if !reflect.DeepEqual(got, expected) {
				for i := 0; i < len(got) || i < len(expected); i++ {
					if i < len(expected) {
						t.Logf("expected %d\t%v\t%v", i, expected[i].Key, expected[i].Value)
					}
					if i < len(got) {
						t.Logf("got      %d\t%v\t%v", i, got[i].Key, got[i].Value)
					}
				}
				t.Fatalf("got      %+v\nexpected %+v", got, expected)
			}
		})
	}
}
