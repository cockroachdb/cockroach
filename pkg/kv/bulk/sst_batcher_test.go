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
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func makeIntTableKVs(numKeys, valueSize, maxRevisions int) []storage.MVCCKeyValue {
	prefix := keys.SystemSQLCodec.IndexPrefix(100, 1)
	kvs := make([]storage.MVCCKeyValue, numKeys)
	r, _ := randutil.NewPseudoRand()

	var k int
	for i := 0; i < numKeys; {
		k += 1 + rand.Intn(100)
		key := encoding.EncodeVarintAscending(append([]byte{}, prefix...), int64(k))
		buf := make([]byte, valueSize)
		randutil.ReadTestdataBytes(r, buf)
		revisions := 1 + r.Intn(maxRevisions)

		ts := int64(maxRevisions * 100)
		for j := 0; j < revisions && i < numKeys; j++ {
			ts -= 1 + r.Int63n(99)
			kvs[i].Key.Key = key
			kvs[i].Key.Timestamp.WallTime = ts
			kvs[i].Key.Timestamp.Logical = r.Int31()
			kvs[i].Value = roachpb.MakeValueFromString(string(buf)).RawBytes
			i++
		}
	}
	return kvs
}

func makePebbleSST(t testing.TB, kvs []storage.MVCCKeyValue) []byte {
	memFile := &storage.MemFile{}
	w := storage.MakeIngestionSSTWriter(memFile)
	defer w.Close()

	for i := range kvs {
		if err := w.Put(kvs[i].Key, kvs[i].Value); err != nil {
			t.Fatal(err)
		}
	}
	require.NoError(t, w.Finish())
	return memFile.Data()
}

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
			mockCache := rangecache.NewRangeCache(s.ClusterSettings(), nil, func() int64 { return 2 << 10 }, s.Stopper())
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
				ctx, kvDB, mockCache, s.ClusterSettings(), ts, kvserverbase.BulkAdderOptions{MinBufferSize: batchSize(), SSTSize: batchSize}, nil, /* bulkMon */
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
			tr := s.Tracer().(*tracing.Tracer)
			addCtx, getRec, cancel := tracing.ContextWithRecordingSpan(ctx, tr, "add")
			defer cancel()
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
			for _, rec := range getRec() {
				for _, l := range rec.Logs {
					for _, line := range l.Fields {
						if strings.Contains(line.Value, "SSTable cannot be added spanning range bounds") {
							splitRetries++
						}
					}
				}
			}
			if splitRetries != expectedSplitRetries {
				t.Fatalf("expected %d split-caused retries, got %d", expectedSplitRetries, splitRetries)
			}
			cancel()

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

type mockSender func(span roachpb.Span) error

func (m mockSender) AddSSTable(
	ctx context.Context,
	begin, end interface{},
	data []byte,
	disallowShadowing bool,
	_ *enginepb.MVCCStats,
	ingestAsWrites bool,
	batchTS hlc.Timestamp,
) error {
	return m(roachpb.Span{Key: begin.(roachpb.Key), EndKey: end.(roachpb.Key)})
}

func (m mockSender) SplitAndScatter(ctx context.Context, _ roachpb.Key, _ hlc.Timestamp) error {
	return nil
}

// TestAddBigSpanningSSTWithSplits tests a situation where a large
// spanning SST is being ingested over a span with a lot of splits.
func TestAddBigSpanningSSTWithSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	skip.UnderShort(t, "this test needs to do a larger SST to see the quadratic mem usage on retries kick in.")

	const numKeys, valueSize, splitEvery = 500, 5000, 1

	// Make some KVs and grab [start,end). Generate one extra for exclusive `end`.
	kvs := makeIntTableKVs(numKeys+1, valueSize, 1)
	start, end := kvs[0].Key.Key, kvs[numKeys].Key.Key
	kvs = kvs[:numKeys]

	// Create a large SST.
	sst := makePebbleSST(t, kvs)

	var splits []roachpb.Key
	for i := range kvs {
		if i%splitEvery == 0 {
			splits = append(splits, kvs[i].Key.Key)
		}
	}

	// Keep track of the memory.
	getMem := func() uint64 {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		return stats.HeapInuse
	}
	var early, late uint64
	var totalAdditionAttempts int
	mock := mockSender(func(span roachpb.Span) error {
		totalAdditionAttempts++
		for i := range splits {
			if span.ContainsKey(splits[i]) && !span.Key.Equal(splits[i]) {
				earlySplit := numKeys / 100
				if i == earlySplit {
					early = getMem()
				} else if i == len(splits)-earlySplit {
					late = getMem()
				}
				return roachpb.NewRangeKeyMismatchError(
					ctx, span.Key, span.EndKey,
					&roachpb.RangeDescriptor{EndKey: roachpb.RKey(splits[i])}, nil /* lease */)
			}
		}
		return nil
	})

	const kb = 1 << 10

	t.Logf("Adding %dkb sst spanning %d splits from %v to %v", len(sst)/kb, len(splits), start, end)
	if _, err := bulk.AddSSTable(
		ctx, mock, start, end, sst, false /* disallowShadowing */, enginepb.MVCCStats{}, cluster.MakeTestingClusterSettings(), hlc.Timestamp{},
	); err != nil {
		t.Fatal(err)
	}
	t.Logf("Adding took %d total attempts", totalAdditionAttempts)
	if late > early*8 {
		t.Fatalf("Mem usage grew from %dkb before grew to %dkb later (%.2fx)",
			early/kb, late/kb, float64(late)/float64(early))
	}
}
