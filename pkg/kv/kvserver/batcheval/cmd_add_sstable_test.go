// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval_test

import (
	"bytes"
	"context"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// createTestPebbleEngine returns a new in-memory Pebble storage engine.
func createTestPebbleEngine() storage.Engine {
	return storage.NewInMemForTesting(context.Background(), roachpb.Attributes{}, 1<<20)
}

var engineImpls = []struct {
	name   string
	create func() storage.Engine
}{
	{"pebble", createTestPebbleEngine},
}

func singleKVSSTable(key storage.MVCCKey, value []byte) ([]byte, error) {
	sstFile := &storage.MemFile{}
	sst := storage.MakeBackupSSTWriter(sstFile)
	defer sst.Close()
	if err := sst.Put(key, value); err != nil {
		return nil, err
	}
	if err := sst.Finish(); err != nil {
		return nil, err
	}
	return sstFile.Data(), nil
}

func TestDBAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	t.Run("store=in-memory", func(t *testing.T) {
		s, _, db := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
		ctx := context.Background()
		defer s.Stopper().Stop(ctx)

		tr := s.ClusterSettings().Tracer
		runTestDBAddSSTable(ctx, t, db, tr, nil)
	})
	t.Run("store=on-disk", func(t *testing.T) {
		dir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()

		storeSpec := base.DefaultTestStoreSpec
		storeSpec.InMemory = false
		storeSpec.Path = dir
		s, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Insecure:   true,
			StoreSpecs: []base.StoreSpec{storeSpec},
		})
		ctx := context.Background()
		defer s.Stopper().Stop(ctx)
		store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}

		tr := s.ClusterSettings().Tracer
		runTestDBAddSSTable(ctx, t, db, tr, store)
	})
}

const ingestAsWrites, ingestAsSST = true, false

var nilStats *enginepb.MVCCStats

// if store != nil, assume it is on-disk and check ingestion semantics.
func runTestDBAddSSTable(
	ctx context.Context, t *testing.T, db *kv.DB, tr *tracing.Tracer, store *kvserver.Store,
) {
	tr.TestingRecordAsyncSpans() // we assert on async span traces in this test
	{
		key := storage.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 2}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("1").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		// Key is before the range in the request span.
		if err := db.AddSSTable(
			ctx, "d", "e", data, false /* disallowShadowing */, nilStats, ingestAsSST, hlc.Timestamp{},
		); !testutils.IsError(err, "not in request range") {
			t.Fatalf("expected request range error got: %+v", err)
		}
		// Key is after the range in the request span.
		if err := db.AddSSTable(
			ctx, "a", "b", data, false /* disallowShadowing */, nilStats, ingestAsSST, hlc.Timestamp{},
		); !testutils.IsError(err, "not in request range") {
			t.Fatalf("expected request range error got: %+v", err)
		}

		// Do an initial ingest.
		ingestCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
		defer cancel()
		if err := db.AddSSTable(
			ingestCtx, "b", "c", data, false /* disallowShadowing */, nilStats, ingestAsSST, hlc.Timestamp{},
		); err != nil {
			t.Fatalf("%+v", err)
		}
		formatted := collect().String()
		if err := testutils.MatchEach(formatted,
			"evaluating AddSSTable",
			"sideloadable proposal detected",
			"ingested SSTable at index",
		); err != nil {
			t.Fatal(err)
		}

		if store != nil {
			// Look for the ingested path and verify it still exists.
			re := regexp.MustCompile(`ingested SSTable at index \d+, term \d+: (\S+)`)
			match := re.FindStringSubmatch(formatted)
			if len(match) != 2 {
				t.Fatalf("failed to extract ingested path from message %q,\n got: %v", formatted, match)
			}
			// The on-disk paths have `.ingested` appended unlike in-memory.
			suffix := ".ingested"
			if _, err := os.Stat(strings.TrimSuffix(match[1], suffix)); err != nil {
				t.Fatalf("%q file missing after ingest: %+v", match[1], err)
			}
		}
		if r, err := db.Get(ctx, "bb"); err != nil {
			t.Fatalf("%+v", err)
		} else if expected := []byte("1"); !bytes.Equal(expected, r.ValueBytes()) {
			t.Errorf("expected %q, got %q", expected, r.ValueBytes())
		}
	}

	// Check that ingesting a key with an earlier mvcc timestamp doesn't affect
	// the value returned by Get.
	{
		key := storage.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("2").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if err := db.AddSSTable(
			ctx, "b", "c", data, false /* disallowShadowing */, nilStats, ingestAsSST, hlc.Timestamp{},
		); err != nil {
			t.Fatalf("%+v", err)
		}
		if r, err := db.Get(ctx, "bb"); err != nil {
			t.Fatalf("%+v", err)
		} else if expected := []byte("1"); !bytes.Equal(expected, r.ValueBytes()) {
			t.Errorf("expected %q, got %q", expected, r.ValueBytes())
		}
		if store != nil {
			metrics := store.Metrics()
			if expected, got := int64(2), metrics.AddSSTableApplications.Count(); expected != got {
				t.Fatalf("expected %d sst ingestions, got %d", expected, got)
			}
		}
	}

	// Key range in request span is not empty. First time through a different
	// key is present. Second time through checks the idempotency.
	{
		key := storage.MVCCKey{Key: []byte("bc"), Timestamp: hlc.Timestamp{WallTime: 1}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("3").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		var metrics *kvserver.StoreMetrics
		var before int64
		if store != nil {
			metrics = store.Metrics()
			before = metrics.AddSSTableApplicationCopies.Count()
		}
		for i := 0; i < 2; i++ {
			ingestCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
			defer cancel()

			if err := db.AddSSTable(
				ingestCtx, "b", "c", data, false /* disallowShadowing */, nilStats, ingestAsSST, hlc.Timestamp{},
			); err != nil {
				t.Fatalf("%+v", err)
			}
			if err := testutils.MatchEach(collect().String(),
				"evaluating AddSSTable",
				"sideloadable proposal detected",
				"ingested SSTable at index",
			); err != nil {
				t.Fatal(err)
			}

			if r, err := db.Get(ctx, "bb"); err != nil {
				t.Fatalf("%+v", err)
			} else if expected := []byte("1"); !bytes.Equal(expected, r.ValueBytes()) {
				t.Errorf("expected %q, got %q", expected, r.ValueBytes())
			}
			if r, err := db.Get(ctx, "bc"); err != nil {
				t.Fatalf("%+v", err)
			} else if expected := []byte("3"); !bytes.Equal(expected, r.ValueBytes()) {
				t.Errorf("expected %q, got %q", expected, r.ValueBytes())
			}
		}
		if store != nil {
			if expected, got := int64(4), metrics.AddSSTableApplications.Count(); expected != got {
				t.Fatalf("expected %d sst ingestions, got %d", expected, got)
			}
			// The second time though we had to make a copy of the SST since rocks saw
			// existing data (from the first time), and rejected the no-modification
			// attempt.
			if after := metrics.AddSSTableApplicationCopies.Count(); before != after {
				t.Fatalf("expected sst copies not to increase, %d before %d after", before, after)
			}
		}
	}

	// ... and doing the same thing but via write-batch works the same.
	{
		key := storage.MVCCKey{Key: []byte("bd"), Timestamp: hlc.Timestamp{WallTime: 1}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("3").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		var metrics *kvserver.StoreMetrics
		var before int64
		if store != nil {
			metrics = store.Metrics()
			before = metrics.AddSSTableApplications.Count()
		}
		for i := 0; i < 2; i++ {
			ingestCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
			defer cancel()

			if err := db.AddSSTable(
				ingestCtx, "b", "c", data, false /* disallowShadowing */, nilStats, ingestAsWrites, hlc.Timestamp{},
			); err != nil {
				t.Fatalf("%+v", err)
			}
			if err := testutils.MatchEach(collect().String(),
				"evaluating AddSSTable",
				"via regular write batch",
			); err != nil {
				t.Fatal(err)
			}

			if r, err := db.Get(ctx, "bb"); err != nil {
				t.Fatalf("%+v", err)
			} else if expected := []byte("1"); !bytes.Equal(expected, r.ValueBytes()) {
				t.Errorf("expected %q, got %q", expected, r.ValueBytes())
			}
			if r, err := db.Get(ctx, "bd"); err != nil {
				t.Fatalf("%+v", err)
			} else if expected := []byte("3"); !bytes.Equal(expected, r.ValueBytes()) {
				t.Errorf("expected %q, got %q", expected, r.ValueBytes())
			}
		}
		if store != nil {
			if expected, got := before, metrics.AddSSTableApplications.Count(); expected != got {
				t.Fatalf("expected %d sst ingestions, got %d", expected, got)
			}
		}
	}

	// Invalid key/value entry checksum.
	{
		key := storage.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		value := roachpb.MakeValueFromString("1")
		value.InitChecksum([]byte("foo"))
		data, err := singleKVSSTable(key, value.RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if err := db.AddSSTable(
			ctx, "b", "c", data, false /* disallowShadowing */, nilStats, ingestAsSST, hlc.Timestamp{},
		); !testutils.IsError(err, "invalid checksum") {
			t.Fatalf("expected 'invalid checksum' error got: %+v", err)
		}
	}
}

type strKv struct {
	k  string
	ts int64
	v  string
}

func mvccKVsFromStrs(in []strKv) []storage.MVCCKeyValue {
	kvs := make([]storage.MVCCKeyValue, len(in))
	for i := range kvs {
		kvs[i].Key.Key = []byte(in[i].k)
		kvs[i].Key.Timestamp.WallTime = in[i].ts
		if in[i].v != "" {
			kvs[i].Value = roachpb.MakeValueFromBytes([]byte(in[i].v)).RawBytes
		} else {
			kvs[i].Value = nil
		}
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key.Less(kvs[j].Key) })
	return kvs
}

func TestAddSSTableMVCCStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range engineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			for _, kv := range mvccKVsFromStrs([]strKv{
				{"A", 1, "A"},
				{"a", 1, "a"},
				{"a", 6, ""},
				{"b", 5, "bb"},
				{"c", 6, "ccccccccccccccccccccccccccccccccccccccccccccc"}, // key 4b, 50b, live 64b
				{"d", 1, "d"},
				{"d", 2, ""},
				{"e", 1, "e"},
				{"z", 2, "zzzzzz"},
			}) {
				if err := e.PutMVCC(kv.Key, kv.Value); err != nil {
					t.Fatalf("%+v", err)
				}
			}

			sstKVs := mvccKVsFromStrs([]strKv{
				{"a", 2, "aa"},     // mvcc-shadowed within SST.
				{"a", 4, "aaaaaa"}, // mvcc-shadowed by existing delete.
				{"c", 6, "ccc"},    // same TS as existing, LSM-shadows existing.
				{"d", 4, "dddd"},   // mvcc-shadow existing deleted d.
				{"e", 4, "eeee"},   // mvcc-shadow existing 1b.
				{"j", 2, "jj"},     // no colission – via MVCC or LSM – with existing.
			})
			var delta enginepb.MVCCStats
			// the sst will think it added 4 keys here, but a, c, and e shadow or are shadowed.
			delta.LiveCount = -3
			delta.LiveBytes = -109
			// the sst will think it added 5 keys, but only j is new so 4 are over-counted.
			delta.KeyCount = -4
			delta.KeyBytes = -20
			// the sst will think it added 6 values, but since one was a perfect (key+ts)
			// collision, it *replaced* the existing value and is over-counted.
			delta.ValCount = -1
			delta.ValBytes = -50

			// Add in a random metadata key.
			ts := hlc.Timestamp{WallTime: 7}
			txn := roachpb.MakeTransaction(
				"test",
				nil, // baseKey
				roachpb.NormalUserPriority,
				ts,
				base.DefaultMaxClockOffset.Nanoseconds(),
			)
			if err := storage.MVCCPut(
				ctx, e, nil, []byte("i"), ts,
				roachpb.MakeValueFromBytes([]byte("it")),
				&txn,
			); err != nil {
				if !errors.HasType(err, (*roachpb.WriteIntentError)(nil)) {
					t.Fatalf("%+v", err)
				}
			}

			// After EvalAddSSTable, cArgs.Stats contains a diff to the existing
			// stats. Make sure recomputing from scratch gets the same answer as
			// applying the diff to the stats
			beforeStats := func() enginepb.MVCCStats {
				iter := e.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
				defer iter.Close()
				beforeStats, err := storage.ComputeStatsForRange(iter, keys.LocalMax, roachpb.KeyMax, 10)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				return beforeStats
			}()

			mkSST := func(kvs []storage.MVCCKeyValue) []byte {
				sstFile := &storage.MemFile{}
				sst := storage.MakeBackupSSTWriter(sstFile)
				defer sst.Close()
				for _, kv := range kvs {
					if err := sst.Put(kv.Key, kv.Value); err != nil {
						t.Fatalf("%+v", err)
					}
				}
				if err := sst.Finish(); err != nil {
					t.Fatalf("%+v", err)
				}
				return sstFile.Data()
			}

			sstBytes := mkSST(sstKVs)

			cArgs := batcheval.CommandArgs{
				Header: roachpb.Header{
					Timestamp: hlc.Timestamp{WallTime: 7},
				},
				Args: &roachpb.AddSSTableRequest{
					RequestHeader: roachpb.RequestHeader{Key: keys.MinKey, EndKey: keys.MaxKey},
					Data:          sstBytes,
				},
				Stats: &enginepb.MVCCStats{},
			}
			if _, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil); err != nil {
				t.Fatalf("%+v", err)
			}

			evaledStats := beforeStats
			evaledStats.Add(*cArgs.Stats)

			if err := e.WriteFile("sst", sstBytes); err != nil {
				t.Fatalf("%+v", err)
			}
			if err := e.IngestExternalFiles(ctx, []string{"sst"}); err != nil {
				t.Fatalf("%+v", err)
			}

			afterStats := func() enginepb.MVCCStats {
				iter := e.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
				defer iter.Close()
				afterStats, err := storage.ComputeStatsForRange(iter, keys.LocalMax, roachpb.KeyMax, 10)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				return afterStats
			}()
			evaledStats.Add(delta)
			evaledStats.ContainsEstimates = 0
			if !afterStats.Equal(evaledStats) {
				t.Errorf("mvcc stats mismatch: diff(expected, actual): %s", pretty.Diff(afterStats, evaledStats))
			}

			cArgsWithStats := batcheval.CommandArgs{
				Header: roachpb.Header{Timestamp: hlc.Timestamp{WallTime: 7}},
				Args: &roachpb.AddSSTableRequest{
					RequestHeader: roachpb.RequestHeader{Key: keys.MinKey, EndKey: keys.MaxKey},
					Data: mkSST([]storage.MVCCKeyValue{{
						Key:   storage.MVCCKey{Key: roachpb.Key("zzzzzzz"), Timestamp: ts},
						Value: roachpb.MakeValueFromBytes([]byte("zzz")).RawBytes,
					}}),
					MVCCStats: &enginepb.MVCCStats{KeyCount: 10},
				},
				Stats: &enginepb.MVCCStats{},
			}
			if _, err := batcheval.EvalAddSSTable(ctx, e, cArgsWithStats, nil); err != nil {
				t.Fatalf("%+v", err)
			}
			expected := enginepb.MVCCStats{ContainsEstimates: 1, KeyCount: 10}
			if got := *cArgsWithStats.Stats; got != expected {
				t.Fatalf("expected %v got %v", expected, got)
			}
		})
	}
}

func TestAddSSTableDisallowShadowing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, engineImpl := range engineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {
			e := engineImpl.create()
			defer e.Close()

			for _, kv := range mvccKVsFromStrs([]strKv{
				{"a", 2, "aa"},
				{"b", 1, "bb"},
				{"b", 6, ""},
				{"g", 5, "gg"},
				{"r", 1, "rr"},
				{"y", 1, "yy"},
				{"y", 2, ""},
				{"y", 5, "yyy"},
				{"z", 2, "zz"},
			}) {
				if err := e.PutMVCC(kv.Key, kv.Value); err != nil {
					t.Fatalf("%+v", err)
				}
			}

			getSSTBytes := func(sstKVs []storage.MVCCKeyValue) []byte {
				sstFile := &storage.MemFile{}
				sst := storage.MakeBackupSSTWriter(sstFile)
				defer sst.Close()
				for _, kv := range sstKVs {
					if err := sst.Put(kv.Key, kv.Value); err != nil {
						t.Fatalf("%+v", err)
					}
				}
				if err := sst.Finish(); err != nil {
					t.Fatalf("%+v", err)
				}
				return sstFile.Data()
			}

			getStats := func(startKey, endKey roachpb.Key, data []byte) enginepb.MVCCStats {
				dataIter, err := storage.NewMemSSTIterator(data, true)
				if err != nil {
					return enginepb.MVCCStats{}
				}
				defer dataIter.Close()

				stats, err := storage.ComputeStatsForRange(dataIter, startKey, endKey, 0)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				return stats
			}

			// Test key collision when ingesting a key in the start of existing data, and
			// SST. The colliding key is also equal to the header start key.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"a", 7, "aa"}, // colliding key has a higher timestamp than existing version.
				})

				sstBytes := getSSTBytes(sstKVs)
				stats := getStats(roachpb.Key("a"), roachpb.Key("b"), sstBytes)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
						Data:              sstBytes,
						DisallowShadowing: true,
						MVCCStats:         &stats,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "ingested key collides with an existing one: \"a\"") {
					t.Fatalf("%+v", err)
				}
			}

			// Test key collision when ingesting a key in the middle of existing data, and
			// start of the SST. The key is equal to the header start key.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"g", 4, "ggg"}, // colliding key has a lower timestamp than existing version.
				})

				sstBytes := getSSTBytes(sstKVs)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")},
						Data:              sstBytes,
						DisallowShadowing: true,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "ingested key collides with an existing one: \"g\"") {
					t.Fatalf("%+v", err)
				}
			}

			// Test key collision when ingesting a key at the end of the existing data and
			// SST. The colliding key is not equal to header start key.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"f", 2, "f"},
					{"h", 4, "h"},
					{"s", 1, "s"},
					{"z", 3, "z"}, // colliding key has a higher timestamp than existing version.
				})

				sstBytes := getSSTBytes(sstKVs)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("f"), EndKey: roachpb.Key("zz")},
						Data:              sstBytes,
						DisallowShadowing: true,
					},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "ingested key collides with an existing one: \"z\"") {
					t.Fatalf("%+v", err)
				}
			}

			// Test for no key collision where the key range being ingested into is
			// non-empty. The header start and end keys are not existing keys.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"c", 2, "bb"},
					{"h", 6, "hh"},
				})

				sstBytes := getSSTBytes(sstKVs)
				stats := getStats(roachpb.Key("c"), roachpb.Key("i"), sstBytes)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("c"), EndKey: roachpb.Key("i")},
						Data:              sstBytes,
						DisallowShadowing: true,
						MVCCStats:         &stats,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if err != nil {
					t.Fatalf("%+v", err)
				}
			}

			// Test that a collision is not reported when ingesting a key for which we
			// find a tombstone from an MVCC delete, and the sst key has a ts >= tombstone
			// ts. Also test that iteration continues from the next key in the existing
			// data after skipping over all the versions of the deleted key.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"b", 7, "bb"},  // colliding key has a higher timestamp than its deleted version.
					{"b", 1, "bbb"}, // older version of deleted key (should be skipped over).
					{"f", 3, "ff"},
					{"y", 3, "yyyy"}, // colliding key.
				})

				sstBytes := getSSTBytes(sstKVs)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("b"), EndKey: roachpb.Key("z")},
						Data:              sstBytes,
						DisallowShadowing: true,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "ingested key collides with an existing one: \"y\"") {
					t.Fatalf("%+v", err)
				}
			}

			// Test that a collision is  reported when ingesting a key for which we find a
			// tombstone from an MVCC delete, but the sst key has a ts < tombstone ts.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"b", 4, "bb"}, // colliding key has a lower timestamp than its deleted version.
					{"f", 3, "ff"},
					{"y", 3, "yyyy"},
				})

				sstBytes := getSSTBytes(sstKVs)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("b"), EndKey: roachpb.Key("z")},
						Data:              sstBytes,
						DisallowShadowing: true,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "ingested key collides with an existing one: \"b\"") {
					t.Fatalf("%+v", err)
				}
			}

			// Test key collision when ingesting a key which has been deleted, and readded
			// in the middle of the existing data. The colliding key is in the middle of
			// the SST, and is the earlier of the two possible collisions.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"f", 2, "ff"},
					{"y", 4, "yyy"}, // colliding key has a lower timestamp than the readded version.
					{"z", 3, "zzz"},
				})

				sstBytes := getSSTBytes(sstKVs)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("f"), EndKey: roachpb.Key("zz")},
						Data:              sstBytes,
						DisallowShadowing: true,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "ingested key collides with an existing one: \"y\"") {
					t.Fatalf("%+v", err)
				}
			}

			// Test key collision when ingesting a key which has a write intent in the
			// existing data.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"f", 2, "ff"},
					{"q", 4, "qq"},
					{"t", 3, "ttt"}, // has a write intent in the existing data.
				})

				// Add in a write intent.
				ts := hlc.Timestamp{WallTime: 7}
				txn := roachpb.MakeTransaction(
					"test",
					nil, // baseKey
					roachpb.NormalUserPriority,
					ts,
					base.DefaultMaxClockOffset.Nanoseconds(),
				)
				if err := storage.MVCCPut(
					ctx, e, nil, []byte("t"), ts,
					roachpb.MakeValueFromBytes([]byte("tt")),
					&txn,
				); err != nil {
					if !errors.HasType(err, (*roachpb.WriteIntentError)(nil)) {
						t.Fatalf("%+v", err)
					}
				}

				sstBytes := getSSTBytes(sstKVs)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("f"), EndKey: roachpb.Key("u")},
						Data:              sstBytes,
						DisallowShadowing: true,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "conflicting intents on \"t") {
					t.Fatalf("%+v", err)
				}
			}

			// Test key collision when ingesting a key which has an inline value in the
			// existing data.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"f", 2, "ff"},
					{"i", 4, "ii"}, // has an inline value in existing data.
					{"j", 3, "jj"},
				})

				// Add in an inline value.
				ts := hlc.Timestamp{}
				if err := storage.MVCCPut(
					ctx, e, nil, []byte("i"), ts,
					roachpb.MakeValueFromBytes([]byte("i")),
					nil,
				); err != nil {
					t.Fatalf("%+v", err)
				}

				sstBytes := getSSTBytes(sstKVs)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("f"), EndKey: roachpb.Key("k")},
						Data:              sstBytes,
						DisallowShadowing: true,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "inline values are unsupported when checking for key collisions") {
					t.Fatalf("%+v", err)
				}
			}

			// Test ingesting a key with the same timestamp and value. This should not
			// trigger a collision error.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"e", 4, "ee"},
					{"f", 2, "ff"},
					{"y", 5, "yyy"}, // key has the same timestamp and value as the one present in the existing data.
				})

				sstBytes := getSSTBytes(sstKVs)
				stats := getStats(roachpb.Key("e"), roachpb.Key("zz"), sstBytes)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("e"), EndKey: roachpb.Key("zz")},
						Data:              sstBytes,
						DisallowShadowing: true,
						MVCCStats:         &stats,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if err != nil {
					t.Fatalf("%+v", err)
				}
			}

			// Test ingesting a key with different timestamp but same value. This should
			// trigger a collision error.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"f", 2, "ff"},
					{"y", 6, "yyy"}, // key has a higher timestamp but same value as the one present in the existing data.
					{"z", 3, "zzz"},
				})

				sstBytes := getSSTBytes(sstKVs)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("f"), EndKey: roachpb.Key("zz")},
						Data:              sstBytes,
						DisallowShadowing: true,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "ingested key collides with an existing one: \"y\"") {
					t.Fatalf("%+v", err)
				}
			}

			// Test ingesting a key with the same timestamp but different value. This should
			// trigger a collision error.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"f", 2, "ff"},
					{"y", 5, "yyyy"}, // key has the same timestamp but different value as the one present in the existing data.
					{"z", 3, "zzz"},
				})

				sstBytes := getSSTBytes(sstKVs)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("f"), EndKey: roachpb.Key("zz")},
						Data:              sstBytes,
						DisallowShadowing: true,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "ingested key collides with an existing one: \"y\"") {
					t.Fatalf("%+v", err)
				}
			}

			// Test that a collision after a key with the same timestamp and value causes
			// a collision error.
			{
				sstKVs := mvccKVsFromStrs([]strKv{
					{"f", 2, "ff"},
					{"y", 5, "yyy"}, // key has the same timestamp and value as the one present in the existing data - not a collision.
					{"z", 3, "zzz"}, // shadow key
				})

				sstBytes := getSSTBytes(sstKVs)
				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("e"), EndKey: roachpb.Key("zz")},
						Data:              sstBytes,
						DisallowShadowing: true,
					},
					Stats: &enginepb.MVCCStats{},
				}

				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if !testutils.IsError(err, "ingested key collides with an existing one: \"z\"") {
					t.Fatalf("%+v", err)
				}
			}

			// This test ensures accuracy of MVCCStats in the situation that successive
			// SSTs being ingested via AddSSTable have "perfectly shadowing" keys (same ts
			// and value). Such KVs are not considered as collisions and so while they are
			// skipped during ingestion, their stats would previously be double counted.
			// To mitigate this problem we now return the stats of such skipped KVs while
			// evaluating the AddSSTable command, and accumulate accurate stats in the
			// CommandArgs Stats field by using:
			// cArgs.Stats + ingested_stats - skipped_stats.
			{
				// Successfully evaluate the first SST as there are no key collisions.
				sstKVs := mvccKVsFromStrs([]strKv{
					{"c", 2, "bb"},
					{"h", 6, "hh"},
				})

				sstBytes := getSSTBytes(sstKVs)
				stats := getStats(roachpb.Key("c"), roachpb.Key("i"), sstBytes)

				// Accumulate stats across SST ingestion.
				commandStats := enginepb.MVCCStats{}

				cArgs := batcheval.CommandArgs{
					Header: roachpb.Header{
						Timestamp: hlc.Timestamp{WallTime: 7},
					},
					Args: &roachpb.AddSSTableRequest{
						RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("c"), EndKey: roachpb.Key("i")},
						Data:              sstBytes,
						DisallowShadowing: true,
						MVCCStats:         &stats,
					},
					Stats: &commandStats,
				}
				_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				firstSSTStats := commandStats

				// Insert KV entries so that we can correctly identify keys to skip when
				// ingesting the perfectly shadowing KVs (same ts and same value) in the
				// second SST.
				for _, kv := range sstKVs {
					if err := e.PutMVCC(kv.Key, kv.Value); err != nil {
						t.Fatalf("%+v", err)
					}
				}

				// Evaluate the second SST. Both the KVs are perfectly shadowing and should
				// not contribute to the stats.
				secondSSTKVs := mvccKVsFromStrs([]strKv{
					{"c", 2, "bb"}, // key has the same timestamp and value as the one present in the existing data.
					{"h", 6, "hh"}, // key has the same timestamp and value as the one present in the existing data.
				})
				secondSSTBytes := getSSTBytes(secondSSTKVs)
				secondStats := getStats(roachpb.Key("c"), roachpb.Key("i"), secondSSTBytes)

				cArgs.Args = &roachpb.AddSSTableRequest{
					RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("c"), EndKey: roachpb.Key("i")},
					Data:              secondSSTBytes,
					DisallowShadowing: true,
					MVCCStats:         &secondStats,
				}
				_, err = batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if err != nil {
					t.Fatalf("%+v", err)
				}

				// Check that there has been no double counting of stats.
				if !firstSSTStats.Equal(*cArgs.Stats) {
					t.Errorf("mvcc stats should not have changed as all keys in second SST are shadowing: %s",
						pretty.Diff(firstSSTStats, *cArgs.Stats))
				}

				// Evaluate the third SST. Two of the three KVs are perfectly shadowing, but
				// there is one valid KV which should contribute to the stats.
				thirdSSTKVs := mvccKVsFromStrs([]strKv{
					{"c", 2, "bb"}, // key has the same timestamp and value as the one present in the existing data.
					{"e", 2, "ee"},
					{"h", 6, "hh"}, // key has the same timestamp and value as the one present in the existing data.
				})
				thirdSSTBytes := getSSTBytes(thirdSSTKVs)
				thirdStats := getStats(roachpb.Key("c"), roachpb.Key("i"), thirdSSTBytes)

				cArgs.Args = &roachpb.AddSSTableRequest{
					RequestHeader:     roachpb.RequestHeader{Key: roachpb.Key("c"), EndKey: roachpb.Key("i")},
					Data:              thirdSSTBytes,
					DisallowShadowing: true,
					MVCCStats:         &thirdStats,
				}
				_, err = batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
				if err != nil {
					t.Fatalf("%+v", err)
				}

				// This is the stats contribution of the KV {"e", 2, "ee"}. This should be
				// the only addition to the cumulative stats, as the other two KVs are
				// perfect shadows of existing data.
				var delta enginepb.MVCCStats
				delta.LiveCount = 1
				delta.LiveBytes = 21
				delta.KeyCount = 1
				delta.KeyBytes = 14
				delta.ValCount = 1
				delta.ValBytes = 7

				// Check that there has been no double counting of stats.
				firstSSTStats.Add(delta)
				if !firstSSTStats.Equal(*cArgs.Stats) {
					t.Errorf("mvcc stats are not accurate: %s",
						pretty.Diff(firstSSTStats, *cArgs.Stats))
				}
			}
		})
	}
}
