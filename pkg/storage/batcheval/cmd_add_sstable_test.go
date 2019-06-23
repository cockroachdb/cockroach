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
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/kr/pretty"
)

func singleKVSSTable(key engine.MVCCKey, value []byte) ([]byte, error) {
	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return nil, err
	}
	defer sst.Close()
	kv := engine.MVCCKeyValue{Key: key, Value: value}
	if err := sst.Add(kv); err != nil {
		return nil, err
	}
	return sst.Finish()
}

func TestDBAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("store=in-memory", func(t *testing.T) {
		s, _, db := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
		ctx := context.Background()
		defer s.Stopper().Stop(ctx)
		runTestDBAddSSTable(ctx, t, db, nil)
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
		store, err := s.GetStores().(*storage.Stores).GetStore(s.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		runTestDBAddSSTable(ctx, t, db, store)
	})
}

// if store != nil, assume it is on-disk and check ingestion semantics.
func runTestDBAddSSTable(ctx context.Context, t *testing.T, db *client.DB, store *storage.Store) {
	{
		key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 2}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("1").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		// Key is before the range in the request span.
		if err := db.AddSSTable(
			ctx, "d", "e", data,
		); !testutils.IsError(err, "not in request range") {
			t.Fatalf("expected request range error got: %+v", err)
		}
		// Key is after the range in the request span.
		if err := db.AddSSTable(
			ctx, "a", "b", data,
		); !testutils.IsError(err, "not in request range") {
			t.Fatalf("expected request range error got: %+v", err)
		}

		// Do an initial ingest.
		ingestCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, "test-recording")
		defer cancel()
		if err := db.AddSSTable(ingestCtx, "b", "c", data); err != nil {
			t.Fatalf("%+v", err)
		}
		formatted := tracing.FormatRecordedSpans(collect())
		if err := testutils.MatchInOrder(formatted,
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
				t.Fatalf("%q file missing after ingest: %v", match[1], err)
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
		key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("2").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if err := db.AddSSTable(ctx, "b", "c", data); err != nil {
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
		key := engine.MVCCKey{Key: []byte("bc"), Timestamp: hlc.Timestamp{WallTime: 1}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("3").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		var metrics *storage.StoreMetrics
		var before int64
		if store != nil {
			metrics = store.Metrics()
			before = metrics.AddSSTableApplicationCopies.Count()
		}
		for i := 0; i < 2; i++ {
			ingestCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, "test-recording")
			defer cancel()

			if err := db.AddSSTable(ingestCtx, "b", "c", data); err != nil {
				t.Fatalf("%+v", err)
			}
			if err := testutils.MatchInOrder(tracing.FormatRecordedSpans(collect()),
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

	// Invalid key/value entry checksum.
	{
		key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		value := roachpb.MakeValueFromString("1")
		value.InitChecksum([]byte("foo"))
		data, err := singleKVSSTable(key, value.RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if err := db.AddSSTable(ctx, "b", "c", data); !testutils.IsError(err, "invalid checksum") {
			t.Fatalf("expected 'invalid checksum' error got: %+v", err)
		}
	}
}

type strKv struct {
	k  string
	ts int64
	v  string
}

func mvccKVsFromStrs(in []strKv) []engine.MVCCKeyValue {
	kvs := make([]engine.MVCCKeyValue, len(in))
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

	ctx := context.Background()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<20)
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
		if err := e.Put(kv.Key, kv.Value); err != nil {
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
	if err := engine.MVCCPut(
		ctx, e, nil, []byte("i"), ts,
		roachpb.MakeValueFromBytes([]byte("it")),
		&txn,
	); err != nil {
		if _, isWriteIntentErr := err.(*roachpb.WriteIntentError); !isWriteIntentErr {
			t.Fatalf("%+v", err)
		}
	}

	// After evalAddSSTable, cArgs.Stats contains a diff to the existing
	// stats. Make sure recomputing from scratch gets the same answer as
	// applying the diff to the stats
	beforeStats := func() enginepb.MVCCStats {
		iter := e.NewIterator(engine.IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()
		beforeStats, err := engine.ComputeStatsGo(iter, engine.NilKey, engine.MVCCKeyMax, 10)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		return beforeStats
	}()

	sstBytes := func() []byte {
		sst, err := engine.MakeRocksDBSstFileWriter()
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer sst.Close()
		for _, kv := range sstKVs {
			if err := sst.Add(kv); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		sstBytes, err := sst.Finish()
		if err != nil {
			t.Fatalf("%+v", err)
		}
		return sstBytes
	}()

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
	_, err := batcheval.EvalAddSSTable(ctx, e, cArgs, nil)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	evaledStats := beforeStats
	evaledStats.Add(*cArgs.Stats)

	if err := e.WriteFile("sst", sstBytes); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := e.IngestExternalFiles(ctx, []string{"sst"}, true /* skip writing global seqno */, true /* modify the sst */); err != nil {
		t.Fatalf("%+v", err)
	}

	afterStats := func() enginepb.MVCCStats {
		iter := e.NewIterator(engine.IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()
		afterStats, err := engine.ComputeStatsGo(iter, engine.NilKey, engine.MVCCKeyMax, 10)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		return afterStats
	}()
	evaledStats.Add(delta)
	evaledStats.ContainsEstimates = false
	if !afterStats.Equal(evaledStats) {
		t.Errorf("mvcc stats mismatch: diff(expected, actual): %s", pretty.Diff(afterStats, evaledStats))
	}
}
