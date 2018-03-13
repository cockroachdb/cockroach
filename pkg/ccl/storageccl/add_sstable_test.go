// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
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
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
				"target key range not empty, will merge existing data with sstable",
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
			if after := metrics.AddSSTableApplicationCopies.Count(); before >= after {
				t.Fatalf("expected sst copies to increase, %d before %d after", before, after)
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

func randomMVCCKeyValues(rng *rand.Rand, numKVs int) []engine.MVCCKeyValue {
	kvs := make([]engine.MVCCKeyValue, numKVs)
	for i := range kvs {
		kvs[i] = engine.MVCCKeyValue{
			Key: engine.MVCCKey{
				Key:       randutil.RandBytes(rng, 1),
				Timestamp: hlc.Timestamp{WallTime: 1 + rand.Int63n(10)},
			},
			Value: roachpb.MakeValueFromBytes(randutil.RandBytes(rng, rng.Intn(10))).RawBytes,
		}
		if rng.Intn(100) < 25 {
			// 25% of the time, make the entry a deletion tombstone.
			kvs[i].Value = nil
		}
	}
	return kvs
}

type mvccKeyValues []engine.MVCCKeyValue

func (kvs mvccKeyValues) Len() int           { return len(kvs) }
func (kvs mvccKeyValues) Less(i, j int) bool { return kvs[i].Key.Less(kvs[j].Key) }
func (kvs mvccKeyValues) Swap(i, j int)      { kvs[i], kvs[j] = kvs[j], kvs[i] }

func TestAddSSTableMVCCStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, seed := randutil.NewPseudoRand()
	t.Logf("random seed: %d", seed)

	const numIterations, maxKVs = 10, 100

	ctx := context.Background()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()

	// This test repeatedly:
	// - puts some random data in an engine
	// - randomly makes one key an intent
	// - puts some random data in an sst
	// - computes pre-ingest mvcc stats for the engine
	// - gets the mvcc stats diff from evalAddSSTable for the sst
	// - ingests the sstable into the engine
	// - computes post-ingest mvcc stats for the engine
	// - compares pre-ingest + diff stats vs post-ingest stats
	//
	// Each time through, the engine accumulates more directly Put keys and more
	// ingested sstables.
	var nowNanos int64
	for i := 0; i < numIterations; i++ {
		nowNanos += rng.Int63n(1e9)
		for _, kv := range randomMVCCKeyValues(rng, 1+rand.Intn(maxKVs)) {
			if err := e.Put(kv.Key, kv.Value); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		// Add in a random metadata key.
		ts := hlc.Timestamp{WallTime: nowNanos}
		txn := roachpb.MakeTransaction(
			"test",
			nil, // baseKey
			roachpb.NormalUserPriority,
			enginepb.SERIALIZABLE,
			ts,
			base.DefaultMaxClockOffset.Nanoseconds(),
		)
		if err := engine.MVCCPut(
			ctx, e, nil, randutil.RandBytes(rng, 1), ts,
			roachpb.MakeValueFromBytes(randutil.RandBytes(rng, rng.Intn(10))),
			&txn,
		); err != nil {
			if _, isWriteIntentErr := err.(*roachpb.WriteIntentError); !isWriteIntentErr {
				t.Fatalf("%+v", err)
			}
		}

		sstBytes := func() []byte {
			sst, err := engine.MakeRocksDBSstFileWriter()
			if err != nil {
				t.Fatalf("%+v", err)
			}
			defer sst.Close()
			sstKVs := mvccKeyValues(randomMVCCKeyValues(rng, 1+rand.Intn(maxKVs)))
			sort.Sort(sstKVs)
			var prevKey engine.MVCCKey
			for _, kv := range sstKVs {
				if kv.Key.Equal(prevKey) {
					// RocksDB doesn't let us add the same key twice.
					continue
				}
				prevKey.Key = append(prevKey.Key[:0], kv.Key.Key...)
				prevKey.Timestamp = kv.Key.Timestamp
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

		nowNanos += rng.Int63n(1e9)
		cArgs := batcheval.CommandArgs{
			Header: roachpb.Header{
				Timestamp: hlc.Timestamp{WallTime: nowNanos},
			},
			Args: &roachpb.AddSSTableRequest{
				Span: roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
				Data: sstBytes,
			},
			Stats: &enginepb.MVCCStats{},
		}
		_, err := evalAddSSTable(ctx, e, cArgs, nil)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		// After evalAddSSTable, cArgs.Stats contains a diff to the existing
		// stats. Make sure recomputing from scratch gets the same answer as
		// applying the diff to the stats
		beforeStats := func() enginepb.MVCCStats {
			iter := e.NewIterator(false)
			defer iter.Close()
			beforeStats, err := engine.ComputeStatsGo(iter, engine.NilKey, engine.MVCCKeyMax, nowNanos)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			return beforeStats
		}()
		beforeStats.Add(*cArgs.Stats)

		filename := fmt.Sprintf("/%d.sst", i)
		if err := e.WriteFile(filename, sstBytes); err != nil {
			t.Fatalf("%+v", err)
		}
		if err := e.IngestExternalFile(ctx, filename, true /* modify the sst */); err != nil {
			t.Fatalf("%+v", err)
		}

		afterStats := func() enginepb.MVCCStats {
			iter := e.NewIterator(false)
			defer iter.Close()
			afterStats, err := engine.ComputeStatsGo(iter, engine.NilKey, engine.MVCCKeyMax, nowNanos)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			return afterStats
		}()

		if !reflect.DeepEqual(beforeStats, afterStats) {
			t.Errorf("mvcc stats mismatch: diff(expected, actual): %s", pretty.Diff(afterStats, beforeStats))
		}
	}
}
