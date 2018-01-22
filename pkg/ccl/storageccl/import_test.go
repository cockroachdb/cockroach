// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func TestMaxImportBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		importBatchSize int64
		maxCommandSize  int64
		expected        int64
	}{
		{importBatchSize: 2 << 20, maxCommandSize: 64 << 20, expected: 2 << 20},
		{importBatchSize: 128 << 20, maxCommandSize: 64 << 20, expected: 63 << 20},
		{importBatchSize: 64 << 20, maxCommandSize: 64 << 20, expected: 63 << 20},
		{importBatchSize: 63 << 20, maxCommandSize: 64 << 20, expected: 63 << 20},
	}
	for i, testCase := range testCases {
		st := cluster.MakeTestingClusterSettings()
		importBatchSize.Override(&st.SV, testCase.importBatchSize)
		storage.MaxCommandSize.Override(&st.SV, testCase.maxCommandSize)
		if e, a := MaxImportBatchSize(st), testCase.expected; e != a {
			t.Errorf("%d: expected max batch size %d, but got %d", i, e, a)
		}
	}
}

func slurpSSTablesLatestKey(
	t *testing.T, dir string, paths []string, kr prefixRewriter,
) []engine.MVCCKeyValue {
	start, end := engine.MVCCKey{Key: keys.MinKey}, engine.MVCCKey{Key: keys.MaxKey}

	e := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()
	batch := e.NewBatch()
	defer batch.Close()

	for _, path := range paths {
		sst := engine.MakeRocksDBSstFileReader()
		defer sst.Close()

		fileContents, err := ioutil.ReadFile(filepath.Join(dir, path))
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if err := sst.IngestExternalFile(fileContents); err != nil {
			t.Fatalf("%+v", err)
		}
		if err := sst.Iterate(start, end, func(kv engine.MVCCKeyValue) (bool, error) {
			var ok bool
			kv.Key.Key, ok = kr.rewriteKey(kv.Key.Key)
			if !ok {
				return true, errors.Errorf("could not rewrite key: %s", kv.Key.Key)
			}
			v := roachpb.Value{RawBytes: kv.Value}
			v.ClearChecksum()
			v.InitChecksum(kv.Key.Key)
			if err := batch.Put(kv.Key, v.RawBytes); err != nil {
				return true, err
			}
			return false, nil
		}); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	var kvs []engine.MVCCKeyValue
	it := batch.NewIterator(false)
	defer it.Close()
	for it.Seek(start); ; it.NextKey() {
		if ok, err := it.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok || !it.UnsafeKey().Less(end) {
			break
		}
		kvs = append(kvs, engine.MVCCKeyValue{Key: it.Key(), Value: it.Value()})
	}
	return kvs
}

func clientKVsToEngineKVs(kvs []client.KeyValue) []engine.MVCCKeyValue {
	var ret []engine.MVCCKeyValue
	for _, kv := range kvs {
		if kv.Value == nil {
			continue
		}
		k := engine.MVCCKey{
			Key:       kv.Key,
			Timestamp: kv.Value.Timestamp,
		}
		ret = append(ret, engine.MVCCKeyValue{Key: k, Value: kv.Value.RawBytes})
	}
	return ret
}

func TestImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("batch=default", func(t *testing.T) {
		runTestImport(t, func(_ *cluster.Settings) {})
	})
	t.Run("batch=1", func(t *testing.T) {
		// The test normally doesn't trigger the batching behavior, so lower
		// the threshold to force it.
		init := func(st *cluster.Settings) {
			importBatchSize.Override(&st.SV, 1)
		}
		runTestImport(t, init)
	})
}

func runTestImport(t *testing.T, init func(*cluster.Settings)) {
	defer leaktest.AfterTest(t)()

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	if err := os.Mkdir(filepath.Join(dir, "foo"), 0755); err != nil {
		t.Fatal(err)
	}

	writeSST := func(keys ...[]byte) string {
		path := strconv.FormatInt(hlc.UnixNano(), 10)

		sst, err := engine.MakeRocksDBSstFileWriter()
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer sst.Close()
		ts := hlc.NewClock(hlc.UnixNano, time.Nanosecond).Now()
		value := roachpb.MakeValueFromString("bar")
		for _, key := range keys {
			value.ClearChecksum()
			value.InitChecksum(key)
			kv := engine.MVCCKeyValue{Key: engine.MVCCKey{Key: key, Timestamp: ts}, Value: value.RawBytes}
			if err := sst.Add(kv); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		sstContents, err := sst.Finish()
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if err := ioutil.WriteFile(filepath.Join(dir, "foo", path), sstContents, 0644); err != nil {
			t.Fatalf("%+v", err)
		}
		return path
	}

	const (
		oldID   = 51
		newID   = 100
		indexID = 1
	)

	kr := prefixRewriter{
		{OldPrefix: makeKeyRewriterPrefixIgnoringInterleaved(oldID, indexID), NewPrefix: makeKeyRewriterPrefixIgnoringInterleaved(newID, indexID)},
	}
	var keys [][]byte
	for i := 0; i < 4; i++ {
		key := append([]byte(nil), kr[0].OldPrefix...)
		key = encoding.EncodeStringAscending(key, fmt.Sprintf("foo%d", i))
		keys = append(keys, key)
	}

	rekeys := []roachpb.ImportRequest_TableRekey{
		{
			OldID: oldID,
			NewDesc: mustMarshalDesc(t, &sqlbase.TableDescriptor{
				ID: newID,
				PrimaryIndex: sqlbase.IndexDescriptor{
					ID: indexID,
				},
			}),
		},
	}

	files := []string{
		writeSST(keys[2], keys[3]),
		writeSST(keys[0], keys[1]),
		writeSST(keys[2], keys[3]),
	}

	dataStartKey := roachpb.Key(keys[0])
	dataEndKey := roachpb.Key(keys[3]).PrefixEnd()
	reqStartKey, ok := kr.rewriteKey(append([]byte(nil), dataStartKey...))
	if !ok {
		t.Fatalf("failed to rewrite key: %s", reqStartKey)
	}
	reqEndKey, ok := kr.rewriteKey(append([]byte(nil), dataEndKey...))
	if !ok {
		t.Fatalf("failed to rewrite key: %s", reqEndKey)
	}

	// Make the first few WriteBatch/AddSSTable calls return
	// AmbiguousResultError. Import should be resilient to this.
	const initialAmbiguousSubReqs = 3
	remainingAmbiguousSubReqs := int64(initialAmbiguousSubReqs)
	knobs := base.TestingKnobs{Store: &storage.StoreTestingKnobs{
		EvalKnobs: batcheval.TestingKnobs{
			TestingEvalFilter: func(filterArgs storagebase.FilterArgs) *roachpb.Error {
				switch filterArgs.Req.(type) {
				case *roachpb.WriteBatchRequest, *roachpb.AddSSTableRequest:
				// No-op.
				default:
					return nil
				}
				r := atomic.AddInt64(&remainingAmbiguousSubReqs, -1)
				if r < 0 {
					return nil
				}
				return roachpb.NewError(roachpb.NewAmbiguousResultError(strconv.Itoa(int(r))))
			},
		},
	}}

	ctx := context.Background()
	args := base.TestServerArgs{Knobs: knobs, ExternalIODir: dir}
	// TODO(dan): This currently doesn't work with AddSSTable on in-memory
	// stores because RocksDB's InMemoryEnv doesn't support NewRandomRWFile
	// (which breaks the global-seqno rewrite used when the added sstable
	// overlaps with existing data in the RocksDB instance). #16345.
	args.StoreSpecs = []base.StoreSpec{{InMemory: false, Path: filepath.Join(dir, "testserver")}}
	s, _, kvDB := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	init(s.ClusterSettings())

	storage, err := ExportStorageConfFromURI("nodelocal:///foo")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	for i := 1; i <= len(files); i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			atomic.StoreInt64(&remainingAmbiguousSubReqs, initialAmbiguousSubReqs)

			req := &roachpb.ImportRequest{
				Span:     roachpb.Span{Key: reqStartKey},
				DataSpan: roachpb.Span{Key: dataStartKey, EndKey: dataEndKey},
				Rekeys:   rekeys,
			}

			for _, f := range files[:i] {
				req.Files = append(req.Files, roachpb.ImportRequest_File{Dir: storage, Path: f})
			}
			expectedKVs := slurpSSTablesLatestKey(t, filepath.Join(dir, "foo"), files[:i], kr)

			// Import may be retried by DistSender if it takes too long to return, so
			// make sure it's idempotent.
			for j := 0; j < 1; j++ {
				b := &client.Batch{}
				b.AddRawRequest(req)
				if err := kvDB.Run(ctx, b); err != nil {
					t.Fatalf("%+v", err)
				}
				clientKVs, err := kvDB.Scan(ctx, reqStartKey, reqEndKey, 0)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				kvs := clientKVsToEngineKVs(clientKVs)

				if !reflect.DeepEqual(kvs, expectedKVs) {
					for _, kv := range append(kvs, expectedKVs...) {
						log.Info(ctx, kv)
					}
					t.Fatalf("got %+v expected %+v", kvs, expectedKVs)
				}
			}

			if r := atomic.LoadInt64(&remainingAmbiguousSubReqs); r > 0 {
				t.Errorf("expected ambiguous sub-requests to be depleted got %d", r)
			}
		})
	}
}
