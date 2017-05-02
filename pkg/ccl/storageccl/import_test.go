// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/LICENSE

package storageccl

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
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

func slurpSSTablesLatestKey(
	t *testing.T, dir string, paths []string, kr KeyRewriter,
) []engine.MVCCKeyValue {
	start, end := engine.MVCCKey{Key: keys.MinKey}, engine.MVCCKey{Key: keys.MaxKey}

	e := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()
	batch := e.NewBatch()
	defer batch.Close()

	for _, path := range paths {
		readerTempDir, err := ioutil.TempDir(dir, "RocksDBSstFileReader")
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer func() {
			if err := os.RemoveAll(readerTempDir); err != nil {
				t.Fatalf("%+v", err)
			}
		}()

		sst, err := engine.MakeRocksDBSstFileReader(readerTempDir)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer sst.Close()
		if err := sst.AddFile(filepath.Join(dir, path)); err != nil {
			t.Fatalf("%+v", err)
		}
		if err := sst.Iterate(start, end, func(kv engine.MVCCKeyValue) (bool, error) {
			var ok bool
			kv.Key.Key, ok = kr.RewriteKey(kv.Key.Key)
			if !ok {
				return true, errors.Errorf("could not rewrite key: %s", roachpb.Key(kv.Key.Key))
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

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	writeSST := func(keys ...[]byte) string {
		path := strconv.FormatInt(hlc.UnixNano(), 10)

		sst := engine.MakeRocksDBSstFileWriter()
		if err := sst.Open(filepath.Join(dir, path)); err != nil {
			_ = sst.Close()
			t.Fatalf("%+v", err)
		}
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
		if err := sst.Close(); err != nil {
			t.Fatalf("%+v", err)
		}
		return path
	}

	kr := KeyRewriter([]roachpb.KeyRewrite{
		{OldPrefix: keys.MakeTablePrefix(51), NewPrefix: keys.MakeTablePrefix(100)},
	})
	var keys [][]byte
	for i := 0; i < 4; i++ {
		key := append([]byte(nil), kr[0].OldPrefix...)
		key = encoding.EncodeStringAscending(key, fmt.Sprintf("foo%d", i))
		keys = append(keys, key)
	}

	files := []string{
		writeSST(keys[2], keys[3]),
		writeSST(keys[0], keys[1]),
		writeSST(keys[2], keys[3]),
	}

	dataStartKey := roachpb.Key(keys[0])
	dataEndKey := roachpb.Key(keys[3]).PrefixEnd()
	reqStartKey, ok := kr.RewriteKey(append([]byte(nil), dataStartKey...))
	if !ok {
		t.Fatalf("failed to rewrite key: %s", reqStartKey)
	}
	reqEndKey, ok := kr.RewriteKey(append([]byte(nil), dataEndKey...))
	if !ok {
		t.Fatalf("failed to rewrite key: %s", reqEndKey)
	}

	// Make the first few WriteBatch calls return AmbiguousResultError. Import
	// should be resilient to this.
	const initialAmbiguousWriteBatches = 3
	remainingAmbiguousWriteBatches := int64(initialAmbiguousWriteBatches)
	knobs := base.TestingKnobs{Store: &storage.StoreTestingKnobs{
		TestingEvalFilter: func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if _, ok := filterArgs.Req.(*roachpb.WriteBatchRequest); !ok {
				return nil
			}
			r := atomic.AddInt64(&remainingAmbiguousWriteBatches, -1)
			if r < 0 {
				return nil
			}
			return roachpb.NewError(roachpb.NewAmbiguousResultError(strconv.Itoa(int(r))))
		},
	}}

	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{Knobs: knobs})
	defer s.Stopper().Stop(ctx)

	storage, err := ExportStorageConfFromURI("nodelocal://" + dir)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	for i := 1; i <= len(files); i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			atomic.StoreInt64(&remainingAmbiguousWriteBatches, initialAmbiguousWriteBatches)

			req := &roachpb.ImportRequest{
				Span:        roachpb.Span{Key: reqStartKey},
				DataSpan:    roachpb.Span{Key: dataStartKey, EndKey: dataEndKey},
				KeyRewrites: kr,
			}

			for _, f := range files[:i] {
				req.Files = append(req.Files, roachpb.ImportRequest_File{Dir: storage, Path: f})
			}
			expectedKVs := slurpSSTablesLatestKey(t, dir, files[:i], kr)

			// Import may be retried by DistSender if it takes too long to return, so
			// make sure it's idempotent.
			for j := 0; j < 3; j++ {
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

			if r := atomic.LoadInt64(&remainingAmbiguousWriteBatches); r > 0 {
				t.Errorf("expected ambiguous write batches to be depleted got %d", r)
			}
		})
	}
}
