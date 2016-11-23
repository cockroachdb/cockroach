// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/LICENSE

package storageccl

import (
	"path/filepath"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func writeOneKeySST(path string, key []byte) error {
	sst := engine.MakeRocksDBSstFileWriter()
	if err := sst.Open(path); err != nil {
		_ = sst.Close()
		return err
	}
	ts := hlc.NewClock(hlc.UnixNano, 0).Now()
	value := roachpb.MakeValueFromString("bar")
	value.InitChecksum(key)
	kv := engine.MVCCKeyValue{Key: engine.MVCCKey{Key: key, Timestamp: ts}, Value: value.RawBytes}
	if err := sst.Add(kv); err != nil {
		return err
	}
	return sst.Close()
}

func TestImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	dir, dirCleanupFn := testutils.TempDir(t, 0)
	defer dirCleanupFn()
	ctx := context.Background()

	kr := KeyRewriter([]roachpb.KeyRewrite{
		{OldPrefix: keys.MakeTablePrefix(0), NewPrefix: keys.MakeTablePrefix(100)},
	})

	const sstName = "data.sst"
	key := encoding.EncodeBytesAscending(kr[0].OldPrefix, []byte("foo"))
	if err := writeOneKeySST(filepath.Join(dir, sstName), key); err != nil {
		t.Fatalf("%+v", err)
	}

	dataStartKey := roachpb.Key(key)
	dataEndKey := dataStartKey.PrefixEnd()
	reqStartKey, ok := kr.RewriteKey(append([]byte(nil), dataStartKey...))
	if !ok {
		t.Fatalf("failed to rewrite key: %s", reqStartKey)
	}
	reqEndKey, ok := kr.RewriteKey(append([]byte(nil), dataEndKey...))
	if !ok {
		t.Fatalf("failed to rewrite key: %s", reqEndKey)
	}
	storage, err := ExportStorageConfFromURI(dir)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	req := &roachpb.ImportRequest{
		Span:       roachpb.Span{Key: reqStartKey, EndKey: reqEndKey},
		DataSpan:   roachpb.Span{Key: dataStartKey, EndKey: dataEndKey},
		KeyRewrite: kr,
		File: []roachpb.ImportRequest_File{
			{Dir: storage, Path: sstName},
		},
	}
	b := &client.Batch{}
	b.AddRawRequest(req)
	if err := kvDB.Run(ctx, b); err != nil {
		t.Fatalf("%+v", err)
	}
	kvs, err := kvDB.Scan(ctx, reqStartKey, reqEndKey, 0)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(kvs) != 1 {
		t.Fatalf("expected 1 kv got %d", len(kvs))
	}

	// The previous request already inserted data, but the request keyrange is
	// required to be empty, so running it again should fail.
	b = &client.Batch{}
	b.AddRawRequest(req)
	if err := kvDB.Run(ctx, b); !testutils.IsError(err, "empty ranges") {
		t.Fatalf("expected 'empty ranges' error got %+v", err)
	}
}
