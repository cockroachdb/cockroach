// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package storageccl

import (
	"bytes"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDBWriteBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop()
	ctx := context.Background()

	var batch engine.RocksDBBatchBuilder
	key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
	batch.Put(key, roachpb.MakeValueFromString("1").RawBytes)
	data := batch.Finish()

	if err := db.WriteBatch(ctx, "b", "c", data); err != nil {
		t.Fatalf("%+v", err)
	}
	result, err := db.Get(ctx, "bb")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if result := result.ValueBytes(); !bytes.Equal([]byte("1"), result) {
		t.Errorf("expected \"%s\", got \"%s\"", []byte("1"), result)
	}

	// Key is before the range in the request span.
	if err := db.WriteBatch(ctx, "d", "e", data); !testutils.IsError(err, "request range") {
		t.Fatalf("expected request range error got: %+v", err)
	}
	// Key is after the range in the request span.
	if err := db.WriteBatch(ctx, "a", "b", data); !testutils.IsError(err, "request range") {
		t.Fatalf("expected request range error got: %+v", err)
	}

	// Key range in request span is not empty.
	if err := db.Put(ctx, "cc", 2); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := db.WriteBatch(ctx, "c", "d", nil); !testutils.IsError(err, "empty range") {
		t.Fatalf("expected empty range error got: %+v", err)
	}

	// Key range in request spans multiple ranges.
	if err := db.WriteBatch(ctx, keys.LocalMax, keys.MaxKey, data); !testutils.IsError(err, "multiple ranges") {
		t.Fatalf("expected multiple ranges error got: %+v", err)
	}

	// TODO(dan): Test the MVCCStats behavior of the WriteBatch impl.
}
