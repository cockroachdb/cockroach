// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/mvcc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestVerifyBatchRepr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyA := mvcc.Key{Key: []byte("a")}
	keyB := mvcc.Key{Key: []byte("b")}
	keyC := mvcc.Key{Key: []byte("c")}
	keyD := mvcc.Key{Key: []byte("d")}
	keyE := mvcc.Key{Key: []byte("e")}

	var batch engine.RocksDBBatchBuilder
	key := mvcc.Key{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
	batch.Put(key, roachpb.MakeValueFromString("1").RawBytes)
	data := batch.Finish()

	ms, err := VerifyBatchRepr(data, keyB, keyC, 0)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if ms.KeyCount != 1 {
		t.Fatalf("got %d expected 1", ms.KeyCount)
	}

	// Key is before the range in the request span.
	if _, err := VerifyBatchRepr(data, keyD, keyE, 0); !testutils.IsError(err, "request range") {
		t.Fatalf("expected request range error got: %+v", err)
	}
	// Key is after the range in the request span.
	if _, err := VerifyBatchRepr(data, keyA, keyB, 0); !testutils.IsError(err, "request range") {
		t.Fatalf("expected request range error got: %+v", err)
	}

	// Invalid key/value entry checksum.
	{
		var batch engine.RocksDBBatchBuilder
		key := mvcc.Key{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		value := roachpb.MakeValueFromString("1")
		value.InitChecksum([]byte("foo"))
		batch.Put(key, value.RawBytes)
		data := batch.Finish()

		if _, err := VerifyBatchRepr(data, keyB, keyC, 0); !testutils.IsError(err, "invalid checksum") {
			t.Fatalf("expected 'invalid checksum' error got: %+v", err)
		}
	}
}
