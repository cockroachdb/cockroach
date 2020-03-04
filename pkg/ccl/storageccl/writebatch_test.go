// Copyright 2016 The Cockroach Authors.
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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDBWriteBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Key range in request spans multiple ranges.
	if err := db.WriteBatch(
		ctx, keys.LocalMax, keys.MaxKey, nil,
	); !testutils.IsError(err, "data spans multiple ranges") {
		t.Fatalf("expected multiple ranges error got: %+v", err)
	}

	{
		var batch storage.RocksDBBatchBuilder
		key := storage.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		batch.Put(key, roachpb.MakeValueFromString("1").RawBytes)
		data := batch.Finish()

		// Key is before the range in the request span.
		if err := db.WriteBatch(
			ctx, "d", "e", data,
		); !testutils.IsError(err, "key not in request range") {
			t.Fatalf("expected request range error got: %+v", err)
		}
		// Key is after the range in the request span.
		if err := db.WriteBatch(
			ctx, "a", "b", data,
		); !testutils.IsError(err, "key not in request range") {
			t.Fatalf("expected request range error got: %+v", err)
		}

		if err := db.WriteBatch(ctx, "b", "c", data); err != nil {
			t.Fatalf("%+v", err)
		}
		if result, err := db.Get(ctx, "bb"); err != nil {
			t.Fatalf("%+v", err)
		} else if result := result.ValueBytes(); !bytes.Equal([]byte("1"), result) {
			t.Errorf("expected \"%s\", got \"%s\"", []byte("1"), result)
		}
	}

	// Key range in request span is not empty.
	{
		var batch storage.RocksDBBatchBuilder
		key := storage.MVCCKey{Key: []byte("bb2"), Timestamp: hlc.Timestamp{WallTime: 1}}
		batch.Put(key, roachpb.MakeValueFromString("2").RawBytes)
		data := batch.Finish()
		if err := db.WriteBatch(ctx, "b", "c", data); err != nil {
			t.Fatalf("%+v", err)
		}

		if result, err := db.Get(ctx, "bb2"); err != nil {
			t.Fatalf("%+v", err)
		} else if result := result.ValueBytes(); !bytes.Equal([]byte("2"), result) {
			t.Errorf("expected \"%s\", got \"%s\"", []byte("2"), result)
		}

		if result, err := db.Get(ctx, "bb"); err != nil {
			t.Fatalf("%+v", err)
		} else if result := result.ValueBytes(); result != nil {
			t.Errorf("expected nil, got \"%s\"", result)
		}
	}

	// Invalid key/value entry checksum.
	{
		var batch storage.RocksDBBatchBuilder
		key := storage.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		value := roachpb.MakeValueFromString("1")
		value.InitChecksum([]byte("foo"))
		batch.Put(key, value.RawBytes)
		data := batch.Finish()

		if err := db.WriteBatch(ctx, "b", "c", data); !testutils.IsError(err, "invalid checksum") {
			t.Fatalf("expected 'invalid checksum' error got: %+v", err)
		}
	}
}

func TestWriteBatchMVCCStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	e := storage.NewDefaultInMem()
	defer e.Close()

	var batch storage.RocksDBBatchBuilder
	{
		key := storage.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		batch.Put(key, roachpb.MakeValueFromString("1").RawBytes)
	}
	data := batch.Finish()
	span := roachpb.Span{Key: []byte("b"), EndKey: []byte("c")}

	// WriteBatch deletes any data that exists in the keyrange before applying
	// the batch. Put something there to delete. The mvcc stats should be
	// adjusted accordingly.
	const numInitialEntries = 100
	for i := 0; i < numInitialEntries; i++ {
		if err := e.Put(storage.MVCCKey{Key: append([]byte("b"), byte(i))}, nil); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	cArgs := batcheval.CommandArgs{
		Args: &roachpb.WriteBatchRequest{
			RequestHeader: roachpb.RequestHeaderFromSpan(span),
			DataSpan:      span,
			Data:          data,
		},
		// Start with some stats to represent data throughout the replica's
		// keyrange.
		Stats: &enginepb.MVCCStats{
			LiveBytes: 10000,
			LiveCount: 10000,
			KeyBytes:  10000,
			KeyCount:  10000,
			ValBytes:  10000,
			ValCount:  10000,
		},
	}
	if _, err := evalWriteBatch(ctx, e, cArgs, nil); err != nil {
		t.Fatalf("%+v", err)
	}

	expectedStats := &enginepb.MVCCStats{
		LiveBytes: 9721,
		LiveCount: 9901,
		KeyBytes:  9715,
		KeyCount:  9901,
		ValBytes:  10006,
		ValCount:  10001,
	}
	if !reflect.DeepEqual(expectedStats, cArgs.Stats) {
		t.Errorf("mvcc stats mismatch %+v != %+v", expectedStats, cArgs.Stats)
	}

	// Run the same WriteBatch command a second time to test the idempotence.
	if _, err := evalWriteBatch(ctx, e, cArgs, nil); err != nil {
		t.Fatalf("%+v", err)
	}
	if !reflect.DeepEqual(expectedStats, cArgs.Stats) {
		t.Errorf("mvcc stats mismatch %+v != %+v", expectedStats, cArgs.Stats)
	}
}
