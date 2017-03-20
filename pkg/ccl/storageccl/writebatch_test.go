// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package storageccl

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestDBWriteBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop()
	ctx := context.Background()

	// Key range in request spans multiple ranges.
	if err := db.WriteBatch(
		ctx, keys.LocalMax, keys.MaxKey, nil,
	); !testutils.IsError(err, "data spans multiple ranges") {
		t.Fatalf("expected multiple ranges error got: %+v", err)
	}

	{
		var batch engine.RocksDBBatchBuilder
		key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
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
		var batch engine.RocksDBBatchBuilder
		key := engine.MVCCKey{Key: []byte("bb2"), Timestamp: hlc.Timestamp{WallTime: 1}}
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
}

func TestWriteBatchMVCCStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	ctx := context.Background()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()

	var batch engine.RocksDBBatchBuilder
	{
		key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		batch.Put(key, roachpb.MakeValueFromString("1").RawBytes)
	}
	data := batch.Finish()
	span := roachpb.Span{Key: []byte("b"), EndKey: []byte("c")}

	// WriteBatch deletes any data that exists in the keyrange before applying
	// the batch. Put something there to delete. The mvcc stats should be
	// adjusted accordingly.
	const numInitialEntries = 100
	for i := 0; i < numInitialEntries; i++ {
		if err := e.Put(engine.MVCCKey{Key: append([]byte("b"), byte(i))}, nil); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	cArgs := storage.CommandArgs{
		Args: &roachpb.WriteBatchRequest{
			Span:     span,
			DataSpan: span,
			Data:     data,
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

func BenchmarkWriteBatch(b *testing.B) {
	if !storage.ProposerEvaluatedKVEnabled() {
		b.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}
	rng, _ := randutil.NewPseudoRand()
	const payloadSize = 100
	v := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, payloadSize))

	ts := hlc.NewClock(hlc.UnixNano, time.Nanosecond).Now()
	for _, numEntries := range []int{100, 1000, 10000, 100000} {
		b.Run(strconv.Itoa(numEntries), func(b *testing.B) {
			ctx := context.Background()
			tc := testcluster.StartTestCluster(b, 3, base.TestClusterArgs{})
			defer tc.Stopper().Stop()
			kvDB := tc.Server(0).KVClient().(*client.DB)

			id := keys.MaxReservedDescID + 1
			var batch engine.RocksDBBatchBuilder

			var totalLen int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				prefix := roachpb.Key(keys.MakeTablePrefix(uint32(id)))
				id++
				for j := 0; j < numEntries; j++ {
					key := encoding.EncodeUvarintAscending(prefix, uint64(j))
					v.ClearChecksum()
					v.InitChecksum(key)
					batch.Put(engine.MVCCKey{Key: key, Timestamp: ts}, v.RawBytes)
				}
				end := prefix.PrefixEnd()
				repr := batch.Finish()
				totalLen += int64(len(repr))
				b.StartTimer()

				if err := kvDB.WriteBatch(ctx, prefix, end, repr); err != nil {
					b.Fatalf("%+v", err)
				}
			}
			b.StopTimer()
			b.SetBytes(totalLen / int64(b.N))
		})
	}
}
