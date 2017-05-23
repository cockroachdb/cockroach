// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package storageccl

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
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
	defer storage.TestingSetDisableSnapshotClearRange(true)()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	{
		key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("1").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		// Key is before the range in the request span.
		if err := db.ExperimentalAddSSTable(
			ctx, "d", "e", data,
		); !testutils.IsError(err, "not in request range") {
			t.Fatalf("expected request range error got: %+v", err)
		}
		// Key is after the range in the request span.
		if err := db.ExperimentalAddSSTable(
			ctx, "a", "b", data,
		); !testutils.IsError(err, "not in request range") {
			t.Fatalf("expected request range error got: %+v", err)
		}

		if err := db.ExperimentalAddSSTable(ctx, "b", "c", data); err != nil {
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
		key := engine.MVCCKey{Key: []byte("bb2"), Timestamp: hlc.Timestamp{WallTime: 1}}
		data, err := singleKVSSTable(key, roachpb.MakeValueFromString("2").RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if err := db.ExperimentalAddSSTable(ctx, "b", "c", data); err != nil {
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
		key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
		value := roachpb.MakeValueFromString("1")
		value.InitChecksum([]byte("foo"))
		data, err := singleKVSSTable(key, value.RawBytes)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if err := db.ExperimentalAddSSTable(ctx, "b", "c", data); !testutils.IsError(err, "invalid checksum") {
			t.Fatalf("expected 'invalid checksum' error got: %+v", err)
		}
	}
}

func TestAddSSTableMVCCStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer storage.TestingSetDisableSnapshotClearRange(true)()

	ctx := context.Background()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()

	key := engine.MVCCKey{Key: []byte("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
	data, err := singleKVSSTable(key, roachpb.MakeValueFromString("1").RawBytes)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	span := roachpb.Span{Key: []byte("b"), EndKey: []byte("c")}

	// AddSSTable deletes any data that exists in the keyrange before applying
	// the batch. Put something there to delete. The mvcc stats should be
	// adjusted accordingly.
	const numInitialEntries = 100
	for i := 0; i < numInitialEntries; i++ {
		if err := e.Put(engine.MVCCKey{Key: append([]byte("b"), byte(i))}, nil); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	cArgs := storage.CommandArgs{
		Args: &roachpb.AddSSTableRequest{
			Span: span,
			Data: data,
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
	if _, err := evalAddSSTable(ctx, e, cArgs, nil); err != nil {
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

	// TODO(dan): The WriteBatch test runs this again to test the idempotence,
	// but AddSSTable requires the ReplicatedEvalResult to be resolved before
	// this will work. Rewrite this test to use TestServer and add the
	// idempotence test back.
}

func BenchmarkAddSSTable(b *testing.B) {
	defer storage.TestingSetDisableSnapshotClearRange(true)()

	rng, _ := randutil.NewPseudoRand()
	const payloadSize = 100
	v := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, payloadSize))

	ts := hlc.NewClock(hlc.UnixNano, time.Nanosecond).Now()
	for _, numEntries := range []int{100, 1000, 10000, 100000} {
		b.Run(strconv.Itoa(numEntries), func(b *testing.B) {
			ctx := context.Background()
			tc := testcluster.StartTestCluster(b, 3, base.TestClusterArgs{})
			defer tc.Stopper().Stop(ctx)
			kvDB := tc.Server(0).KVClient().(*client.DB)

			id := keys.MaxReservedDescID + 1
			sst, err := engine.MakeRocksDBSstFileWriter()
			if err != nil {
				b.Fatalf("%+v", err)
			}
			defer sst.Close()

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
					kv := engine.MVCCKeyValue{
						Key:   engine.MVCCKey{Key: key, Timestamp: ts},
						Value: v.RawBytes,
					}
					if err := sst.Add(kv); err != nil {
						b.Fatalf("%+v", err)
					}
				}
				end := prefix.PrefixEnd()
				data, err := sst.Finish()
				if err != nil {
					b.Fatalf("%+v", err)
				}
				sst.Close()
				sst, err = engine.MakeRocksDBSstFileWriter()
				if err != nil {
					b.Fatalf("%+v", err)
				}
				totalLen += int64(len(data))
				b.StartTimer()

				if err := kvDB.ExperimentalAddSSTable(ctx, prefix, end, data); err != nil {
					b.Fatalf("%+v", err)
				}
			}
			b.StopTimer()
			b.SetBytes(totalLen / int64(b.N))
		})
	}
}
