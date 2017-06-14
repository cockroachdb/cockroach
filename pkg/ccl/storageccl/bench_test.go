// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package storageccl_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

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

func BenchmarkWriteBatch(b *testing.B) {
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
