// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftlog

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

const rangeID = 1

type mockStorageIter struct {
	val []byte
}

func (m *mockStorageIter) SeekGE(storage.MVCCKey) {}

func (m *mockStorageIter) Valid() (bool, error) {
	return true, nil
}

func (m *mockStorageIter) Next() {}

func (m *mockStorageIter) Close() {}

func (m *mockStorageIter) UnsafeValue() ([]byte, error) {
	return m.val, nil
}

type mockReader struct {
	iter storage.MVCCIterator
}

func (m *mockReader) NewMVCCIterator(
	context.Context, storage.MVCCIterKind, storage.IterOptions,
) (storage.MVCCIterator, error) {
	return m.iter, nil
}

func mkRaftCommand(keySize, valSize, writeBatchSize int) *kvserverpb.RaftCommand {
	r := rand.New(rand.NewSource(123))
	return &kvserverpb.RaftCommand{
		ProposerLeaseSequence: 1,
		MaxLeaseIndex:         1159192591,
		ClosedTimestamp:       &hlc.Timestamp{WallTime: 12512591925, Logical: 1},
		ReplicatedEvalResult: kvserverpb.ReplicatedEvalResult{
			WriteTimestamp: hlc.Timestamp{WallTime: 18581258253},
			Delta: enginepb.MVCCStatsDelta{
				LastUpdateNanos: 123581285,
				LiveBytes:       1000,
				LiveCount:       1,
				KeyBytes:        100,
				KeyCount:        1,
				ValBytes:        900,
				ValCount:        1,
			},
			RaftLogDelta: 1300,
		},
		WriteBatch: &kvserverpb.WriteBatch{Data: randutil.RandBytes(r, writeBatchSize)},
		LogicalOpLog: &kvserverpb.LogicalOpLog{Ops: []enginepb.MVCCLogicalOp{
			{
				WriteValue: &enginepb.MVCCWriteValueOp{
					Key:       roachpb.Key(randutil.RandBytes(r, keySize)),
					Timestamp: hlc.Timestamp{WallTime: 1284581285},
					Value:     roachpb.Key(randutil.RandBytes(r, valSize)),
				},
			},
		}},
	}
}

func mkBenchEnt(b *testing.B) (_ raftpb.Entry, metaB []byte) {
	// A realistic-ish raft command for a ~1kb write.
	cmd := mkRaftCommand(100, 1800, 2000)
	cmdB, err := protoutil.Marshal(cmd)
	require.NoError(b, err)
	data := EncodeCommandBytes(
		EntryEncodingStandardWithoutAC, "cmd12345", cmdB, 0 /* pri */)

	ent := raftpb.Entry{
		Term:  1,
		Index: 1,
		Type:  raftpb.EntryNormal,
		Data:  data,
	}

	e, err := NewEntry(ent)
	require.NoError(b, err)

	metaB, err = e.ToRawBytes()
	require.NoError(b, err)

	return ent, metaB
}

// BenchmarkIterator micro-benchmarks Iterator and its methods. This mocks out
// the storage engine to avoid measuring its overhead.
func BenchmarkIterator(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	b.ReportAllocs()

	_, metaB := mkBenchEnt(b)

	setMockIter := func(it *Iterator) {
		if it.iter != nil {
			it.iter.Close()
		}
		it.iter = &mockStorageIter{
			val: metaB,
		}

	}
	ctx := context.Background()

	b.Run("NewIterator", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it, err := NewIterator(ctx, rangeID, &mockReader{}, IterOptions{Hi: 123456})
			if err != nil {
				b.Fatal(err)
			}
			setMockIter(it)
			it.Close()
		}
	})

	benchForOp := func(b *testing.B, method func(*Iterator) (bool, error)) {
		it, err := NewIterator(ctx, rangeID, &mockReader{}, IterOptions{Hi: 123456})
		if err != nil {
			b.Fatal(err)
		}
		setMockIter(it)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ok, err := method(it)
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				b.Fatal("entry missing")
			}
			e := it.Entry()
			_ = e
		}
	}

	b.Run("Next", func(b *testing.B) {
		benchForOp(b, (*Iterator).Next)
	})

	b.Run("SeekGE", func(b *testing.B) {
		benchForOp(b, (*Iterator).Next)
	})
}

// Visit benchmarks Visit on a pebble engine, i.e. the results will measure
// overhead and allocations inside pebble as well.
func BenchmarkVisit(b *testing.B) {
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	ent, metaB := mkBenchEnt(b)
	require.NoError(b, eng.PutUnversioned(keys.RaftLogKey(rangeID, kvpb.RaftIndex(ent.Index)), metaB))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Visit(ctx, eng, rangeID, 0, math.MaxUint64, func(entry raftpb.Entry) error {
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}
