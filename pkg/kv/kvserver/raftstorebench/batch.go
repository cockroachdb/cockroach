// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstorebench

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type writeOptions struct {
	cfg      Config
	keyLen   int
	valueLen int
	batchLen int
	raftEng  *storage.Pebble
	smEng    *storage.Pebble
}

type writeBatches struct {
	raftBatch storage.Batch
	smBatch   storage.Batch
}

func generateBatches(
	ctx context.Context, o writeOptions, r *replicaWriteState, rng *rand.Rand,
) (writeBatches, int64, int64, error) { // keyBytes, valBytes
	size := len(r.keyPrefix) + o.keyLen + o.valueLen
	if cap(r.buf) < size {
		r.buf = make([]byte, 0, size)
	}
	raftBatch := o.raftEng.NewBatch()
	smBatch := o.smEng.NewBatch()

	{
		r.hs.Commit = r.nextRaftLogIndex - 1
		n := r.hs.Size()
		if cap(r.hsBuf) < n {
			r.hsBuf = make([]byte, n)
		}
		if _, err := r.hs.MarshalToSizedBuffer(r.hsBuf[:n:n]); err != nil {
			return writeBatches{}, 0, 0, err
		}
	}

	if err := raftBatch.PutUnversioned(r.rangeIDPrefixBuf.RaftHardStateKey(), r.hsBuf); err != nil {
		return writeBatches{}, 0, 0, err
	}
	var keyBytes int64
	var valBytes int64
	for i := 0; i < o.batchLen; i++ {
		keyBytes += int64(len(r.keyPrefix) + o.keyLen)
		valBytes += int64(o.valueLen)

		r.buf = r.buf[:0]
		key := append(r.buf, r.keyPrefix...)
		randKey := key[len(key) : len(key)+o.keyLen]
		rng.Read(randKey)
		key = key[0 : len(key)+o.keyLen]
		value := r.buf[len(key):size]
		rng.Read(value)
		tmpBatch := o.smEng.NewBatch()
		var ms enginepb.MVCCStats
		_, err := storage.MVCCBlindPut(ctx, tmpBatch, key, hlc.Timestamp{WallTime: int64(r.nextRaftLogIndex)},
			roachpb.Value{RawBytes: value}, storage.MVCCWriteOptions{Stats: &ms})
		if err != nil {
			return writeBatches{}, 0, 0, err
		}
		batchBytes := tmpBatch.Repr()
		if err := smBatch.ApplyBatchRepr(batchBytes, false); err != nil {
			panic(err)
		}
		err = raftBatch.PutUnversioned(r.rangeIDPrefixBuf.RaftLogKey(kvpb.RaftIndex(r.nextRaftLogIndex)), batchBytes)
		if err != nil {
			return writeBatches{}, 0, 0, err
		}
		r.nextRaftLogIndex++
		r.logSizeBytes += int64(len(batchBytes))
		tmpBatch.Close()
	}
	// TODO: fully populate the applied state (bogus data ok).
	appliedState := kvserverpb.RangeAppliedState{RaftAppliedIndex: kvpb.RaftIndex(r.nextRaftLogIndex - 1)}
	appliedStateBytes, err := protoutil.Marshal(&appliedState)
	if err != nil {
		return writeBatches{}, 0, 0, err
	}
	if err = smBatch.PutUnversioned(r.rangeIDPrefixBuf.RangeAppliedStateKey(), appliedStateBytes); err != nil {
		return writeBatches{}, keyBytes, valBytes, err
	}
	return writeBatches{smBatch: smBatch, raftBatch: raftBatch}, keyBytes, valBytes, nil
}
