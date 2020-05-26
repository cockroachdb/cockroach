// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mvcc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

// Replacement for mvcc.go. Incomplete.
// This assumes the lockTable has already dealt with conflicting locks, though errors
// can still happen due to uncertainty etc.
// We don't need to remove mvcc.go in one-shot since the code there to deal with
// conflicting intents is harmless. We can transition callers one at a time and
// incrementally remove dead code from mvcc.go

func MVCCPut2(
	ctx context.Context,
	rw concurrency.MVCCReadWriter,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value []byte,
	txn *roachpb.Transaction,
) error {
	rw.SeekGE(key)
	var err error
	var valid bool
	if valid, err = rw.Valid(); err != nil {
		return err
	}
	writeTimestamp := timestamp
	if txn != nil {
		writeTimestamp = txn.WriteTimestamp
	}
	if valid {
		prevKey := rw.UnsafeKey()
		if rw.IsMyKeyValue() {
			if writeTimestamp.Less(prevKey.Timestamp) {
				writeTimestamp = prevKey.Timestamp
			}
		} else if writeTimestamp.LessEq(prevKey.Timestamp) {
			writeTimestamp = prevKey.Timestamp.Next()
			err = roachpb.NewWriteTooOldError(timestamp, writeTimestamp)
		}
	}
	if err2 := rw.Put(storage.MVCCKey{Key: key, Timestamp: writeTimestamp}, value, &txn.TxnMeta); err2 != nil {
		return err2
	}
	return err
}

func MVCCGet2(
	ctx context.Context, reader concurrency.MVCCReadWriter, key roachpb.Key,
) (*roachpb.Value, error) {
	reader.SeekGE(key)
	if valid, err := reader.Valid(); err != nil || !valid {
		return nil, err
	}
	value := &roachpb.Value{
		RawBytes:  append([]byte(nil), reader.UnsafeValue()...),
		Timestamp: reader.UnsafeKey().Timestamp,
	}
	return value, nil
}

type MVCCScanOptions2 struct {
	Reverse          bool
	FailOnMoreRecent bool
	// MaxKeys is the maximum number of kv pairs returned from this operation.
	// The zero value represents an unbounded scan. If the limit stops the scan,
	// a corresponding ResumeSpan is returned. As a special case, the value -1
	// returns no keys in the result (returning the first key via the
	// ResumeSpan).
	MaxKeys int64
	// TargetBytes is a byte threshold to limit the amount of data pulled into
	// memory during a Scan operation. Once the target is satisfied (i.e. met or
	// exceeded) by the emitted emitted KV pairs, iteration stops (with a
	// ResumeSpan as appropriate). In particular, at least one kv pair is
	// returned (when one exists).
	//
	// The number of bytes a particular kv pair accrues depends on internal data
	// structures, but it is guaranteed to exceed that of the bytes stored in
	// the key and value itself.
	//
	// The zero value indicates no limit.
	TargetBytes int64
}

func MVCCScan2(
	ctx context.Context,
	reader concurrency.MVCCReadWriter,
	timestamp hlc.Timestamp,
	endKey roachpb.Key,
	opts MVCCScanOptions2,
) (storage.MVCCScanResult, error) {
	// TODO: implement opts.Reverse
	var res storage.MVCCScanResult
	for {
		if valid, err := reader.Valid(); err != nil || !valid {
			return res, err
		}
		if opts.FailOnMoreRecent && !reader.IsMyKeyValue() && timestamp.Less(reader.UnsafeKey().Timestamp) {
			return res, errors.Errorf("todo")
		}
		if (opts.MaxKeys != 0 && opts.MaxKeys <= res.NumKeys) ||
			(opts.TargetBytes != 0 && opts.TargetBytes <= res.NumBytes) {
			break
		}
		kv := roachpb.KeyValue{
			Key: append([]byte(nil), reader.UnsafeKey().Key...),
			Value: roachpb.Value{
				RawBytes:  append([]byte(nil), reader.UnsafeValue()...),
				Timestamp: reader.UnsafeKey().Timestamp,
			},
		}
		res.KVs = append(res.KVs, kv)
		res.NumKeys++
		res.NumBytes += int64(len(reader.UnsafeValue()))
	}
	res.ResumeSpan = &roachpb.Span{
		Key:    append(roachpb.Key(nil), reader.UnsafeKey().Key...),
		EndKey: endKey,
	}
	return res, nil
}

func MVCCScan2ToBytes(
	ctx context.Context,
	reader concurrency.MVCCReadWriter,
	timestamp hlc.Timestamp,
	endKey roachpb.Key,
	opts MVCCScanOptions2,
) (storage.MVCCScanResult, error) {
	// TODO
	return storage.MVCCScanResult{}, nil
}
