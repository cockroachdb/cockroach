// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/errors"
)

// VerifyBatchRepr asserts that all keys in a BatchRepr are between the specified
// start and end keys and computes the enginepb.MVCCStats for it.
func VerifyBatchRepr(
	repr []byte, start, end storage.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	// We store a 4 byte checksum of each key/value entry in the value. Make
	// sure all the ones in this BatchRepr validate.
	var kvs []storage.MVCCKeyValue

	r, err := storage.NewRocksDBBatchReader(repr)
	if err != nil {
		return enginepb.MVCCStats{}, errors.Wrapf(err, "verifying key/value checksums")
	}
	for r.Next() {
		switch r.BatchType() {
		case storage.BatchTypeValue:
			mvccKey, err := r.MVCCKey()
			if err != nil {
				return enginepb.MVCCStats{}, errors.Wrapf(err, "verifying key/value checksums")
			}
			v := roachpb.Value{RawBytes: r.Value()}
			if err := v.Verify(mvccKey.Key); err != nil {
				return enginepb.MVCCStats{}, err
			}
			kvs = append(kvs, storage.MVCCKeyValue{
				Key:   mvccKey,
				Value: v.RawBytes,
			})
		default:
			return enginepb.MVCCStats{}, errors.Errorf(
				"unexpected entry type in batch: %d", r.BatchType())
		}
	}
	if err := r.Error(); err != nil {
		return enginepb.MVCCStats{}, errors.Wrapf(err, "verifying key/value checksums")
	}
	if len(kvs) == 0 {
		return enginepb.MVCCStats{}, nil
	}
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key.Less(kvs[j].Key)
	})
	// Check the first and last keys for being within range.
	if kvs[0].Key.Less(start) {
		return enginepb.MVCCStats{}, errors.Errorf("key not in request range: %s", kvs[0].Key.String())
	}
	if end.Less(kvs[len(kvs)-1].Key) {
		return enginepb.MVCCStats{}, errors.Errorf("key not in request range: %s", kvs[len(kvs)-1].Key.String())
	}
	// Generate an SST out of these kvs. Then, instantiate a MemSSTIterator.
	var memFile storage.MemFile
	writer := storage.MakeIngestionSSTWriter(&memFile)
	for _, kv := range kvs {
		if err := writer.Put(kv.Key, kv.Value); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	if err := writer.Finish(); err != nil {
		return enginepb.MVCCStats{}, err
	}
	writer.Close()
	iter, err := storage.NewMemSSTIterator(memFile.Data(), false)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	defer iter.Close()

	return storage.ComputeStatsGo(iter, start.Key, end.Key, nowNanos)
}
