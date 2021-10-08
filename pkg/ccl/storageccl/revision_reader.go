// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// VersionedValues is similar to roachpb.KeyValue except instead of just the
// value at one time, it contains all the retrieved revisions of the value for
// the key, with the value timestamps set accordingly.
type VersionedValues struct {
	Key    roachpb.Key
	Values []roachpb.Value
}

// GetAllRevisions scans all keys between startKey and endKey getting all
// revisions between startTime and endTime.
// TODO(dt): if/when client gets a ScanRevisionsRequest or similar, use that.
func GetAllRevisions(
	ctx context.Context, db *kv.DB, startKey, endKey roachpb.Key, startTime, endTime hlc.Timestamp,
) ([]VersionedValues, error) {
	// TODO(dt): version check.
	header := roachpb.Header{Timestamp: endTime}
	req := &roachpb.ExportRequest{
		RequestHeader: roachpb.RequestHeader{Key: startKey, EndKey: endKey},
		StartTime:     startTime,
		MVCCFilter:    roachpb.MVCCFilter_All,
		ReturnSST:     true,
	}
	resp, pErr := kv.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
	if pErr != nil {
		return nil, pErr.GoError()
	}

	var res []VersionedValues
	for _, file := range resp.(*roachpb.ExportResponse).Files {
		iter, err := storage.NewMemSSTIterator(file.SST, false)
		if err != nil {
			return nil, err
		}
		defer iter.Close()
		iter.SeekGE(storage.MVCCKey{Key: startKey})

		for ; ; iter.Next() {
			if valid, err := iter.Valid(); !valid || err != nil {
				if err != nil {
					return nil, err
				}
				break
			} else if iter.UnsafeKey().Key.Compare(endKey) >= 0 {
				break
			}
			key := iter.UnsafeKey()
			keyCopy := make([]byte, len(key.Key))
			copy(keyCopy, key.Key)
			key.Key = keyCopy
			value := make([]byte, len(iter.UnsafeValue()))
			copy(value, iter.UnsafeValue())
			if len(res) == 0 || !res[len(res)-1].Key.Equal(key.Key) {
				res = append(res, VersionedValues{Key: key.Key})
			}
			res[len(res)-1].Values = append(res[len(res)-1].Values, roachpb.Value{Timestamp: key.Timestamp, RawBytes: value})
		}
	}
	return res, nil
}
