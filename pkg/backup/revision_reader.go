// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
func GetAllRevisions(
	ctx context.Context,
	db *kv.DB,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	allRevs chan []VersionedValues,
) error {
	for {
		header := kvpb.Header{
			Timestamp:                   endTime,
			ReturnElasticCPUResumeSpans: true,
		}
		req := &kvpb.ExportRequest{
			RequestHeader: kvpb.RequestHeader{Key: startKey, EndKey: endKey},
			StartTime:     startTime,
			MVCCFilter:    kvpb.MVCCFilter_All,
		}
		resp, pErr := kv.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
		if pErr != nil {
			return pErr.GoError()
		}

		exportResp := resp.(*kvpb.ExportResponse)
		var res []VersionedValues
		for _, file := range exportResp.Files {
			iterOpts := storage.IterOptions{
				KeyTypes:   storage.IterKeyTypePointsOnly,
				LowerBound: file.Span.Key,
				UpperBound: file.Span.EndKey,
			}
			iter, err := storage.NewMemSSTIterator(file.SST, true, iterOpts)
			if err != nil {
				return err
			}
			//nolint:deferloop TODO(#137605)
			defer func() {
				if iter != nil {
					iter.Close()
				}
			}()
			iter.SeekGE(storage.MVCCKey{Key: startKey})

			for ; ; iter.Next() {
				if valid, err := iter.Valid(); !valid || err != nil {
					if err != nil {
						return err
					}
					break
				} else if iter.UnsafeKey().Key.Compare(endKey) >= 0 {
					break
				}
				key := iter.UnsafeKey()
				keyCopy := make([]byte, len(key.Key))
				copy(keyCopy, key.Key)
				key.Key = keyCopy
				v, err := iter.UnsafeValue()
				if err != nil {
					return err
				}
				value := make([]byte, len(v))
				copy(value, v)
				if len(res) == 0 || !res[len(res)-1].Key.Equal(key.Key) {
					res = append(res, VersionedValues{Key: key.Key})
				}
				res[len(res)-1].Values = append(res[len(res)-1].Values, roachpb.Value{Timestamp: key.Timestamp, RawBytes: value})
			}

			// Close and nil out the iter to release the underlying resources.
			iter.Close()
			iter = nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case allRevs <- res:
		}

		// Check if the ExportRequest paginated with a resume span.
		if exportResp.ResumeSpan == nil {
			return nil
		}
		startKey = exportResp.ResumeSpan.Key
	}
}
