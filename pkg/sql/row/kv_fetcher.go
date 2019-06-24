// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

// kvFetcher wraps kvBatchFetcher, providing a nextKV interface that returns the
// next kv from its input.
type kvFetcher struct {
	kvBatchFetcher

	kvs []roachpb.KeyValue

	batchResponse []byte
	bytesRead     int64
	span          roachpb.Span
	newSpan       bool
}

func newKVFetcher(batchFetcher kvBatchFetcher) kvFetcher {
	return kvFetcher{
		kvBatchFetcher: batchFetcher,
	}
}

// nextKV returns the next kv from this fetcher. Returns false if there are no
// more kvs to fetch, the kv that was fetched, and any errors that may have
// occurred.
func (f *kvFetcher) nextKV(
	ctx context.Context,
) (ok bool, kv roachpb.KeyValue, newSpan bool, err error) {
	for {
		newSpan = f.newSpan
		f.newSpan = false
		if len(f.kvs) != 0 {
			kv = f.kvs[0]
			f.kvs = f.kvs[1:]
			return true, kv, newSpan, nil
		}
		if len(f.batchResponse) > 0 {
			var key []byte
			var rawBytes []byte
			var err error
			key, rawBytes, f.batchResponse, err = enginepb.ScanDecodeKeyValueNoTS(f.batchResponse)
			if err != nil {
				return false, kv, false, err
			}
			return true, roachpb.KeyValue{
				Key: key,
				Value: roachpb.Value{
					RawBytes: rawBytes,
				},
			}, newSpan, nil
		}

		ok, f.kvs, f.batchResponse, f.span, err = f.nextBatch(ctx)
		if err != nil {
			return ok, kv, false, err
		}
		if !ok {
			return false, kv, false, nil
		}
		f.newSpan = true
		f.bytesRead += int64(len(f.batchResponse))
	}
}
