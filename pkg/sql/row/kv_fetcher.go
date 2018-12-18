// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	batchNumKvs   int64
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
	newSpan = f.newSpan
	f.newSpan = false
	if len(f.kvs) != 0 {
		kv = f.kvs[0]
		f.kvs = f.kvs[1:]
		return true, kv, newSpan, nil
	}
	if f.batchNumKvs > 0 {
		f.batchNumKvs--
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

	var numKeys int64
	ok, f.kvs, f.batchResponse, numKeys, f.span, err = f.nextBatch(ctx)
	if f.batchResponse != nil {
		f.batchNumKvs = numKeys
	}
	if err != nil {
		return ok, kv, false, err
	}
	if !ok {
		return false, kv, false, nil
	}
	f.newSpan = true
	return f.nextKV(ctx)
}
