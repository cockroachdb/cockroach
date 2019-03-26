// Copyright 2019 The Cockroach Authors.
//
/// Licensed under the Apache License, Version 2.0 (the "License");
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

package bulk

import (
	"bytes"
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// BufferingAdder is a wrapper for an SSTBatcher that allows out-of-order calls
// to Add, buffering them up and then sorting them before then passing them in
// order into an SSTBatcher
type BufferingAdder struct {
	SSTBatcher
	timestamp      hlc.Timestamp
	skipDuplicates bool

	flushSize int64

	// currently buffered kvs.
	curBuf kvsByKey
	// estimated memory usage of curBuf.
	curBufSize int64
}

const kvOverhead = 24 + 24 // 2 slice headers, each assuming each is 8 + 8 + 8.

// MakeBulkAdder makes a storagebase.BulkAdder that buffers and sorts K/Vs passed
// to add into SSTs that are then ingested.
func MakeBulkAdder(
	db *client.DB,
	rangeCache *kv.RangeDescriptorCache,
	flushBytes, sstBytes int64,
	timestamp hlc.Timestamp,
) (*BufferingAdder, error) {
	b := &BufferingAdder{
		SSTBatcher: SSTBatcher{db: db, maxSize: sstBytes, rc: rangeCache},
		timestamp:  timestamp,
		flushSize:  flushBytes,
	}
	return b, nil
}

// SkipLocalDuplicates configures skipping of duplicate keys in local batches.
func (b *BufferingAdder) SkipLocalDuplicates(skip bool) {
	b.skipDuplicates = skip
}

// Add adds a key to the buffer and checks if it needs to flush.
func (b *BufferingAdder) Add(ctx context.Context, key roachpb.Key, value []byte) error {
	if len(b.curBuf) == 0 {
		if err := b.SSTBatcher.Reset(); err != nil {
			return err
		}
	}
	b.curBuf = append(b.curBuf, kvPair{key, value})
	b.curBufSize += int64(cap(key)+cap(value)) + kvOverhead

	if b.curBufSize > b.flushSize {
		return b.Flush(ctx)
	}
	return nil
}

// Flush flushes any buffered kvs to the batcher.
func (b *BufferingAdder) Flush(ctx context.Context) error {
	if len(b.curBuf) == 0 {
		return nil
	}
	sort.Sort(b.curBuf)
	for i, kv := range b.curBuf {
		if b.skipDuplicates && i > 0 && bytes.Equal(b.curBuf[i-1].key, kv.key) {
			continue
		}

		if err := b.AddMVCCKey(ctx, engine.MVCCKey{Key: kv.key, Timestamp: b.timestamp}, kv.value); err != nil {
			if i > 0 && bytes.Equal(b.curBuf[i-1].key, kv.key) {
				return storagebase.DuplicateKeyError{Key: kv.key, Value: kv.value}
			}
			return err
		}
	}
	b.curBufSize = 0
	b.curBuf = b.curBuf[:0]
	return b.SSTBatcher.Flush(ctx)
}

// kvPair is a bytes -> bytes kv pair.
type kvPair struct {
	key   roachpb.Key
	value []byte
}

type kvsByKey []kvPair

// Len implements sort.Interface.
func (kvs kvsByKey) Len() int {
	return len(kvs)
}

// Less implements sort.Interface.
func (kvs kvsByKey) Less(i, j int) bool {
	return bytes.Compare(kvs[i].key, kvs[j].key) < 0
}

// Swap implements sort.Interface.
func (kvs kvsByKey) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}

var _ sort.Interface = kvsByKey{}
