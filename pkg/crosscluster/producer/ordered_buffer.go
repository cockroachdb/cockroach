// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"context"
	"encoding/binary"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// encodeTimestampKey encodes (ts, key) for sort order ts asc, then key asc.
// Format: 8 bytes WallTime (big-endian) + 4 bytes Logical (big-endian) + key.
func encodeTimestampKey(ts hlc.Timestamp, key roachpb.Key) []byte {
	buf := make([]byte, 0, 12+len(key))
	buf = binary.BigEndian.AppendUint64(buf, uint64(ts.WallTime))
	buf = binary.BigEndian.AppendUint32(buf, uint32(ts.Logical))
	buf = append(buf, key...)
	return buf
}

// decodeTimestampKey reverses encodeTimestampKey.
func decodeTimestampKey(encoded []byte) (hlc.Timestamp, roachpb.Key, error) {
	if len(encoded) < 12 {
		return hlc.Timestamp{}, nil, errors.New("encoded key too short")
	}
	ts := hlc.Timestamp{
		WallTime: int64(binary.BigEndian.Uint64(encoded[0:8])),
		Logical:  int32(binary.BigEndian.Uint32(encoded[8:12])),
	}
	return ts, append(roachpb.Key(nil), encoded[12:]...), nil
}

// transactionalBufferConfig holds dependencies for the disk-backed buffer.
type OrderedBufferConfig struct {
	settings               *cluster.Settings
	streamID               streampb.StreamID
	tempStorage            diskmap.Factory
	flushByteSizeThreshold int64
}

// transactionalBuffer buffers rangefeed KVs and flushes them to a SortedDiskMap
// using (timestamp, key) ordering for chronological iteration.
// It only flushes when resolvedTs >= minTsInBuffer (at least one buffer entry is
// resolved). If resolvedTs < minTsInBuffer, every buffer entry has ts > resolvedTs.
type OrderedBuffer struct {
	cfg           OrderedBufferConfig
	buffer        []storage.MVCCKeyValue
	diskMap       diskmap.SortedDiskMap
	minTsInBuffer hlc.Timestamp // min timestamp in buffer; empty when buffer empty
	totalByteSize int64
}

func newOrderedBuffer(cfg OrderedBufferConfig) *OrderedBuffer {
	return &OrderedBuffer{
		cfg:    cfg,
		buffer: make([]storage.MVCCKeyValue, 0),
	}
}

// Add appends a rangefeed value to the in-memory buffer.
func (b *OrderedBuffer) Add(ctx context.Context, v *kvpb.RangeFeedValue) error {
	ts := v.Value.Timestamp
	mvccKeyValue := storage.MVCCKeyValue{
		Key:   storage.MVCCKey{Key: v.Key, Timestamp: ts},
		Value: v.Value.RawBytes,
	}
	b.buffer = append(b.buffer, mvccKeyValue)
	b.totalByteSize += int64(mvccKeyValue.Key.EncodedSize() + len(mvccKeyValue.Value))
	if b.minTsInBuffer.IsEmpty() || ts.Less(b.minTsInBuffer) {
		b.minTsInBuffer = ts
	}
	return b.MaybeFlushToDisk(ctx)
}

// MaybeFlushToDisk flushes when the buffer size exceeds the configured threshold,
// to bound memory usage.
func (b *OrderedBuffer) MaybeFlushToDisk(ctx context.Context) error {
	if b.cfg.flushByteSizeThreshold > b.totalByteSize {
		return nil
	}
	err := b.FlushToDisk(ctx, hlc.MaxTimestamp)
	if err != nil {
		return err
	}
	b.totalByteSize = 0
	return nil
}

// Len returns the number of KVs in the buffer.
func (b *OrderedBuffer) Len() int {
	return len(b.buffer)
}

// FlushToDisk flushes the in-memory buffer to disk only when resolvedTs >= minTsInBuffer.
// If resolvedTs < minTsInBuffer, every buffer entry has ts > resolvedTs; skip flushing.
func (b *OrderedBuffer) FlushToDisk(ctx context.Context, resolvedTs hlc.Timestamp) (err error) {
	if len(b.buffer) == 0 {
		return nil
	}
	if resolvedTs.Less(b.minTsInBuffer) {
		return nil
	}
	if b.diskMap == nil {
		b.diskMap = b.cfg.tempStorage.NewSortedDiskMap()
	}
	writer := b.diskMap.NewBatchWriter()
	defer func() {
		err = errors.CombineErrors(err, writer.Close(ctx))
	}()
	for _, kv := range b.buffer {
		encodedKey := encodeTimestampKey(kv.Key.Timestamp, kv.Key.Key)
		if err := writer.Put(encodedKey, kv.Value); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	b.buffer = b.buffer[:0]
	b.minTsInBuffer = hlc.Timestamp{}
	return nil
}

func (b *OrderedBuffer) GetKVsFromDisk(
	ctx context.Context, resolvedTimestamp hlc.Timestamp,
) ([]*kvpb.RangeFeedValue, bool, error) {
	if b.diskMap == nil {
		return nil, false, nil
	}
	iter := b.diskMap.NewIterator()
	defer iter.Close()

	var result []*kvpb.RangeFeedValue
	var keysToDelete [][]byte
	resultByteSize := int64(0)
	iterExhausted := false

	iter.Rewind()
	for {
		ok, err := iter.Valid()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			iterExhausted = true
			break
		}
		rawKey := iter.UnsafeKey()
		resultByteSize += int64(len(rawKey)) + int64(len(iter.UnsafeValue()))
		ts, key, err := decodeTimestampKey(rawKey)
		if err != nil {
			return nil, false, err
		}
		if ts.Compare(resolvedTimestamp) > 0 {
			iterExhausted = true
			break
		}
		keysToDelete = append(keysToDelete, append([]byte(nil), rawKey...))
		value := append([]byte(nil), iter.UnsafeValue()...)
		result = append(result, &kvpb.RangeFeedValue{
			Key:   key,
			Value: roachpb.Value{RawBytes: value, Timestamp: ts},
		})
		if resultByteSize > b.cfg.flushByteSizeThreshold {
			break
		}
		iter.Next()
	}
	// Remove returned keys from disk.
	if len(keysToDelete) > 0 {
		writer := b.diskMap.NewBatchWriter()
		for _, k := range keysToDelete {
			if err := writer.Delete(k); err != nil {
				return nil, false, err
			}
		}
		if err := writer.Close(ctx); err != nil {
			return nil, false, err
		}
	}
	return result, iterExhausted, nil
}

func (b *OrderedBuffer) Close(ctx context.Context) error {
	if b.diskMap != nil {
		b.diskMap.Close(ctx)
		b.diskMap = nil
	}
	return nil
}
