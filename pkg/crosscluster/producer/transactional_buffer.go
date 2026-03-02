// Copyright 2025 The Cockroach Authors.
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

const defaultTransactionalBufferFlushThreshold = 25

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
type transactionalBufferConfig struct {
	settings       *cluster.Settings
	streamID       streampb.StreamID
	tempStorage    diskmap.Factory
	flushThreshold int
}

// transactionalBuffer buffers rangefeed KVs and flushes them to a SortedDiskMap
// using (timestamp, key) ordering for chronological iteration.
// It only flushes when resolvedTs >= minTsInBuffer (at least one buffer entry is
// resolved). If resolvedTs < minTsInBuffer, every buffer entry has ts > resolvedTs.
type transactionalBuffer struct {
	cfg           transactionalBufferConfig
	buffer        []storage.MVCCKeyValue
	diskMap       diskmap.SortedDiskMap
	minTsInBuffer hlc.Timestamp // min timestamp in buffer; empty when buffer empty
}

func newTransactionalBuffer(cfg transactionalBufferConfig) *transactionalBuffer {
	if cfg.flushThreshold <= 0 {
		cfg.flushThreshold = defaultTransactionalBufferFlushThreshold
	}
	return &transactionalBuffer{
		cfg:    cfg,
		buffer: make([]storage.MVCCKeyValue, 0),
	}
}

// Add appends a rangefeed value to the in-memory buffer.
func (b *transactionalBuffer) Add(ctx context.Context, v *kvpb.RangeFeedValue) error {
	ts := v.Value.Timestamp
	b.buffer = append(b.buffer, storage.MVCCKeyValue{
		Key:   storage.MVCCKey{Key: v.Key, Timestamp: ts},
		Value: v.Value.RawBytes,
	})
	if b.minTsInBuffer.IsEmpty() || ts.Less(b.minTsInBuffer) {
		b.minTsInBuffer = ts
	}
	return b.MaybeFlushToDisk(ctx)
}

// MaybeFlushToDisk flushes when the buffer size exceeds the configured threshold,
// to bound memory usage.
func (b *transactionalBuffer) MaybeFlushToDisk(ctx context.Context) error {
	if len(b.buffer) < b.cfg.flushThreshold {
		return nil
	}
	return b.FlushToDisk(ctx, hlc.MaxTimestamp)
}

// Len returns the number of KVs in the buffer.
func (b *transactionalBuffer) Len() int {
	return len(b.buffer)
}

// FlushToDisk flushes the in-memory buffer to disk only when resolvedTs >= minTsInBuffer.
// If resolvedTs < minTsInBuffer, every buffer entry has ts > resolvedTs; skip flushing.
func (b *transactionalBuffer) FlushToDisk(
	ctx context.Context, resolvedTs hlc.Timestamp,
) (err error) {
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

func (b *transactionalBuffer) GetMVCCValuesFromDisk(
	ctx context.Context, resolvedTimestamp hlc.Timestamp,
) (keyValues []storage.MVCCKeyValue, err error) {
	if b.diskMap == nil {
		return nil, nil
	}
	iter := b.diskMap.NewIterator()
	defer iter.Close()

	var result []storage.MVCCKeyValue
	iter.Rewind()
	for {
		ok, err := iter.Valid()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		rawKey := iter.UnsafeKey()
		ts, key, err := decodeTimestampKey(rawKey)
		if err != nil {
			return nil, err
		}
		if ts.Compare(resolvedTimestamp) > 0 {
			break
		}
		value := append([]byte(nil), iter.UnsafeValue()...)
		result = append(result, storage.MVCCKeyValue{
			Key:   storage.MVCCKey{Key: key, Timestamp: ts},
			Value: value,
		})
		iter.Next()
	}
	return result, nil
}

// DeleteResolvedFromDisk removes all keys with timestamp <= resolvedTimestamp
// from the disk map.
func (b *transactionalBuffer) DeleteResolvedFromDisk(
	ctx context.Context, resolvedTimestamp hlc.Timestamp,
) (err error) {
	if b.diskMap == nil {
		return nil
	}
	iter := b.diskMap.NewIterator()
	defer iter.Close()

	writer := b.diskMap.NewBatchWriter()
	defer func() {
		err = errors.CombineErrors(err, writer.Close(ctx))
	}()

	iter.Rewind()
	for {
		ok, err := iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		rawKey := iter.UnsafeKey()
		ts, _, err := decodeTimestampKey(rawKey)
		if err != nil {
			return err
		}
		if ts.Compare(resolvedTimestamp) > 0 {
			break
		}
		if err := writer.Delete(rawKey); err != nil {
			return err
		}
		iter.Next()
	}
	return nil
}

func (b *transactionalBuffer) Close(ctx context.Context) error {
	if b.diskMap != nil {
		b.diskMap.Close(ctx)
		b.diskMap = nil
	}
	return nil
}
