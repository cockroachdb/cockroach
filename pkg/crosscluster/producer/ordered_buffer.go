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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// encodeTimestampKey encodes (timestamp, key) as a sortable composite key.
// Format: 8 bytes WallTime (big-endian) + 4 bytes Logical (big-endian) + key.
// key must be non-empty to avoid duplicates.
func encodeTimestampKey(ts hlc.Timestamp, key roachpb.Key) []byte {
	buf := make([]byte, 0, 12+len(key))
	buf = binary.BigEndian.AppendUint64(buf, uint64(ts.WallTime))
	buf = binary.BigEndian.AppendUint32(buf, uint32(ts.Logical))
	buf = append(buf, key...)
	return buf
}

// decodeTimestampKey decodes a composite key into (timestamp, key).
func decodeTimestampKey(encoded []byte) (hlc.Timestamp, error) {
	if len(encoded) < 12 {
		return hlc.Timestamp{}, errors.AssertionFailedf("key too short: got %d bytes", len(encoded))
	}
	if len(encoded) == 12 {
		return hlc.Timestamp{}, errors.AssertionFailedf("encoded key missing rangefeed key suffix (contains timestamp only)")
	}
	ts := hlc.Timestamp{
		WallTime: int64(binary.BigEndian.Uint64(encoded[0:8])),
		Logical:  int32(binary.BigEndian.Uint32(encoded[8:12])),
	}
	return ts, nil
}

// OrderedBufferConfig holds dependencies for the disk-backed buffer.
type OrderedBufferConfig struct {
	settings               *cluster.Settings
	streamID               streampb.StreamID
	tempStorage            diskmap.Factory
	flushByteSizeThreshold int64
}

// bufferedEvent is the in-memory form of one event before flush.
type bufferedEvent struct {
	ts  hlc.Timestamp
	val kvpb.RangeFeedEvent
}

// OrderedBuffer buffers rangefeed KVs and delete ranges in a SortedDiskMap
// using (timestamp, key) ordering for deduplication. Disk keys are encoded
// (timestamp, key) composites; values are marshaled RangeFeedEvents.
type OrderedBuffer struct {
	cfg           OrderedBufferConfig
	events        []bufferedEvent
	diskMap       diskmap.SortedDiskMap
	minTsInBuffer hlc.Timestamp // Min timestamp in events; empty when none.
	inMemoryBytes int64
}

func newOrderedBuffer(cfg OrderedBufferConfig) *OrderedBuffer {
	return &OrderedBuffer{cfg: cfg, events: make([]bufferedEvent, 0)}
}

// addEvent appends an event to the buffer, updates size and minTs, and maybe flushes.
func (b *OrderedBuffer) addEvent(
	ctx context.Context, ts hlc.Timestamp, val *kvpb.RangeFeedEvent,
) error {
	b.events = append(b.events, bufferedEvent{ts: ts, val: *val})
	b.inMemoryBytes += int64(val.Size())
	if b.minTsInBuffer.IsEmpty() || ts.Less(b.minTsInBuffer) {
		b.minTsInBuffer = ts
	}
	return b.maybeFlushToDisk(ctx)
}

// Add appends a rangefeed value to the in-memory buffer.
func (b *OrderedBuffer) Add(ctx context.Context, v *kvpb.RangeFeedValue) error {
	ts := v.Value.Timestamp
	return b.addEvent(ctx, ts, &kvpb.RangeFeedEvent{
		Val: v,
	})
}

// AddDelRange appends a delete range to the in-memory buffer for ordered delivery.
func (b *OrderedBuffer) AddDelRange(ctx context.Context, d *kvpb.RangeFeedDeleteRange) error {
	ts := d.Timestamp
	return b.addEvent(ctx, ts, &kvpb.RangeFeedEvent{
		DeleteRange: d,
	})
}

// MaybeFlushToDisk flushes when the buffer size exceeds the configured threshold,
func (b *OrderedBuffer) maybeFlushToDisk(ctx context.Context) error {
	if b.cfg.flushByteSizeThreshold > b.inMemoryBytes {
		return nil
	}
	return b.FlushToDisk(ctx, hlc.MaxTimestamp)
}

// Len returns the number of events (KVs + DelRanges) in the memory buffer.
func (b *OrderedBuffer) Len() int {
	return len(b.events)
}

// FlushToDisk flushes the in-memory buffer to the disk map only when
// resolvedTs >= minTsInBuffer.
func (b *OrderedBuffer) FlushToDisk(ctx context.Context, resolvedTs hlc.Timestamp) (err error) {
	if len(b.events) == 0 {
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
	for _, e := range b.events {
		var key roachpb.Key
		switch {
		case e.val.Val != nil:
			key = e.val.Val.Key
		case e.val.DeleteRange != nil:
			key = e.val.DeleteRange.Span.Key
		default:
			return errors.AssertionFailedf("unexpected RangeFeedEvent variant: %v", e.val)
		}
		if len(key) == 0 {
			return errors.AssertionFailedf("rangefeed event has empty key for ordered buffer indexing")
		}

		tsKey := encodeTimestampKey(e.ts, key)
		valBytes, err := protoutil.Marshal(&e.val)
		if err != nil {
			return err
		}
		if err := writer.Put(tsKey, valBytes); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	b.events = b.events[:0]
	b.inMemoryBytes = 0
	b.minTsInBuffer = hlc.Timestamp{}
	return nil
}

// GetEventsFromDisk returns RangeFeedEvents where ts <= resolvedTimestamp from disk
// in (timestamp, key) order, deletes them from the map, and returns once the batch
// threshold is reached.
func (b *OrderedBuffer) GetEventsFromDisk(
	ctx context.Context, resolvedTimestamp hlc.Timestamp,
) (_ []kvpb.RangeFeedEvent, _ bool, retErr error) {
	if b.diskMap == nil {
		return nil, true, nil
	}
	iter := b.diskMap.NewIterator()
	defer iter.Close()

	var result []kvpb.RangeFeedEvent
	var lastKey []byte
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
		rawVal := iter.UnsafeValue()
		resultByteSize += int64(len(rawVal))
		ts, err := decodeTimestampKey(rawKey)
		if err != nil {
			return nil, false, err
		}
		if ts.Compare(resolvedTimestamp) > 0 {
			iterExhausted = true
			break
		}
		lastKey = append([]byte(nil), rawKey...)

		// Copy rawVal before unmarshaling since UnsafeValue() returns memory
		// that's only valid until the next iterator operation. RangeFeedEvent
		// contains byte slice fields that may retain references.
		valCopy := append([]byte(nil), rawVal...)
		var event kvpb.RangeFeedEvent
		if err := protoutil.Unmarshal(valCopy, &event); err != nil {
			return nil, false, err
		}

		if event.Val == nil && event.DeleteRange == nil {
			return nil, false, errors.AssertionFailedf("unexpected RangeFeedEvent variant: %v", event)
		}
		result = append(result, event)
		if resultByteSize >= b.cfg.flushByteSizeThreshold {
			break
		}
		iter.Next()
	}
	// Remove returned events from disk.
	if lastKey != nil {
		writer := b.diskMap.NewBatchWriter()
		defer func() {
			retErr = errors.CombineErrors(retErr, writer.Close(ctx))
		}()
		endExclusive := append(roachpb.Key(nil), lastKey...).Next()
		if err := writer.DeleteRange(nil, endExclusive); err != nil {
			return nil, false, err
		}
	}
	if len(result) == 0 {
		return nil, iterExhausted, nil
	}
	return result, iterExhausted, nil
}

// Close closes the disk map.
func (b *OrderedBuffer) Close(ctx context.Context) error {
	if b.diskMap != nil {
		b.diskMap.Close(ctx)
		b.diskMap = nil
	}
	return nil
}
