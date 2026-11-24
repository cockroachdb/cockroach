// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrdecoder

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
)

// CoalescingDecoder takes a KV from the source cluster and decodes it into datums
// that are appropriate for the destination table. If there are multiple KVs for
// the same primary key in a batch, it coalesces them into a single event.
type CoalescingDecoder struct {
	decoder tableDecoder

	// TODO(jeffswenson): clean this interface up. There's a problem with
	// layering that requires the event decoder to know about the most recent
	// row. If a batch fails to process, its broken up into batches of size 1 and
	// those are retried until they succeed or are DLQ'd. The batch handler is
	// responsible for decoding rows, but the distsql processor is responsible
	// for calling the batch handler and adding rows to the DLQ.
	LastRow cdcevent.Row
}

func NewCoalescingDecoder(
	ctx context.Context, descriptors descs.DB, settings *cluster.Settings, tables []TableMapping,
) (*CoalescingDecoder, error) {
	decoder, err := newTableDecoder(ctx, descriptors, settings, tables)
	if err != nil {
		return nil, err
	}
	return &CoalescingDecoder{
		decoder: decoder,
	}, nil
}

// decodeAndCoalesceEvents returns the decoded events sorted by key and
// deduplicated to a single event for each primary key.
func (d *CoalescingDecoder) DecodeAndCoalesceEvents(
	ctx context.Context,
	batch []streampb.StreamEvent_KV,
	discard jobspb.LogicalReplicationDetails_Discard,
) ([]DecodedRow, error) {
	// Basic idea:
	// 1. Sort the batch so the keys and mvcc timestamps are in ascending order. Sorting by key
	//    ensures that all events for a given table and row are adjacent. Sorting by timestamp
	//    ensures that if i < j then row[i] comes before row[j] in application time.
	// 2. For eacy row in the batch, decode the first row and use it as the previous value. We use
	//    the earliest row as the previous value because as long as the batch is not a replay, the
	//    previous value of the first instance of the row is expected to match the local value.
	// 3. For the last event for each row, decode it as the value to insert.

	toDecode := make([]streampb.StreamEvent_KV, 0, len(batch))
	for _, event := range batch {
		// Discard deletes before sorting and coalescing updates. Its possible that
		// the correct previous value was attached to a deleted event. That's okay because:
		// 1. The previous value is only a guess at the previous value. It does not have to match
		//    the actual local value.
		// 2. DELETE -> INSERT isn't expected to be a super common pattern. Trying to coalesce the previous value
		//    and discard deletes is more complex than just discarding them.
		if discard == jobspb.LogicalReplicationDetails_DiscardAllDeletes && event.KeyValue.Value.RawBytes == nil {
			continue
		}

		toDecode = append(toDecode, event)
	}

	if len(toDecode) == 0 {
		return nil, nil
	}

	sort.Slice(toDecode, func(i, j int) bool {
		cmp := toDecode[i].KeyValue.Key.Compare(toDecode[j].KeyValue.Key)
		if cmp != 0 {
			return cmp < 0
		}
		return toDecode[i].KeyValue.Value.Timestamp.Less(toDecode[j].KeyValue.Value.Timestamp)
	})

	var result []DecodedRow

	first, last := toDecode[0], toDecode[0]
	for _, event := range toDecode[1:] {
		if event.KeyValue.Key.Compare(first.KeyValue.Key) != 0 {
			decoded, err := d.decodeEvent(ctx, first, last)
			if err != nil {
				return nil, err
			}
			result = append(result, decoded)
			first, last = event, event
		} else {
			last = event
		}
	}
	decoded, err := d.decodeEvent(ctx, first, last)
	if err != nil {
		return nil, err
	}
	result = append(result, decoded)

	return result, nil
}

func (d *CoalescingDecoder) decodeEvent(
	ctx context.Context, first streampb.StreamEvent_KV, last streampb.StreamEvent_KV,
) (DecodedRow, error) {
	coalesced := last
	coalesced.PrevValue = first.PrevValue
	event, cdcEvent, err := d.decoder.decodeEvent(ctx, coalesced)
	if err != nil {
		return DecodedRow{}, err
	}
	d.LastRow = cdcEvent
	return event, nil
}
