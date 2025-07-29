// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// eventDecoder takes a KV from the source cluster and decodes it into datums
// that are appropriate for the destination table.
type eventDecoder struct {
	decoder   cdcevent.Decoder
	srcToDest map[descpb.ID]destinationTable

	// TODO(jeffswenson): clean this interface up. There's a problem with
	// layering that requires the event decoder to know about the most recent
	// row. If a batch fails to process, its broken up into batches of size 1 and
	// those are retried until they succeed or are DLQ'd. The batch handler is
	// responsible for decoding rows, but the distsql processor is responsible
	// for calling the batch handler and adding rows to the DLQ.
	lastRow cdcevent.Row
}

// decodedEvent is constructed from a replication stream event. The replication
// stream event is encoded using the source table descriptor. The decoded must
// contain datums that are compatible with the destination table descriptor.
type decodedEvent struct {
	// dstDescID is the descriptor ID for the table in the destination cluster.
	dstDescID descpb.ID

	// isDelete is true if the event is a delete. This implies values in the row
	// may be NULL that are not allowed to be NULL in the source table's schema.
	// Only the primary key columns are expected to have values.
	isDelete bool

	// originTimestamp is the mvcc timestamp of the row in the source cluster.
	originTimestamp hlc.Timestamp

	// row is the decoded row from the replication stream. The datums in row are
	// in col id order for the destination table.
	row tree.Datums

	// prevRow is either the previous row from the replication stream or it is
	// the local version of the row if there was a read refresh.
	//
	// The datums in prevRow are in col id order for the destination table. nil
	// prevRow may still lose LWW if there is a recent tombstone.
	prevRow tree.Datums
}

func newEventDecoder(
	ctx context.Context,
	descriptors descs.DB,
	settings *cluster.Settings,
	procConfigByDestID map[descpb.ID]sqlProcessorTableConfig,
) (*eventDecoder, error) {
	srcToDest := make(map[descpb.ID]destinationTable, len(procConfigByDestID))
	err := descriptors.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		for dstID, s := range procConfigByDestID {
			descriptor, err := txn.Descriptors().GetLeasedImmutableTableByID(ctx, txn.KV(), dstID)
			if err != nil {
				return err
			}

			columns := getColumnSchema(descriptor)
			columnNames := make([]string, 0, len(columns))
			for _, column := range columns {
				columnNames = append(columnNames, column.column.GetName())
			}

			srcToDest[s.srcDesc.GetID()] = destinationTable{
				id:      dstID,
				columns: columnNames,
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	decoder, err := newCdcEventDecoder(ctx, procConfigByDestID, settings)
	if err != nil {
		return nil, err
	}

	return &eventDecoder{
		decoder:   decoder,
		srcToDest: srcToDest,
	}, nil
}

// decodeAndCoalesceEvents returns the decoded events sorted by key and
// deduplicated to a single event for each primary key.
func (d *eventDecoder) decodeAndCoalesceEvents(
	ctx context.Context,
	batch []streampb.StreamEvent_KV,
	discard jobspb.LogicalReplicationDetails_Discard,
) ([]decodedEvent, error) {
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

	var result []decodedEvent

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

func (d *eventDecoder) decodeEvent(
	ctx context.Context, first streampb.StreamEvent_KV, last streampb.StreamEvent_KV,
) (decodedEvent, error) {
	decodedRow, err := d.decoder.DecodeKV(ctx, last.KeyValue, cdcevent.CurrentRow, last.KeyValue.Value.Timestamp, false)
	if err != nil {
		return decodedEvent{}, err
	}

	dstTable, ok := d.srcToDest[decodedRow.TableID]
	if !ok {
		return decodedEvent{}, errors.AssertionFailedf("table %d not found", decodedRow.TableID)
	}

	row := make(tree.Datums, 0, len(dstTable.columns))
	row, err = appendDatums(row, decodedRow, dstTable.columns)
	if err != nil {
		return decodedEvent{}, err
	}
	d.lastRow = decodedRow

	var prevKV roachpb.KeyValue
	prevKV.Key = first.KeyValue.Key
	prevKV.Value = first.PrevValue

	decodedPrevRow, err := d.decoder.DecodeKV(ctx, prevKV, cdcevent.PrevRow, first.PrevValue.Timestamp, false)
	if err != nil {
		return decodedEvent{}, err
	}

	prevRow := make(tree.Datums, 0, len(dstTable.columns))
	prevRow, err = appendDatums(prevRow, decodedPrevRow, dstTable.columns)
	if err != nil {
		return decodedEvent{}, err
	}

	return decodedEvent{
		dstDescID:       dstTable.id,
		isDelete:        decodedRow.IsDeleted(),
		originTimestamp: last.KeyValue.Value.Timestamp,
		row:             row,
		prevRow:         prevRow,
	}, nil
}

// appendDatums appends datums for the specified column names from the cdcevent.Row
// to the datums slice and returns the updated slice.
func appendDatums(datums tree.Datums, row cdcevent.Row, columnNames []string) (tree.Datums, error) {
	it, err := row.DatumsNamed(columnNames)
	if err != nil {
		return nil, err
	}

	if err := it.Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		if dEnum, ok := d.(*tree.DEnum); ok {
			// Override the type to Unknown to avoid a mismatched type OID error
			// during execution. Note that Unknown is the type used by default
			// when a SQL statement is executed without type hints.
			//
			// TODO(jeffswenson): this feels like the wrong place to do this,
			// but its inspired by the implementation in queryBuilder.AddRow.
			//
			// Really we should be mapping from the source datum type to the
			// destination datum type.
			dEnum.EnumTyp = types.Unknown
		}
		datums = append(datums, d)
		return nil
	}); err != nil {
		return nil, err
	}

	return datums, nil
}

type destinationTable struct {
	id      descpb.ID
	columns []string
}
