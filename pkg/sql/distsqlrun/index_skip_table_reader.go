// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package distsqlrun

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

type indexSkipTableReader struct {
	ProcessorBase

	spans roachpb.Spans

	// maintains which span we are currently
	// getting rows from
	currentSpan int

	// how much of the primary key prefix we are
	// doing the distinct over
	keyPrefixLen int

	limitHint int64

	maxResults uint64

	// unsure how to take this into account as of now
	maxTimestampAge time.Duration

	// unsure how to take this into account as of now
	ignoreMisplannedRanges bool

	fetcher row.Fetcher
	alloc   sqlbase.DatumAlloc

	decodedDatums tree.Datums

	tableInfo sqlbase.TableDescriptor
	indexInfo *sqlbase.IndexDescriptor
	colIdxMap map[sqlbase.ColumnID]int
}

const indexSkipTableReaderProcName = "index skip table reader"

var istrPool = sync.Pool{
	New: func() interface{} {
		return &indexSkipTableReader{}
	},
}

var _ Processor = &indexSkipTableReader{}
var _ RowSource = &indexSkipTableReader{}
var _ distsqlpb.MetadataSource = &indexSkipTableReader{}

// TODO: implement
func newIndexSkipTableReader(
	flowCtx *FlowCtx,
	processorID int32,
	spec *distsqlpb.IndexSkipTableReaderSpec,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (*indexSkipTableReader, error) {
	if flowCtx.nodeID == 0 {
		return nil, errors.Errorf("attempting to create a tableReader with uninitialized NodeID")
	}

	t := istrPool.Get().(*indexSkipTableReader)

	t.currentSpan = 0
	t.tableInfo = spec.Table

	// hardcode right now to just get size 1 batches
	t.limitHint = limitHint(1, post)

	returnMutations := spec.Visibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	types := spec.Table.ColumnTypesWithMutations(returnMutations)

	if err := t.Init(
		t,
		post,
		types,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		ProcStateOpts{
			InputsToDrain:        nil,
			TrailingMetaCallback: t.generateTrailingMeta,
		},
	); err != nil {
		return nil, err
	}

	neededColumns := t.out.neededColumns()
	t.keyPrefixLen = neededColumns.Len()

	columnIdxMap := spec.Table.ColumnIdxMapWithMutations(returnMutations)
	t.colIdxMap = columnIdxMap

	immutDesc := sqlbase.NewImmutableTableDescriptor(spec.Table)
	index, isSecondaryIndex, err := immutDesc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, err
	}
	t.indexInfo = index

	cols := immutDesc.Columns
	if returnMutations {
		cols = immutDesc.ReadableColumns
	}

	t.decodedDatums = make(tree.Datums, len(cols))

	tableArgs := row.FetcherTableArgs{
		Desc:             immutDesc,
		Index:            index,
		ColIdxMap:        columnIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             cols,
		ValNeededForCol:  neededColumns,
	}

	// we aren't supporting reverse scans right now using this method
	if err := t.fetcher.Init(false /* reverseScan */, true, /* returnRangeInfo */
		spec.IsCheck, &t.alloc, tableArgs); err != nil {
		return nil, err
	}

	// add spans to this tableReader
	nSpans := len(spec.Spans)
	if cap(t.spans) >= nSpans {
		t.spans = t.spans[:nSpans]
	} else {
		t.spans = make(roachpb.Spans, nSpans)
	}
	for i, s := range spec.Spans {
		t.spans[i] = s.Span
	}

	return t, nil
}

func (t *indexSkipTableReader) Start(ctx context.Context) context.Context {
	t.StartInternal(ctx, indexSkipTableReaderProcName)
	return ctx
}

func (t *indexSkipTableReader) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for t.State == StateRunning {
		if t.currentSpan >= len(t.spans) {
			t.MoveToDraining(nil)
			return nil, t.DrainHelper()
		}
		if !t.spans[t.currentSpan].Valid() {
			t.currentSpan++
			continue
		}

		// do a scan
		err := t.fetcher.StartScan(
			t.Ctx, t.flowCtx.txn, []roachpb.Span{t.spans[t.currentSpan]},
			true, t.limitHint, t.flowCtx.traceKV,
		)
		if err != nil {
			t.MoveToDraining(err)
			return nil, &distsqlpb.ProducerMetadata{Err: err}
		}

		key, err := t.fetcher.PartialKey(t.keyPrefixLen)
		if err != nil {
			t.MoveToDraining(err)
			return nil, &distsqlpb.ProducerMetadata{Err: err}
		}

		row, _, _, err := t.fetcher.NextRow(t.Ctx)
		if row == nil {
			// no more rows in this span, so move to the next one!
			t.currentSpan++
			continue
		}
		if err != nil {
			t.MoveToDraining(err)
			return nil, &distsqlpb.ProducerMetadata{Err: err}
		}

		// decode the datum row here
		for i, encDatum := range row {
			if encDatum.IsUnset() {
				t.decodedDatums[i] = tree.DNull
				continue
			}
			if err := encDatum.EnsureDecoded(&t.tableInfo.Columns[i].Type, &t.alloc); err != nil {
				t.MoveToDraining(err)
				return nil, &distsqlpb.ProducerMetadata{Err: err}
			}
			t.decodedDatums[i] = encDatum.Datum
		}

		keyColIdx := t.keyPrefixLen - 1
		direction := encoding.Ascending
		if t.indexInfo.ColumnDirections[keyColIdx] == sqlbase.IndexDescriptor_DESC {
			direction = encoding.Descending
		}
		colID := t.indexInfo.ColumnIDs[keyColIdx]
		ordinal := t.colIdxMap[colID]

		datum := t.decodedDatums[ordinal]

		// TODO: deal with getting next or previous depending on the
		// direction
		newDatum, nextExists := datum.Next(t.evalCtx)

		// only construct a new span if the next value exists,
		// otherwise move onto the next span
		if nextExists {
			decomposedKey, _, err := encoding.DecomposeKeyTokens(key)
			if err != nil {
				t.MoveToDraining(err)
				return nil, &distsqlpb.ProducerMetadata{Err: err}
			}
			newEncDatum, err := sqlbase.EncodeTableKey([]byte{}, newDatum, direction)
			if err != nil {
				t.MoveToDraining(err)
				return nil, &distsqlpb.ProducerMetadata{Err: err}
			}
			var newKey roachpb.Key
			for i, kp := range decomposedKey {
				if i != len(decomposedKey)-1 {
					newKey = append(newKey, kp...)
				}
			}
			newKey = append(newKey, newEncDatum...)
			t.spans[t.currentSpan].Key = newKey
		} else {
			t.currentSpan++
		}
		if outRow := t.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, t.DrainHelper()
}

func (t *indexSkipTableReader) Release() {
	t.ProcessorBase.Reset()
	t.fetcher.Reset()
	*t = indexSkipTableReader{
		ProcessorBase: t.ProcessorBase,
		fetcher:       t.fetcher,
		spans:         t.spans[:0],
	}
	istrPool.Put(t)
}

func (t *indexSkipTableReader) ConsumerClosed() {
	t.InternalClose()
}

func (t *indexSkipTableReader) generateTrailingMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	trailingMeta := t.generateMeta(ctx)
	t.InternalClose()
	return trailingMeta
}

func (t *indexSkipTableReader) generateMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	var trailingMeta []distsqlpb.ProducerMetadata
	if meta := getTxnCoordMeta(ctx, t.flowCtx.txn); meta != nil {
		trailingMeta = append(trailingMeta, distsqlpb.ProducerMetadata{TxnCoordMeta: meta})
	}
	return trailingMeta
}

func (t *indexSkipTableReader) DrainMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	return t.generateMeta(ctx)
}
