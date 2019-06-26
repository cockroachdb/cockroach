// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// indexSkipTableReader is a processor that retrieves distinct rows from
// a table using the prefix of an index to skip reading some rows in the
// table. Specifically, given a prefix of an index to distinct over,
// the indexSkipTableReader returns all distinct rows where that prefix
// of the index is distinct. It uses the index to seek to distinct values
// of the prefix instead of doing a full table scan.
// As of now, the indexSkipTableReader does not support use with
// interleaved tables.
type indexSkipTableReader struct {
	ProcessorBase

	spans roachpb.Spans

	// currentSpan maintains which span we are currently scanning.
	currentSpan int

	// keyPrefixLen holds the length of the prefix of the index
	// that we are performing a distinct over.
	keyPrefixLen int
	// indexLen holds the number of columns in the index that
	// is being considered.
	indexLen int

	ignoreMisplannedRanges bool
	misplannedRanges       []roachpb.RangeInfo

	fetcher row.Fetcher
	alloc   sqlbase.DatumAlloc
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

	returnMutations := spec.Visibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	types := spec.Table.ColumnTypesWithMutations(returnMutations)
	t.ignoreMisplannedRanges = flowCtx.local

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

	immutDesc := sqlbase.NewImmutableTableDescriptor(spec.Table)
	index, isSecondaryIndex, err := immutDesc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, err
	}
	t.indexLen = len(index.ColumnIDs)

	cols := immutDesc.Columns
	if returnMutations {
		cols = immutDesc.ReadableColumns
	}

	tableArgs := row.FetcherTableArgs{
		Desc:             immutDesc,
		Index:            index,
		ColIdxMap:        columnIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             cols,
		ValNeededForCol:  neededColumns,
	}

	// TODO: support reverse scans
	if err := t.fetcher.Init(false /* reverseScan */, true, /* returnRangeInfo */
		false /* isCheck */, &t.alloc, tableArgs); err != nil {
		return nil, err
	}

	// Make a copy of the spans for this reader, as we will modify them
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

		// Start a scan to get the smallest value within this span
		err := t.fetcher.StartScan(
			t.Ctx, t.flowCtx.txn, t.spans[t.currentSpan:t.currentSpan+1],
			true, 1 /* batch size limit */, t.flowCtx.traceKV,
		)
		if err != nil {
			t.MoveToDraining(err)
			return nil, &distsqlpb.ProducerMetadata{Err: err}
		}

		// range info resets once a scan begins, so we need to maintain
		// the range info we get after each scan
		if !t.ignoreMisplannedRanges {
			ranges := misplannedRanges(t.Ctx, t.fetcher.GetRangesInfo(), t.flowCtx.nodeID)
			for _, r := range ranges {
				t.misplannedRanges = roachpb.InsertRangeInfo(t.misplannedRanges, r)
			}
		}

		key, err := t.fetcher.PartialKey(t.keyPrefixLen)
		if err != nil {
			t.MoveToDraining(err)
			return nil, &distsqlpb.ProducerMetadata{Err: err}
		}

		row, _, _, err := t.fetcher.NextRow(t.Ctx)
		if err != nil {
			t.MoveToDraining(err)
			return nil, &distsqlpb.ProducerMetadata{Err: err}
		}
		if row == nil {
			// no more rows in this span, so move to the next one!
			t.currentSpan++
			continue
		}

		// 0xff is the largest prefix marker for any encoded key. To ensure that
		// our new key is larger than any value with the same prefix, we place
		// 0xff at all other index column values, and one more to guard against
		// 0xff present as a value in the table (0xff encodes a type of null)
		for i := 0; i < (t.indexLen - t.keyPrefixLen + 1); i++ {
			key = append(key, 0xff)
		}

		t.spans[t.currentSpan].Key = key
		if !t.spans[t.currentSpan].Valid() {
			t.currentSpan++
			continue
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
		ProcessorBase:    t.ProcessorBase,
		fetcher:          t.fetcher,
		spans:            t.spans[:0],
		misplannedRanges: t.misplannedRanges[:0],
		currentSpan:      0,
	}
	istrPool.Put(t)
}

func (t *indexSkipTableReader) ConsumerClosed() {
	t.InternalClose()
}

func (t *indexSkipTableReader) generateTrailingMeta(
	ctx context.Context,
) []distsqlpb.ProducerMetadata {
	trailingMeta := t.generateMeta(ctx)
	t.InternalClose()
	return trailingMeta
}

func (t *indexSkipTableReader) generateMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	var trailingMeta []distsqlpb.ProducerMetadata
	if !t.ignoreMisplannedRanges {
		if len(t.misplannedRanges) != 0 {
			trailingMeta = append(trailingMeta, distsqlpb.ProducerMetadata{Ranges: t.misplannedRanges})
		}
	}
	if meta := getTxnCoordMeta(ctx, t.flowCtx.txn); meta != nil {
		trailingMeta = append(trailingMeta, distsqlpb.ProducerMetadata{TxnCoordMeta: meta})
	}
	return trailingMeta
}

func (t *indexSkipTableReader) DrainMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	return t.generateMeta(ctx)
}
