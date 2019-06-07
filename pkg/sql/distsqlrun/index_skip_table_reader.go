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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

type indexSkipTableReader struct {
	ProcessorBase

	spans roachpb.Spans

	// maintains which span we are currently
	// getting rows from
	currentSpan  int
	keyPrefixLen int

	limitHint int64

	maxResults uint64

	// unsure how to take this into account as of now
	maxTimestampAge time.Duration

	// unsure how to take this into account as of now
	ignoreMisplannedRanges bool

	// need to figure out how to use the below
	input   RowSource
	fetcher row.Fetcher
	alloc   sqlbase.DatumAlloc

	// used for debug
	count int
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
			InputsToDrain: nil,
			// TODO: update this function call
			TrailingMetaCallback: nil,
		},
	); err != nil {
		return nil, err
	}

	neededColumns := t.out.neededColumns()
	t.keyPrefixLen = neededColumns.Len()
	t.count = 0

	columnIdxMap := spec.Table.ColumnIdxMapWithMutations(returnMutations)

	immutDesc := sqlbase.NewImmutableTableDescriptor(spec.Table)
	index, isSecondaryIndex, err := immutDesc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		// TODO
		return nil, err
	}

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

// TODO: implement
func (t *indexSkipTableReader) Start(ctx context.Context) context.Context {
	t.StartInternal(ctx, indexSkipTableReaderProcName)
	return ctx
}

// TODO: implement
// TODO: move this logic into a row fetcher like the table reader?
func (t *indexSkipTableReader) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	if t.State == StateRunning {

		// if our current span is invalid, bump up current span
		if t.currentSpan < len(t.spans) &&
			!t.spans[t.currentSpan].Valid() {
			t.currentSpan++
		}

		// if we have no more spans to look at
		// return nil and start draining
		if t.currentSpan >= len(t.spans) {
			t.MoveToDraining(nil /* err */)
			return nil, t.DrainHelper()
		}

		oldSpan := t.spans[t.currentSpan]

		// start a scan on the fetcher for our span
		// TODO: not worrying about inconsistent scans right now
		err := t.fetcher.StartScan(
			t.Ctx, t.flowCtx.txn, []roachpb.Span{t.spans[t.currentSpan]},
			true /* limitBatches */, t.limitHint, t.flowCtx.traceKV,
		)

		if err != nil {
			// TODO: !!
			t.MoveToDraining(err)
			return nil, t.DrainHelper()
		}

		// use the fetcher to get the next key + row in our span
		var key roachpb.Key
		var row sqlbase.EncDatumRow
		for t.State == StateRunning {
			key, err = t.fetcher.PartialKey(t.keyPrefixLen)
			// key, err = t.fetcher.PartialKey(1)
			// key = t.fetcher.Key()
			// fmt.Println("testpartial", tkey.String())
			if err != nil {
				// TODO: if the key is nil, do we move along
				// to the next span?
				// maybe need to restructure this loop then...
				t.MoveToDraining(nil /* err */)
				return nil, t.DrainHelper()
			}

			row, _, _, err = t.fetcher.NextRow(t.Ctx)
			if row == nil {
				// TODO: if the row is nil, do we move along
				// to the next span?
				// maybe need to restructure this loop then...
				// TODO: deal with this when we have many spans
				t.MoveToDraining(nil /* err */)
				return nil, t.DrainHelper()
			}
			if err != nil {
				return nil, &distsqlpb.ProducerMetadata{Err: err}
			}
			if row != nil && err == nil {
				// fmt.Println(row.String(t.OutputTypes()))
				break
			}
		}

		// use Key.NextKey to get the next largest key for us
		newKey := key.Next()
		_ = newKey
		t.count++

		test, _, err := encoding.DecomposeKeyTokens(key)
		if err != nil {
			panic(err)
		}
		var d sqlbase.DatumAlloc
		ty := types.Int
		last := test[len(test)-1]
		data, _, _ := sqlbase.DecodeTableKey(&d, ty, last, encoding.Ascending)
		fmt.Println("DATUM VALUE", data.String())
		newDatum, _ := data.Next(nil)

		var newKeyDatum roachpb.Key
		for i, kp := range test {
			if i != len(test)-1 {
				newKeyDatum = append(newKeyDatum, kp...)
			}
		}

		encDatum, _ := sqlbase.EncodeTableKey([]byte{}, newDatum, encoding.Ascending)
		newKeyDatum = append(newKeyDatum, encDatum...)

		// adjust our span to have the same end key, but the next largest key
		t.spans[t.currentSpan].Key = newKeyDatum
		fmt.Printf("Old key %s, new key %s\n", key.String(), newKeyDatum.String())
		fmt.Printf("Old span %s, new span %s\n", oldSpan.String(), t.spans[t.currentSpan].String())

		// if t.count == 5 {
		// 	panic("wei!")
		// }

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

// TODO: implement
func (t *indexSkipTableReader) ConsumerDone() {

}

// TODO: implement
func (t *indexSkipTableReader) ConsumerClosed() {

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
