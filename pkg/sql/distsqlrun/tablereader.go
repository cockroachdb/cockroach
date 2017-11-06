// Copyright 2016 The Cockroach Authors.
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

package distsqlrun

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// ScrubTypes is the schema for TableReaders that are doing a SCRUB
// check. This schema is what TableReader output streams are overrided
// to for check. The column types correspond to:
// - Error type.
// - Primary key as a string, if it was obtainable.
// - JSON of all decoded column values.
var ScrubTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_STRING},
	{SemanticType: sqlbase.ColumnType_STRING},
	// FIXME(joey): Changethis to be type.JSON once the JSON column type
	// has been added.
	{SemanticType: sqlbase.ColumnType_STRING},
}

// tableReader is the start of a computation flow; it performs KV operations to
// retrieve rows for a table, runs a filter expression, and passes rows with the
// desired column values to an output RowReceiver.
// See docs/RFCS/distributed_sql.md
type tableReader struct {
	processorBase

	flowCtx *FlowCtx

	tableDesc sqlbase.TableDescriptor
	spans     roachpb.Spans
	limitHint int64

	fetcher sqlbase.RowFetcher
	alloc   sqlbase.DatumAlloc

	isCheck bool
	// fetcherResultToColIdx maps the corresponding fetcher results to the
	// column index in the TableDescriptor.
	// NB: This is only initialized and used during SCRUB physical checks.
	fetcherResultToColIdx []int
	indexIdx              int
}

var _ Processor = &tableReader{}

// newTableReader creates a tableReader.
func newTableReader(
	flowCtx *FlowCtx, spec *TableReaderSpec, post *PostProcessSpec, output RowReceiver,
) (*tableReader, error) {
	if flowCtx.nodeID == 0 {
		return nil, errors.Errorf("attempting to create a tableReader with uninitialized NodeID")
	}
	tr := &tableReader{
		flowCtx:   flowCtx,
		tableDesc: spec.Table,
		isCheck:   spec.IsCheck,
		indexIdx:  int(spec.IndexIdx),
	}

	// We ignore any limits that are higher than this value to avoid any
	// overflows.
	const overflowProtection = 1000000000
	if post.Limit != 0 && post.Limit <= overflowProtection {
		// In this case the ProcOutputHelper will tell us to stop once we emit
		// enough rows.
		tr.limitHint = int64(post.Limit)
	} else if spec.LimitHint != 0 && spec.LimitHint <= overflowProtection {
		// If it turns out that limiHint rows are sufficient for our consumer, we
		// want to avoid asking for another batch. Currently, the only way for us to
		// "stop" is if we block on sending rows and the consumer sets
		// ConsumerDone() on the RowChannel while we block. So we want to block
		// *after* sending all the rows in the limit hint; to do this, we request
		// rowChannelBufSize + 1 more rows:
		//  - rowChannelBufSize rows guarantee that we will fill the row channel
		//    even after limitHint rows are consumed
		//  - the extra row gives us chance to call Push again after we unblock,
		//    which will notice that ConsumerDone() was called.
		//
		// This flimsy mechanism is only useful in the (optimistic) case that the
		// processor that only needs this many rows is our direct, local consumer.
		// If we have a chain of processors and RowChannels, or remote streams, this
		// reasoning goes out the door.
		//
		// TODO(radu, andrei): work on a real mechanism for limits.
		tr.limitHint = spec.LimitHint + rowChannelBufSize + 1
	}

	if post.Filter.Expr != "" {
		// We have a filter so we will likely need to read more rows.
		tr.limitHint *= 2
	}

	types := make([]sqlbase.ColumnType, len(spec.Table.Columns))
	for i := range types {
		types[i] = spec.Table.Columns[i].Type
	}

	if spec.IsCheck {
		types = ScrubTypes
	}
	if err := tr.out.Init(post, types, &flowCtx.EvalCtx, output); err != nil {
		return nil, err
	}

	neededColumns := tr.out.neededColumns()

	// When doing a SCRUB check the neededColumns will reflect the
	// ScrubTypes output schema. It instead needs to be overrided to be
	// the columns in the scan.
	// FIXME(joey): Add the case for doing a SCRUB check on a secondary
	// index.
	if tr.isCheck && spec.IndexIdx == 0 {
		neededColumns = make([]bool, len(spec.Table.Columns))
		for i := range spec.Table.Columns {
			neededColumns[i] = true
			tr.fetcherResultToColIdx = append(tr.fetcherResultToColIdx, i)
		}
	}

	if _, _, err := initRowFetcher(
		&tr.fetcher, &tr.tableDesc, int(spec.IndexIdx), spec.Reverse,
		neededColumns, spec.IsCheck, &tr.alloc,
	); err != nil {
		return nil, err
	}

	tr.spans = make(roachpb.Spans, len(spec.Spans))
	for i, s := range spec.Spans {
		tr.spans[i] = s.Span
	}

	return tr, nil
}

func initRowFetcher(
	fetcher *sqlbase.RowFetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	reverseScan bool,
	valNeededForCol []bool,
	isCheck bool,
	alloc *sqlbase.DatumAlloc,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	// indexIdx is 0 for the primary index, or 1 to <num-indexes> for a
	// secondary index.
	if indexIdx < 0 || indexIdx > len(desc.Indexes) {
		return nil, false, errors.Errorf("invalid indexIdx %d", indexIdx)
	}

	if indexIdx > 0 {
		index = &desc.Indexes[indexIdx-1]
		isSecondaryIndex = true
	} else {
		index = &desc.PrimaryIndex
	}

	colIdxMap := make(map[sqlbase.ColumnID]int, len(desc.Columns))
	for i, c := range desc.Columns {
		colIdxMap[c.ID] = i
	}
	if err := fetcher.Init(
		desc, colIdxMap, index, reverseScan, false /* lockForUpdate */, isSecondaryIndex,
		desc.Columns, valNeededForCol, true /* returnRangeInfo */, isCheck, alloc,
	); err != nil {
		return nil, false, err
	}
	return index, isSecondaryIndex, nil
}

// sendMisplannedRangesMetadata sends information about the non-local ranges
// that were read by this tableReader. This should be called after the fetcher
// was used to read everything this tableReader was supposed to read.
func (tr *tableReader) sendMisplannedRangesMetadata(ctx context.Context) {
	rangeInfos := tr.fetcher.GetRangeInfo()
	var misplannedRanges []roachpb.RangeInfo
	for _, ri := range rangeInfos {
		if ri.Lease.Replica.NodeID != tr.flowCtx.nodeID {
			misplannedRanges = append(misplannedRanges, ri)
		}
	}
	if len(misplannedRanges) != 0 {
		var msg string
		if len(misplannedRanges) < 3 {
			msg = fmt.Sprintf("%+v", misplannedRanges[0].Desc)
		} else {
			msg = fmt.Sprintf("%+v...", misplannedRanges[:3])
		}
		log.VEventf(ctx, 2, "tableReader pushing metadata about misplanned ranges: %s",
			msg)
		tr.out.output.Push(nil /* row */, ProducerMetadata{Ranges: misplannedRanges})
	}
}

// Run is part of the processor interface.
func (tr *tableReader) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTagInt(ctx, "TableReader", int(tr.tableDesc.ID))
	ctx, span := processorSpan(ctx, "table reader")
	defer tracing.FinishSpan(span)

	txn := tr.flowCtx.txn
	if txn == nil {
		log.Fatalf(ctx, "joinReader outside of txn")
	}

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	if err := tr.fetcher.StartScan(
		ctx, txn, tr.spans, true /* limit batches */, tr.limitHint, false, /* traceKV */
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		tr.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
		tr.out.Close()
		return
	}

	for {
		var fetcherRow sqlbase.EncDatumRow
		var err error
		if tr.isCheck {
			fetcherRow, err = tr.fetcher.NextRowWithErrors(ctx)
			if v, ok := err.(*scrub.Error); ok {
				// FIXME(joey): This code is a bit unclear. We overwrite err so
				// that it is only a non-scrubbable error, so the err != nil and
				// EmitRow handles this properly.
				fetcherRow, err = tr.generateScrubErrorRow(fetcherRow, v)
			} else if err == nil && fetcherRow != nil {
				continue
			}
		} else {
			fetcherRow, err = tr.fetcher.NextRow(ctx)
		}
		if err != nil || fetcherRow == nil {
			if err != nil {
				tr.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
			}
			break
		}
		// Emit the row; stop if no more rows are needed.
		consumerStatus, err := tr.out.EmitRow(ctx, fetcherRow)
		if err != nil || consumerStatus != NeedMoreRows {
			if err != nil {
				tr.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
			}
			break
		}
	}
	tr.sendMisplannedRangesMetadata(ctx)
	sendTraceData(ctx, tr.out.output)
	tr.out.Close()
}

func (tr *tableReader) generateScrubErrorRow(
	row sqlbase.EncDatumRow, scrubErr *scrub.Error,
) (sqlbase.EncDatumRow, error) {

	details := make(map[string]interface{})

	// Collect all the row values into JSON
	rowDetails := make(map[string]interface{})
	for i, colIdx := range tr.fetcherResultToColIdx {
		col := tr.tableDesc.Columns[colIdx]
		// TODO(joey): We should maybe try to get the underlying type.
		rowDetails[col.Name] = row[i].String(&col.Type)
	}
	details["row_data"] = rowDetails

	// Get the index name.
	if tr.indexIdx == 0 {
		details["index_name"] = tr.tableDesc.PrimaryIndex.Name
	} else {
		details["index_name"] = tr.tableDesc.Indexes[tr.indexIdx-1]
	}

	detailsJSON, err := tree.MakeDJSON(details)
	if err != nil {
		return nil, err
	}

	return sqlbase.EncDatumRow{
		sqlbase.DatumToEncDatum(
			ScrubTypes[0],
			tree.NewDString(scrubErr.Code),
		),
		sqlbase.DatumToEncDatum(
			ScrubTypes[1],
			// FIXME(joey): create a reconstructed string of the primary key.
			tree.NewDString("TODO"),
		),
		sqlbase.DatumToEncDatum(
			ScrubTypes[2],
			tree.NewDString(detailsJSON.String()),
		),
	}, nil
}
