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
	"bytes"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// ScrubTypes is the schema for TableReaders that are doing a SCRUB
// check. This schema is what TableReader output streams are overrided
// to for check. The column types correspond to:
// - Error type.
// - Primary key as a string, if it was obtainable.
// - JSON of all decoded column values.
//
// TODO(joey): If we want a way find the key for the error, we will need
// additional data such as the key bytes and the table descriptor ID.
// Repair won't be possible without this.
var ScrubTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_STRING},
	{SemanticType: sqlbase.ColumnType_STRING},
	{SemanticType: sqlbase.ColumnType_JSON},
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

	fetcher sqlbase.MultiRowFetcher
	alloc   sqlbase.DatumAlloc

	started bool
	isCheck bool
	// fetcherResultToColIdx maps RowFetcher results to the column index in
	// the TableDescriptor. This is only initialized and used during scrub
	// physical checks.
	fetcherResultToColIdx []int
	// indexIdx refers to the index being scanned. This is only used
	// during scrub physical checks.
	indexIdx int
}

var _ Processor = &tableReader{}
var _ RowSource = &tableReader{}

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

	tr.limitHint = limitHint(spec.LimitHint, post)

	types := make([]sqlbase.ColumnType, len(spec.Table.Columns))
	for i := range types {
		types[i] = spec.Table.Columns[i].Type
	}
	// IsCheck is only enabled while running a scrub physical check on an
	// index. When running the check, the output schema of the table
	// reader is instead ScrubTypes.
	if spec.IsCheck {
		types = ScrubTypes
	}
	if err := tr.init(post, types, flowCtx, output); err != nil {
		return nil, err
	}

	neededColumns := tr.out.neededColumns()

	// If we are doing a scrub physical check, neededColumns needs to be
	// changed to be all columns available in the index we are scanning.
	// This is because the emitted schema is ScrubTypes so neededColumns
	// does not correctly represent the data being scanned.
	if tr.isCheck {
		if spec.IndexIdx == 0 {
			neededColumns = util.FastIntSet{}
			neededColumns.AddRange(0, len(spec.Table.Columns)-1)
			for i := range spec.Table.Columns {
				tr.fetcherResultToColIdx = append(tr.fetcherResultToColIdx, i)
			}
		} else {
			colIDToIdx := make(map[sqlbase.ColumnID]int, len(spec.Table.Columns))
			for i := range spec.Table.Columns {
				colIDToIdx[spec.Table.Columns[i].ID] = i
			}
			neededColumns = util.FastIntSet{}
			for _, id := range spec.Table.Indexes[spec.IndexIdx-1].ColumnIDs {
				neededColumns.Add(colIDToIdx[id])
			}
			for _, id := range spec.Table.Indexes[spec.IndexIdx-1].ExtraColumnIDs {
				neededColumns.Add(colIDToIdx[id])
			}
			for _, id := range spec.Table.Indexes[spec.IndexIdx-1].StoreColumnIDs {
				neededColumns.Add(colIDToIdx[id])
			}
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
	fetcher *sqlbase.MultiRowFetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	alloc *sqlbase.DatumAlloc,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	index, isSecondaryIndex, err = desc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	colIdxMap := make(map[sqlbase.ColumnID]int, len(desc.Columns))
	for i, c := range desc.Columns {
		colIdxMap[c.ID] = i
	}

	tableArgs := sqlbase.MultiRowFetcherTableArgs{
		Desc:             desc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             desc.Columns,
		ValNeededForCol:  valNeededForCol,
	}
	if err := fetcher.Init(
		reverseScan, true /* returnRangeInfo */, isCheck, alloc, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}

// sendMisplannedRangesMetadata sends information about the non-local ranges
// that were read by this tableReader. This should be called after the fetcher
// was used to read everything this tableReader was supposed to read.
func (tr *tableReader) sendMisplannedRangesMetadata(ctx context.Context) {
	misplannedRanges := misplannedRanges(ctx, tr.fetcher.GetRangeInfo(), tr.flowCtx.nodeID)

	if len(misplannedRanges) != 0 {
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

	if tr.out.output == nil {
		panic("output RowReceiver not initialized for emitting rows")
	}

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	for {
		row, meta := tr.Next(ctx)
		// Emit the row; stop if no more rows are needed.
		if row != nil || meta.Err != nil {
			status := tr.out.output.Push(row, meta)
			if status != NeedMoreRows {
				break
			}
		}
		if row == nil || tr.out.rowIdx == tr.out.maxRowIdx {
			break
		}
	}

	tr.sendMisplannedRangesMetadata(ctx)
	sendTraceData(ctx, tr.out.output)
	tr.out.Close()
}

// generateScrubErrorRow will create an EncDatumRow describing a
// physical check error encountered when scanning table data. The schema
// of the EncDatumRow is the ScrubTypes constant.
func (tr *tableReader) generateScrubErrorRow(
	row sqlbase.EncDatumRow, scrubErr *scrub.Error,
) (sqlbase.EncDatumRow, error) {
	details := make(map[string]interface{})
	var index *sqlbase.IndexDescriptor
	if tr.indexIdx == 0 {
		index = &tr.tableDesc.PrimaryIndex
	} else {
		index = &tr.tableDesc.Indexes[tr.indexIdx-1]
	}
	// Collect all the row values into JSON
	rowDetails := make(map[string]interface{})
	for i, colIdx := range tr.fetcherResultToColIdx {
		col := tr.tableDesc.Columns[colIdx]
		// TODO(joey): We should maybe try to get the underlying type.
		rowDetails[col.Name] = row[i].String(&col.Type)
	}
	details["row_data"] = rowDetails
	details["index_name"] = index.Name
	details["error_message"] = scrub.UnwrapScrubError(error(scrubErr)).Error()

	detailsJSON, err := tree.MakeDJSON(details)
	if err != nil {
		return nil, err
	}

	primaryKeyValues := tr.prettyPrimaryKeyValues(row, &tr.tableDesc)
	return sqlbase.EncDatumRow{
		sqlbase.DatumToEncDatum(
			ScrubTypes[0],
			tree.NewDString(scrubErr.Code),
		),
		sqlbase.DatumToEncDatum(
			ScrubTypes[1],
			tree.NewDString(primaryKeyValues),
		),
		sqlbase.DatumToEncDatum(
			ScrubTypes[2],
			detailsJSON,
		),
	}, nil
}

func (tr *tableReader) prettyPrimaryKeyValues(
	row sqlbase.EncDatumRow, table *sqlbase.TableDescriptor,
) string {
	colIdxMap := make(map[sqlbase.ColumnID]int, len(table.Columns))
	for i, c := range table.Columns {
		colIdxMap[c.ID] = i
	}
	colIDToRowIdxMap := make(map[sqlbase.ColumnID]int, len(table.Columns))
	for rowIdx, colIdx := range tr.fetcherResultToColIdx {
		colIDToRowIdxMap[tr.tableDesc.Columns[colIdx].ID] = rowIdx
	}
	var primaryKeyValues bytes.Buffer
	primaryKeyValues.WriteByte('(')
	for i, id := range table.PrimaryIndex.ColumnIDs {
		if i > 0 {
			primaryKeyValues.WriteByte(',')
		}
		primaryKeyValues.WriteString(
			row[colIDToRowIdxMap[id]].String(&table.Columns[colIdxMap[id]].Type))
	}
	primaryKeyValues.WriteByte(')')
	return primaryKeyValues.String()
}

func (tr *tableReader) Types() []sqlbase.ColumnType {
	return tr.out.outputTypes
}

func (tr *tableReader) Next(ctx context.Context) (sqlbase.EncDatumRow, ProducerMetadata) {
	if !tr.started {
		tr.started = true

		if tr.flowCtx.txn == nil {
			log.Fatalf(ctx, "tableReader outside of txn")
		}

		// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
		if err := tr.fetcher.StartScan(
			ctx, tr.flowCtx.txn, tr.spans, true /* limit batches */, tr.limitHint, false, /* traceKV */
		); err != nil {
			log.Errorf(ctx, "scan error: %s", err)
			return nil, ProducerMetadata{Err: err}
		}
	}

	for {
		var row sqlbase.EncDatumRow
		var err error
		if !tr.isCheck {
			row, _, _, err = tr.fetcher.NextRow(ctx)
		} else {
			// If we are running a scrub physical check, we use a specialized
			// procedure that runs additional checks while fetching the row
			// data.
			row, err = tr.fetcher.NextRowWithErrors(ctx)
			// There are four cases that can happen after NextRowWithErrors:
			// 1) We encounter a ScrubError. We do not propagate the error up,
			//    but instead generate and emit a row for the final results.
			// 2) No errors were found. We simply continue scanning the data
			//    and discard the row values, as they are not needed for any
			//    results.
			// 3) A non-scrub error was encountered. This was not considered a
			//    physical data error, and so we propagate this to the user
			//    immediately.
			// 4) There was no error or row data. This signals that there is
			//    no more data to scan.
			//
			// NB: Cases 3 and 4 are handled further below, in the standard
			// table scanning code path.
			if v, ok := err.(*scrub.Error); ok {
				row, err = tr.generateScrubErrorRow(row, v)
			} else if err == nil && row != nil {
				continue
			}
		}
		if row == nil || err != nil {
			return nil, ProducerMetadata{Err: scrub.UnwrapScrubError(err)}
		}

		outRow, status, err := tr.out.ProcessRow(ctx, row)
		if outRow == nil && err == nil && status == NeedMoreRows {
			continue
		}
		return outRow, ProducerMetadata{Err: err}
	}
}

func (tr *tableReader) ConsumerDone() {
	// TODO(peter): what to do here?
}

func (tr *tableReader) ConsumerClosed() {
	// TODO(peter): what to do here?
}
