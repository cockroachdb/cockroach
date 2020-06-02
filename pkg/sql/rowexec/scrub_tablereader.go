// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
var ScrubTypes = []*types.T{
	types.String,
	types.String,
	types.Jsonb,
}

type scrubTableReader struct {
	tableReader
	tableDesc sqlbase.TableDescriptor
	// fetcherResultToColIdx maps Fetcher results to the column index in
	// the TableDescriptor. This is only initialized and used during scrub
	// physical checks.
	fetcherResultToColIdx []int
	// indexIdx refers to the index being scanned. This is only used
	// during scrub physical checks.
	indexIdx int
}

var _ execinfra.Processor = &scrubTableReader{}
var _ execinfra.RowSource = &scrubTableReader{}

var scrubTableReaderProcName = "scrub"

// newScrubTableReader creates a scrubTableReader.
func newScrubTableReader(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*scrubTableReader, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); nodeID == 0 && ok {
		return nil, errors.Errorf("attempting to create a tableReader with uninitialized NodeID")
	}
	tr := &scrubTableReader{
		indexIdx: int(spec.IndexIdx),
	}

	tr.tableDesc = spec.Table
	tr.limitHint = execinfra.LimitHint(spec.LimitHint, post)

	if err := tr.Init(
		tr,
		post,
		ScrubTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			// We don't pass tr.input as an inputToDrain; tr.input is just an adapter
			// on top of a Fetcher; draining doesn't apply to it. Moreover, Andrei
			// doesn't trust that the adapter will do the right thing on a Next() call
			// after it had previously returned an error.
			InputsToDrain:        nil,
			TrailingMetaCallback: tr.generateTrailingMeta,
		},
	); err != nil {
		return nil, err
	}

	var neededColumns util.FastIntSet
	// If we are doing a scrub physical check, NeededColumns needs to be
	// changed to be all columns available in the index we are scanning.
	// This is because the emitted schema is ScrubTypes so NeededColumns
	// does not correctly represent the data being scanned.
	if spec.IndexIdx == 0 {
		neededColumns.AddRange(0, len(spec.Table.Columns)-1)
		for i := range spec.Table.Columns {
			tr.fetcherResultToColIdx = append(tr.fetcherResultToColIdx, i)
		}
	} else {
		colIdxMap := spec.Table.ColumnIdxMap()
		err := spec.Table.Indexes[spec.IndexIdx-1].RunOverAllColumns(func(id sqlbase.ColumnID) error {
			neededColumns.Add(colIdxMap[id])
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	var fetcher row.Fetcher
	if _, _, err := initRowFetcher(
		flowCtx, &fetcher, &tr.tableDesc, int(spec.IndexIdx), tr.tableDesc.ColumnIdxMap(),
		spec.Reverse, neededColumns, true /* isCheck */, &tr.alloc,
		execinfra.ScanVisibilityPublic, spec.LockingStrength,
	); err != nil {
		return nil, err
	}
	tr.fetcher = &fetcher

	tr.spans = make(roachpb.Spans, len(spec.Spans))
	for i, s := range spec.Spans {
		tr.spans[i] = s.Span
	}

	return tr, nil
}

// generateScrubErrorRow will create an EncDatumRow describing a
// physical check error encountered when scanning table data. The schema
// of the EncDatumRow is the ScrubTypes constant.
func (tr *scrubTableReader) generateScrubErrorRow(
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
		rowDetails[col.Name] = row[i].String(col.Type)
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

func (tr *scrubTableReader) prettyPrimaryKeyValues(
	row sqlbase.EncDatumRow, table *sqlbase.TableDescriptor,
) string {
	colIdxMap := make(map[sqlbase.ColumnID]int, len(table.Columns))
	for i := range table.Columns {
		id := table.Columns[i].ID
		colIdxMap[id] = i
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
			row[colIDToRowIdxMap[id]].String(table.Columns[colIdxMap[id]].Type))
	}
	primaryKeyValues.WriteByte(')')
	return primaryKeyValues.String()
}

// Start is part of the RowSource interface.
func (tr *scrubTableReader) Start(ctx context.Context) context.Context {
	if tr.FlowCtx.Txn == nil {
		tr.MoveToDraining(errors.Errorf("scrubTableReader outside of txn"))
	}

	ctx = tr.StartInternal(ctx, scrubTableReaderProcName)

	log.VEventf(ctx, 1, "starting")

	if err := tr.fetcher.StartScan(
		ctx, tr.FlowCtx.Txn, tr.spans,
		true /* limit batches */, tr.limitHint, tr.FlowCtx.TraceKV,
	); err != nil {
		tr.MoveToDraining(err)
	}

	return ctx
}

// Next is part of the RowSource interface.
func (tr *scrubTableReader) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for tr.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var err error
		// If we are running a scrub physical check, we use a specialized
		// procedure that runs additional checks while fetching the row
		// data.
		row, err = tr.fetcher.NextRowWithErrors(tr.Ctx)
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
		var v *scrub.Error
		if errors.As(err, &v) {
			row, err = tr.generateScrubErrorRow(row, v)
		} else if err == nil && row != nil {
			continue
		}
		if row == nil || err != nil {
			tr.MoveToDraining(scrub.UnwrapScrubError(err))
			break
		}

		if outRow := tr.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, tr.DrainHelper()
}
