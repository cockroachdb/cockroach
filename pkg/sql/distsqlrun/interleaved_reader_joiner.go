// Copyright 2017 The Cockroach Authors.
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
	"context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// irjState represents the state of the processor.
type irjState int

const (
	irjStateUnknown irjState = iota
	// jrReadingInput means that a batch of rows is being read from the input.
	irjInitializing
	// jrPerformingLookup means we are performing an index lookup for the current
	// input row batch.
	irjReading
	// jrEmittingRows means we are emitting the results of the index lookup.
	irjUnmatchedChild
)

type tableInfo struct {
	tableID  sqlbase.ID
	indexID  sqlbase.IndexID
	post     ProcOutputHelper
	ordering sqlbase.ColumnOrdering
}

// interleavedReaderJoiner is at the start of a computation flow: it performs KV
// operations to retrieve rows for two tables (ancestor and child), internally
// filters the rows, performs a merge join with equality constraints.
// See docs/RFCS/20171025_interleaved_table_joins.md
type interleavedReaderJoiner struct {
	joinerBase

	// runningState represents the state of the processor. This is in addition to
	// processorBase.state - the runningState is only relevant when
	// processorBase.state == stateRunning.
	runningState irjState

	input RowSource

	// Each tableInfo contains the output helper (for intermediate
	// filtering) and ordering info for each table-index being joined.
	tables    []tableInfo
	allSpans  roachpb.Spans
	limitHint int64

	fetcher sqlbase.RowFetcher
	alloc   sqlbase.DatumAlloc

	// TODO(richardwu): If we need to buffer more than 1 ancestor row for
	// prefix joins, subset joins, and/or outer joins, we need to buffer an
	// arbitrary number of ancestor and child rows.
	// We can use streamMerger here for simplicity.
	ancestorRow sqlbase.EncDatumRow
	// These are required for OUTER joins where the ancestor need to be
	// emitted regardless.
	ancestorJoined     bool
	ancestorJoinSide   joinSide
	descendantJoinSide joinSide
	unmatchedChild     sqlbase.EncDatumRow
	// ancestorTablePos is the corresponding index of the ancestor table in
	// tables.
	ancestorTablePos int
}

func (irj *interleavedReaderJoiner) Start(ctx context.Context) context.Context {
	irj.runningState = irjInitializing
	return irj.startInternal(ctx, interleavedReaderJoinerProcName)
}

func (irj *interleavedReaderJoiner) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	// The lookup join is implemented as follows:
	// - Read the input rows in batches.
	// - For each batch, map the the rows onto index keys and perform an index
	//   lookup for those keys. Note that multiple rows may map to the same key.
	// - Retrieve the index lookup results in batches, since the index scan may
	//   return more rows than the input batch size.
	// - If the index is a secondary index which does not contain all the needed
	//   output columns, perform a second lookup on the primary index.
	// - Join the index rows with the corresponding input rows and buffer the
	//   results in jr.toEmit.
	for irj.state == stateRunning {
		var row sqlbase.EncDatumRow
		var meta *ProducerMetadata
		switch irj.runningState {
		case irjInitializing:
			// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
			if err := irj.fetcher.StartScan(
				irj.ctx, irj.flowCtx.txn, irj.allSpans, true /* limitBatches */, irj.limitHint, false, /* traceKV */
			); err != nil {
				irj.moveToDraining(err)
				return nil, irj.drainHelper()
			}
			irj.runningState = irjReading
		case irjReading:
			irj.runningState, row, meta = irj.nextRow()
		case irjUnmatchedChild:
			irj.runningState, row, meta = irjReading, irj.unmatchedChild, nil
		default:
			log.Fatalf(irj.ctx, "unsupported state: %d", irj.runningState)
		}
		if row != nil || meta != nil {
			return row, meta
		}
	}
	return nil, irj.drainHelper()
}

func (irj *interleavedReaderJoiner) nextRow() (irjState, sqlbase.EncDatumRow, *ProducerMetadata) {
	row, desc, index, err := irj.fetcher.NextRow(irj.ctx)
	if row == nil {
		// All done - just finish maybe emitting our last ancestor.
		irj.moveToDraining(nil)
		return irjReading, irj.maybeUnmatchedAncestor(), nil
	}

	// Lookup the helper that belongs to this row.
	var tInfo *tableInfo
	isAncestorRow := false
	for i := range irj.tables {
		tInfo = &irj.tables[i]
		if desc.ID == tInfo.tableID && index.ID == tInfo.indexID {
			if i == irj.ancestorTablePos {
				isAncestorRow = true
			}
			break
		}
	}

	// We post-process the intermediate row from either table.
	tableRow, ok, err := tInfo.post.ProcessRow(irj.ctx, row)
	if err != nil {
		irj.moveToDraining(scrub.UnwrapScrubError(err))
		return irjStateUnknown, nil, irj.drainHelper()
	}
	if !ok {
		irj.moveToDraining(nil)
	}

	// Row was filtered out.
	if tableRow == nil {
		return irjReading, nil, nil
	}

	if isAncestorRow {
		maybeAncestor := irj.maybeUnmatchedAncestor()

		irj.ancestorJoined = false
		irj.ancestorRow = tInfo.post.rowAlloc.CopyRow(tableRow)

		// If maybeAncestor is nil, we'll loop back around and read the next row
		// without returning a row to the caller.
		return irjReading, maybeAncestor, nil
	}

	// A child row (tableRow) is fetched.

	// TODO(richardwu): Generalize this to 2+ tables and sibling
	// tables.
	var lrow, rrow sqlbase.EncDatumRow
	if irj.ancestorTablePos == 0 {
		lrow, rrow = irj.ancestorRow, tableRow
	} else {
		lrow, rrow = tableRow, irj.ancestorRow
	}

	// TODO(richardwu): this is a very expensive comparison
	// in the hot path. We can avoid this if there is a foreign
	// key constraint between the merge columns.
	// That is: any child rows can be joined with the most
	// recent parent row without this comparison.
	cmp, err := CompareEncDatumRowForMerge(
		irj.tables[0].post.outputTypes,
		lrow,
		rrow,
		irj.tables[0].ordering,
		irj.tables[1].ordering,
		false, /* nullEquality */
		&irj.alloc,
		&irj.flowCtx.EvalCtx,
	)
	if err != nil {
		irj.moveToDraining(err)
		return irjStateUnknown, nil, irj.drainHelper()
	}

	// The child row match the most recent ancestorRow on the
	// equality columns.
	// Try to join/render and emit.
	if cmp == 0 {
		renderedRow, err := irj.render(lrow, rrow)
		if err != nil {
			irj.moveToDraining(err)
			return irjStateUnknown, nil, irj.drainHelper()
		}
		if renderedRow != nil {
			irj.ancestorJoined = true
		}
		return irjReading, irj.processRowHelper(renderedRow), nil
	}

	// Child does not match previous ancestorRow.
	// Try to emit the ancestor row.
	unmatchedAncestor := irj.maybeUnmatchedAncestor()

	// Reset the ancestorRow (we know there are no more
	// corresponding children rows).
	irj.ancestorRow = nil

	newState := irjReading
	// Set the unmatched child if necessary (we'll pick it up again after we emit
	// the ancestor).
	if shouldEmitUnmatchedRow(irj.descendantJoinSide, irj.joinType) {
		rendered := irj.renderUnmatchedRow(row, irj.descendantJoinSide)
		if row := irj.processRowHelper(rendered); row != nil {
			irj.unmatchedChild = row
			newState = irjUnmatchedChild
		}
	}

	return newState, unmatchedAncestor, nil
}

func (irj *interleavedReaderJoiner) ConsumerDone() {
	irj.moveToDraining(nil /* err */)
}

func (irj *interleavedReaderJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	irj.internalClose()
}

var _ Processor = &interleavedReaderJoiner{}

// newInterleavedReaderJoiner creates a interleavedReaderJoiner.
func newInterleavedReaderJoiner(
	flowCtx *FlowCtx,
	processorID int32,
	spec *InterleavedReaderJoinerSpec,
	post *PostProcessSpec,
	output RowReceiver,
) (*interleavedReaderJoiner, error) {
	if flowCtx.nodeID == 0 {
		return nil, errors.Errorf("attempting to create an interleavedReaderJoiner with uninitialized NodeID")
	}

	// TODO(richardwu): We can relax this to < 2 (i.e. permit 2+ tables).
	// This will require modifying joinerBase init logic.
	if len(spec.Tables) != 2 {
		return nil, errors.Errorf("interleavedReaderJoiner only reads from two tables in an interleaved hierarchy")
	}

	// Ensure the column orderings of all tables being merged are in the
	// same direction.
	for i, c := range spec.Tables[0].Ordering.Columns {
		for _, table := range spec.Tables[1:] {
			if table.Ordering.Columns[i].Direction != c.Direction {
				return nil, errors.Errorf("unmatched column orderings")
			}
		}
	}

	tables := make([]tableInfo, len(spec.Tables))
	// We need to take spans from all tables and merge them together
	// for RowFetcher.
	allSpans := make(roachpb.Spans, 0, len(spec.Tables))

	// We need to figure out which table is the ancestor.
	var ancestorTablePos int
	var numAncestorPKCols int
	minAncestors := -1
	for i, table := range spec.Tables {
		index, _, err := table.Desc.FindIndexByIndexIdx(int(table.IndexIdx))
		if err != nil {
			return nil, err
		}

		// The simplest way is to find the table with the fewest
		// interleave ancestors.
		// TODO(richardwu): Adapt this for sibling joins and multi-table joins.
		if minAncestors == -1 || len(index.Interleave.Ancestors) < minAncestors {
			minAncestors = len(index.Interleave.Ancestors)
			ancestorTablePos = i
			numAncestorPKCols = len(index.ColumnIDs)
		}

		if err := tables[i].post.Init(
			&table.Post, table.Desc.ColumnTypes(), &flowCtx.EvalCtx, nil, /*output*/
		); err != nil {
			return nil, errors.Wrapf(err, "failed to initialize post-processing helper")
		}

		tables[i].tableID = table.Desc.ID
		tables[i].indexID = index.ID
		tables[i].ordering = convertToColumnOrdering(table.Ordering)
		for _, trSpan := range table.Spans {
			allSpans = append(allSpans, trSpan.Span)
		}
	}

	if len(spec.Tables[0].Ordering.Columns) != numAncestorPKCols {
		return nil, errors.Errorf("interleavedReaderJoiner only supports joins on the entire interleaved prefix")
	}

	allSpans, _ = roachpb.MergeSpans(allSpans)

	ancestorJoinSide := leftSide
	descendantJoinSide := rightSide
	if ancestorTablePos == 1 {
		ancestorJoinSide = rightSide
		descendantJoinSide = leftSide
	}

	irj := &interleavedReaderJoiner{
		tables:             tables,
		allSpans:           allSpans,
		ancestorTablePos:   ancestorTablePos,
		ancestorJoinSide:   ancestorJoinSide,
		descendantJoinSide: descendantJoinSide,
	}

	if err := irj.initRowFetcher(
		spec.Tables, spec.Reverse, &irj.alloc,
	); err != nil {
		return nil, err
	}

	irj.limitHint = limitHint(spec.LimitHint, post)

	// TODO(richardwu): Generalize this to 2+ tables.
	if err := irj.joinerBase.init(
		irj,
		flowCtx,
		processorID,
		irj.tables[0].post.outputTypes,
		irj.tables[1].post.outputTypes,
		spec.Type,
		spec.OnExpr,
		nil, /*leftEqColumns*/
		nil, /*rightEqColumns*/
		0,   /*numMergedColumns*/
		post,
		output,
		procStateOpts{
			inputsToDrain: []RowSource{},
			trailingMetaCallback: func() []ProducerMetadata {
				irj.internalClose()
				if meta := getTxnCoordMeta(irj.flowCtx.txn); meta != nil {
					return []ProducerMetadata{{TxnCoordMeta: meta}}
				}
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	return irj, nil
}

func (irj *interleavedReaderJoiner) initRowFetcher(
	tables []InterleavedReaderJoinerSpec_Table, reverseScan bool, alloc *sqlbase.DatumAlloc,
) error {
	args := make([]sqlbase.RowFetcherTableArgs, len(tables))

	for i, table := range tables {
		desc := table.Desc
		var err error
		args[i].Index, args[i].IsSecondaryIndex, err = desc.FindIndexByIndexIdx(int(table.IndexIdx))
		if err != nil {
			return err
		}

		// We require all values from the tables being read
		// since we do not expect any projections or rendering
		// on a scan before a join.
		args[i].ValNeededForCol.AddRange(0, len(desc.Columns)-1)
		args[i].ColIdxMap = desc.ColumnIdxMap()
		args[i].Desc = &desc
		args[i].Cols = desc.Columns
		args[i].Spans = make(roachpb.Spans, len(table.Spans))
		for j, trSpan := range table.Spans {
			args[i].Spans[j] = trSpan.Span
		}
	}

	return irj.fetcher.Init(reverseScan, true /* returnRangeInfo */, true /* isCheck */, alloc,
		args...)
}

// sendMisplannedRangesMetadata sends information about the non-local ranges
// that were read by this interleavedReaderJoiner.
// This should be called after the fetcher was used to read everything this
// interleavedReaderJoiner was supposed to read.
func (irj *interleavedReaderJoiner) sendMisplannedRangesMetadata(ctx context.Context) {
	misplannedRanges := misplannedRanges(ctx, irj.fetcher.GetRangeInfo(), irj.flowCtx.nodeID)

	if len(misplannedRanges) != 0 {
		irj.out.output.Push(nil /* row */, &ProducerMetadata{Ranges: misplannedRanges})
	}
}

const interleavedReaderJoinerProcName = "interleaved reader joiner"

func (irj *interleavedReaderJoiner) maybeUnmatchedAncestor() sqlbase.EncDatumRow {
	// We first try to emit the previous ancestor row if it
	// was never joined with a child row.
	if irj.ancestorRow != nil && !irj.ancestorJoined {
		if !shouldEmitUnmatchedRow(irj.ancestorJoinSide, irj.joinType) {
			return nil
		}

		rendered := irj.renderUnmatchedRow(irj.ancestorRow, irj.ancestorJoinSide)
		return irj.processRowHelper(rendered)
	}
	return nil
}
