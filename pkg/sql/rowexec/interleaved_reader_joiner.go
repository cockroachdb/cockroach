// Copyright 2017 The Cockroach Authors.
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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// irjState represents the state of the processor.
type irjState int

const (
	irjStateUnknown irjState = iota
	// irjReading causes the state machine to read the next row from the kvFetcher
	// and potentially output a merged row.
	irjReading
	// irjUnmatchedChild indicates that the state machine should output the
	// unmatched child row stored in the unmatchedChild field.
	irjUnmatchedChild
)

type tableInfo struct {
	tableID  sqlbase.ID
	indexID  sqlbase.IndexID
	post     execinfra.ProcOutputHelper
	ordering sqlbase.ColumnOrdering
}

// interleavedReaderJoiner is at the start of a computation flow: it performs KV
// operations to retrieve rows for two tables (ancestor and child), internally
// filters the rows, performs a merge join with equality constraints.
// See docs/RFCS/20171025_interleaved_table_joins.md
type interleavedReaderJoiner struct {
	joinerBase

	// runningState represents the state of the processor. This is in addition to
	// ProcessorBase.State - the runningState is only relevant when
	// ProcessorBase.State == StateRunning.
	runningState irjState

	// Each tableInfo contains the output helper (for intermediate
	// filtering) and ordering info for each table-index being joined.
	tables    []tableInfo
	allSpans  roachpb.Spans
	limitHint int64

	fetcher row.Fetcher
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
	irj.runningState = irjReading
	ctx = irj.StartInternal(ctx, interleavedReaderJoinerProcName)
	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	if err := irj.fetcher.StartScan(
		irj.Ctx, irj.FlowCtx.Txn, irj.allSpans, true /* limitBatches */, irj.limitHint, false, /* traceKV */
	); err != nil {
		irj.MoveToDraining(err)
	}
	return ctx
}

func (irj *interleavedReaderJoiner) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// Next is implemented as a state machine. The states are represented by the
	// irjState enum at the top of this file.
	// Roughly, the state machine is either in an initialization phase, a steady
	// state phase that outputs either 1 or 0 rows on every call, or a special
	// unmatched child phase that outputs a child row that doesn't match the last
	// seen ancestor if the join type calls for it.
	for irj.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch irj.runningState {
		case irjReading:
			irj.runningState, row, meta = irj.nextRow()
		case irjUnmatchedChild:
			rendered := irj.renderUnmatchedRow(irj.unmatchedChild, irj.descendantJoinSide)
			row = irj.ProcessRowHelper(rendered)
			irj.unmatchedChild = nil
			irj.runningState = irjReading
		default:
			log.Fatalf(irj.Ctx, "unsupported state: %d", irj.runningState)
		}
		if row != nil || meta != nil {
			return row, meta
		}
	}
	return nil, irj.DrainHelper()
}

// findTable returns the tableInfo for the given table and index descriptor,
// along with a boolean that is true if the found tableInfo represents the
// ancestor table in this join. err is non-nil if the table was missing from the
// list.
func (irj *interleavedReaderJoiner) findTable(
	table *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) (tInfo *tableInfo, isAncestorRow bool, err error) {
	for i := range irj.tables {
		tInfo = &irj.tables[i]
		if table.ID == tInfo.tableID && index.ID == tInfo.indexID {
			if i == irj.ancestorTablePos {
				isAncestorRow = true
			}
			return tInfo, isAncestorRow, nil
		}
	}
	return nil,
		false,
		errors.Errorf("index %q.%q missing from interleaved join",
			table.Name, index.Name)
}

// nextRow implements the steady state of the interleavedReaderJoiner. It
// requests the next row from its backing kv fetcher, determines whether its an
// ancestor or child row, and conditionally merges and outputs a result.
func (irj *interleavedReaderJoiner) nextRow() (
	irjState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	row, desc, index, err := irj.fetcher.NextRow(irj.Ctx)
	if err != nil {
		irj.MoveToDraining(scrub.UnwrapScrubError(err))
		return irjStateUnknown, nil, irj.DrainHelper()
	}
	if row == nil {
		// All done - just finish maybe emitting our last ancestor.
		lastAncestor := irj.maybeUnmatchedAncestor()
		irj.MoveToDraining(nil)
		return irjReading, lastAncestor, nil
	}

	// Lookup the helper that belongs to this row.
	tInfo, isAncestorRow, err := irj.findTable(desc, index)
	if err != nil {
		irj.MoveToDraining(err)
		return irjStateUnknown, nil, irj.DrainHelper()
	}

	// We post-process the intermediate row from either table.
	tableRow, ok, err := tInfo.post.ProcessRow(irj.Ctx, row)
	if err != nil {
		irj.MoveToDraining(err)
		return irjStateUnknown, nil, irj.DrainHelper()
	}
	if !ok {
		irj.MoveToDraining(nil)
	}

	// Row was filtered out.
	if tableRow == nil {
		return irjReading, nil, nil
	}

	if isAncestorRow {
		maybeAncestor := irj.maybeUnmatchedAncestor()

		irj.ancestorJoined = false
		irj.ancestorRow = tInfo.post.RowAlloc.CopyRow(tableRow)

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
		irj.tables[0].post.OutputTypes,
		lrow,
		rrow,
		irj.tables[0].ordering,
		irj.tables[1].ordering,
		false, /* nullEquality */
		&irj.alloc,
		irj.FlowCtx.EvalCtx,
	)
	if err != nil {
		irj.MoveToDraining(err)
		return irjStateUnknown, nil, irj.DrainHelper()
	}

	// The child row match the most recent ancestorRow on the
	// equality columns.
	// Try to join/render and emit.
	if cmp == 0 {
		renderedRow, err := irj.render(lrow, rrow)
		if err != nil {
			irj.MoveToDraining(err)
			return irjStateUnknown, nil, irj.DrainHelper()
		}
		if renderedRow != nil {
			irj.ancestorJoined = true
		}
		return irjReading, irj.ProcessRowHelper(renderedRow), nil
	}

	// Child does not match previous ancestorRow.
	// Try to emit the ancestor row.
	unmatchedAncestor := irj.maybeUnmatchedAncestor()

	// Reset the ancestorRow (we know there are no more
	// corresponding children rows).
	irj.ancestorRow = nil
	irj.ancestorJoined = false

	newState := irjReading
	// Set the unmatched child if necessary (we'll pick it up again after we emit
	// the ancestor).
	if shouldEmitUnmatchedRow(irj.descendantJoinSide, irj.joinType) {
		irj.unmatchedChild = row
		newState = irjUnmatchedChild
	}

	return newState, unmatchedAncestor, nil
}

func (irj *interleavedReaderJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	irj.InternalClose()
}

var _ execinfra.Processor = &interleavedReaderJoiner{}
var _ execinfra.RowSource = &interleavedReaderJoiner{}
var _ execinfrapb.MetadataSource = &interleavedReaderJoiner{}
var _ execinfra.OpNode = &interleavedReaderJoiner{}

// newInterleavedReaderJoiner creates a interleavedReaderJoiner.
func newInterleavedReaderJoiner(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.InterleavedReaderJoinerSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*interleavedReaderJoiner, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); nodeID == 0 && ok {
		return nil, errors.AssertionFailedf("attempting to create an interleavedReaderJoiner with uninitialized NodeID")
	}

	// Increment some telemetry counters about use of the interleaved table join feature.
	telemetry.Inc(sqltelemetry.InterleavedTableJoinCounter)

	// TODO(richardwu): We can relax this to < 2 (i.e. permit 2+ tables).
	// This will require modifying joinerBase init logic.
	if len(spec.Tables) != 2 {
		return nil, errors.AssertionFailedf("interleavedReaderJoiner only reads from two tables in an interleaved hierarchy")
	}

	// Ensure the column orderings of all tables being merged are in the
	// same direction.
	for i, c := range spec.Tables[0].Ordering.Columns {
		for _, table := range spec.Tables[1:] {
			if table.Ordering.Columns[i].Direction != c.Direction {
				return nil, errors.AssertionFailedf("unmatched column orderings")
			}
		}
	}

	tables := make([]tableInfo, len(spec.Tables))
	// We need to take spans from all tables and merge them together
	// for Fetcher.
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
			&table.Post, table.Desc.ColumnTypes(), flowCtx.NewEvalCtx(), nil, /*output*/
		); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to initialize post-processing helper")
		}

		tables[i].tableID = table.Desc.ID
		tables[i].indexID = index.ID
		tables[i].ordering = execinfrapb.ConvertToColumnOrdering(table.Ordering)
		for _, trSpan := range table.Spans {
			allSpans = append(allSpans, trSpan.Span)
		}
	}

	if len(spec.Tables[0].Ordering.Columns) != numAncestorPKCols {
		return nil, errors.AssertionFailedf(
			"interleavedReaderJoiner only supports joins on the entire interleaved prefix")
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
		flowCtx, spec.Tables, tables, spec.Reverse, spec.LockingStrength, &irj.alloc,
	); err != nil {
		return nil, err
	}

	irj.limitHint = execinfra.LimitHint(spec.LimitHint, post)

	// TODO(richardwu): Generalize this to 2+ tables.
	if err := irj.joinerBase.init(
		irj,
		flowCtx,
		processorID,
		irj.tables[0].post.OutputTypes,
		irj.tables[1].post.OutputTypes,
		spec.Type,
		spec.OnExpr,
		nil, /*leftEqColumns*/
		nil, /*rightEqColumns*/
		0,   /*numMergedColumns*/
		post,
		output,
		execinfra.ProcStateOpts{
			InputsToDrain:        []execinfra.RowSource{},
			TrailingMetaCallback: irj.generateTrailingMeta,
		},
	); err != nil {
		return nil, err
	}

	return irj, nil
}

func (irj *interleavedReaderJoiner) initRowFetcher(
	flowCtx *execinfra.FlowCtx,
	tables []execinfrapb.InterleavedReaderJoinerSpec_Table,
	tableInfos []tableInfo,
	reverseScan bool,
	lockStr sqlbase.ScanLockingStrength,
	alloc *sqlbase.DatumAlloc,
) error {
	args := make([]row.FetcherTableArgs, len(tables))

	for i, table := range tables {
		desc := sqlbase.NewImmutableTableDescriptor(table.Desc)
		var err error
		args[i].Index, args[i].IsSecondaryIndex, err = desc.FindIndexByIndexIdx(int(table.IndexIdx))
		if err != nil {
			return err
		}

		args[i].ValNeededForCol = tableInfos[i].post.NeededColumns()
		args[i].ColIdxMap = desc.ColumnIdxMap()
		args[i].Desc = desc
		args[i].Cols = desc.Columns
		args[i].Spans = make(roachpb.Spans, len(table.Spans))
		for j, trSpan := range table.Spans {
			args[i].Spans[j] = trSpan.Span
		}
	}

	return irj.fetcher.Init(
		flowCtx.Codec(),
		reverseScan,
		lockStr,
		true, /* returnRangeInfo */
		true, /* isCheck */
		alloc,
		args...,
	)
}

func (irj *interleavedReaderJoiner) generateTrailingMeta(
	ctx context.Context,
) []execinfrapb.ProducerMetadata {
	trailingMeta := irj.generateMeta(ctx)
	irj.InternalClose()
	return trailingMeta
}

func (irj *interleavedReaderJoiner) generateMeta(
	ctx context.Context,
) []execinfrapb.ProducerMetadata {
	var trailingMeta []execinfrapb.ProducerMetadata
	nodeID, ok := irj.FlowCtx.NodeID.OptionalNodeID()
	if ok {
		ranges := execinfra.MisplannedRanges(ctx, irj.fetcher.GetRangesInfo(), nodeID)
		if ranges != nil {
			trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
		}
	}
	if tfs := execinfra.GetLeafTxnFinalState(ctx, irj.FlowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	return trailingMeta
}

// DrainMeta is part of the MetadataSource interface.
func (irj *interleavedReaderJoiner) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	return irj.generateMeta(ctx)
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
		return irj.ProcessRowHelper(rendered)
	}
	return nil
}

// ChildCount is part of the execinfra.OpNode interface.
func (irj *interleavedReaderJoiner) ChildCount(verbose bool) int {
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (irj *interleavedReaderJoiner) Child(nth int, verbose bool) execinfra.OpNode {
	panic(fmt.Sprintf("invalid index %d", nth))
}
