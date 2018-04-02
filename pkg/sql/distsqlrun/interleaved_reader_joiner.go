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
	"sync"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	// ancestorTablePos is the corresponding index of the ancestor table in
	// tables.
	ancestorTablePos int
}

var _ Processor = &interleavedReaderJoiner{}

// newInterleavedReaderJoiner creates a interleavedReaderJoiner.
func newInterleavedReaderJoiner(
	flowCtx *FlowCtx, spec *InterleavedReaderJoinerSpec, post *PostProcessSpec, output RowReceiver,
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
		flowCtx,
		irj.tables[0].post.outputTypes,
		irj.tables[1].post.outputTypes,
		spec.Type,
		spec.OnExpr,
		nil, /*leftEqColumns*/
		nil, /*rightEqColumns*/
		0,   /*numMergedColumns*/
		post,
		output,
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
		args[i].ColIdxMap = make(map[sqlbase.ColumnID]int, len(desc.Columns))
		for j, c := range desc.Columns {
			args[i].ColIdxMap[c.ID] = j
		}
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

// Run is part of the processor interface.
func (irj *interleavedReaderJoiner) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	tableIDs := make([]sqlbase.ID, len(irj.tables))
	for i := range tableIDs {
		tableIDs[i] = irj.tables[i].tableID
	}
	ctx := log.WithLogTag(irj.flowCtx.Ctx, "InterleaveReaderJoiner", tableIDs)
	ctx, span := processorSpan(ctx, "interleaved reader joiner")
	defer tracing.FinishSpan(span)

	txn := irj.flowCtx.txn
	if txn == nil {
		log.Fatalf(ctx, "interleavedReaderJoiner outside of txn")
	}

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	if err := irj.fetcher.StartScan(
		ctx, txn, irj.allSpans, true /* limit batches */, irj.limitHint, false, /* traceKV */
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		irj.out.output.Push(nil /* row */, &ProducerMetadata{Err: err})
		irj.out.Close()
		return
	}

	for {
		row, desc, index, err := irj.fetcher.NextRow(ctx)
		if err != nil || row == nil {
			if err != nil {
				err = scrub.UnwrapScrubError(err)
				irj.out.output.Push(nil /* row */, &ProducerMetadata{Err: err})
			} else if irj.ancestorRow != nil && !irj.ancestorJoined {
				// If the last row read is a parent, we may need to
				// emit it unmatched for OUTER joins.
				_, err := irj.maybeEmitUnmatchedRow(ctx, irj.ancestorRow, irj.ancestorJoinSide)
				if err != nil {
					irj.out.output.Push(nil /* row */, &ProducerMetadata{Err: err})
				}
			}
			break
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
		tableRow, consumerStatus, err := tInfo.post.ProcessRow(ctx, row)
		if err != nil || consumerStatus != NeedMoreRows {
			if err != nil {
				irj.out.output.Push(nil /* row */, &ProducerMetadata{Err: err})
			}
			break
		}

		// Row was filtered out.
		if tableRow == nil {
			continue
		}

		if isAncestorRow {
			needMoreRows := irj.maybeEmitUnmatchedAncestor(ctx)
			if !needMoreRows {
				break
			}

			// A new ancestor row is fetched. We re-assign our reference
			// to the most recent ancestor row.
			// This is safe because tableRow is a newly alloc'd
			// row.
			irj.ancestorRow = tableRow
			irj.ancestorJoined = false
			continue
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
			irj.out.output.Push(nil /* row */, &ProducerMetadata{Err: err})
			break
		}

		// The child row match the most recent ancestorRow on the
		// equality columns.
		// Try to join/render and emit.
		if cmp == 0 {
			renderedRow, err := irj.render(lrow, rrow)
			if err != nil {
				irj.out.output.Push(nil /* row */, &ProducerMetadata{Err: err})
				break
			}
			if renderedRow != nil {
				consumerStatus, err = irj.out.EmitRow(ctx, renderedRow)
				if err != nil || consumerStatus != NeedMoreRows {
					if err != nil {
						irj.out.output.Push(nil /* row */, &ProducerMetadata{Err: err})
					}
					break
				}

				irj.ancestorJoined = true

				continue
			}
		} else {
			// Child does not match previous ancestorRow.
			// Try to emit the ancestor row.
			needMoreRows := irj.maybeEmitUnmatchedAncestor(ctx)
			if !needMoreRows {
				break
			}

			// Reset the ancestorRow (we know there are no more
			// corresponding children rows).
			irj.ancestorRow = nil
		}

		// Either a child row is read before an ancestor row is read
		// (which is possible at the beginning of a span partition)
		// or the child row cannot be joined with the ancestor row.
		// We will need to try to emit the unmatched row if we have an
		// OUTER join.
		needMoreRows, err := irj.maybeEmitUnmatchedRow(ctx, tableRow, irj.descendantJoinSide)
		if !needMoreRows || err != nil {
			if err != nil {
				irj.out.output.Push(nil /* row */, &ProducerMetadata{Err: err})
			}
			break
		}
	}

	irj.sendMisplannedRangesMetadata(ctx)
	sendTraceData(ctx, irj.out.output)
	sendTxnCoordMetaMaybe(irj.flowCtx.txn, irj.out.output)
	irj.out.Close()
}

func (irj *interleavedReaderJoiner) maybeEmitUnmatchedAncestor(
	ctx context.Context,
) (needMoreRows bool) {
	// We first try to emit the previous ancestor row if it
	// was never joined with a child row.
	if irj.ancestorRow != nil && !irj.ancestorJoined {
		needMoreRows, err := irj.maybeEmitUnmatchedRow(ctx, irj.ancestorRow, irj.ancestorJoinSide)
		if !needMoreRows || err != nil {
			if err != nil {
				irj.out.output.Push(nil /* row */, &ProducerMetadata{Err: err})
			}
			return false
		}
	}

	return true
}
