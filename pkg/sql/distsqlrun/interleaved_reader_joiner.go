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
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type postProcHelper struct {
	tableID sqlbase.ID
	indexID sqlbase.IndexID
	post    ProcOutputHelper
}

// interleavedReaderJoiner is at the start of a computation flow: it performs KV
// operations to retrieve rows for two tables (parent and child), internally
// filters the rows, performs a merge join with equality constraints.
// See docs/RFCS/20171025_interleaved_table_joins.md
type interleavedReaderJoiner struct {
	joinerBase

	flowCtx *FlowCtx

	// TableIDs for tracing.
	tableIDs []int
	// Post-processing helper for each table-index's rows.
	tablePosts []postProcHelper
	allSpans   roachpb.Spans
	limitHint  int64

	fetcher sqlbase.MultiRowFetcher
	alloc   sqlbase.DatumAlloc

	// TODO(richardwu): If we need to buffer more than 1 parent row for
	// prefix joins, subset joins, and/or outer joins, we need to buffer an
	// arbitrary number of parent and child rows.
	parentRow sqlbase.EncDatumRow
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

	// TODO(richardwu): Support all types of joins by re-using
	// streamMerger.
	// We give up streaming joined rows for full interleave prefix joins
	// unless we refactor streamMerger to stream rows if the first
	// (left/parent) batch only has one row.
	if spec.Type != JoinType_INNER {
		return nil, errors.Errorf("interleavedReaderJoiner only supports inner joins")
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

	// Table IDs are used for tracing.
	tableIDs := make([]int, len(spec.Tables))
	// Post-processing for each table's rows that comes out of
	// MultiRowFetcher.
	tablePosts := make([]postProcHelper, len(spec.Tables))
	// We need to take spans from all tables and merge them together
	// for MultiRowFetcher.
	allSpans := make(roachpb.Spans, 0, len(spec.Tables))
	for i, table := range spec.Tables {
		index, _, err := table.Desc.FindIndexByIndexIdx(int(table.IndexIdx))
		if err != nil {
			return nil, err
		}

		if err := tablePosts[i].post.Init(
			&table.Post, table.Desc.ColumnTypes(), &flowCtx.EvalCtx, nil, /*output*/
		); err != nil {
			return nil, errors.Wrapf(err, "failed to initialize post-processing helper")
		}

		tableIDs[i] = int(table.Desc.ID)
		tablePosts[i].tableID = table.Desc.ID
		tablePosts[i].indexID = index.ID
		for _, trSpan := range table.Spans {
			allSpans = append(allSpans, trSpan.Span)
		}
	}

	allSpans, _ = roachpb.MergeSpans(allSpans)

	irj := &interleavedReaderJoiner{
		flowCtx:    flowCtx,
		tableIDs:   tableIDs,
		tablePosts: tablePosts,
		allSpans:   allSpans,
	}

	if err := irj.initMultiRowFetcher(
		spec.Tables, spec.Reverse, &irj.alloc,
	); err != nil {
		return nil, err
	}

	irj.limitHint = limitHint(spec.LimitHint, post)

	// TODO(richardwu): Generalize this to 2+ tables.
	if err := irj.joinerBase.init(
		flowCtx,
		irj.tablePosts[0].post.outputTypes,
		irj.tablePosts[1].post.outputTypes,
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

func (irj *interleavedReaderJoiner) initMultiRowFetcher(
	tables []InterleavedReaderJoinerSpec_Table, reverseScan bool, alloc *sqlbase.DatumAlloc,
) error {
	args := make([]sqlbase.MultiRowFetcherTableArgs, len(tables))

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

	return irj.fetcher.Init(reverseScan, true /* returnRangeInfo */, alloc, args...)
}

// sendMisplannedRangesMetadata sends information about the non-local ranges
// that were read by this interleavedReaderJoiner.
// This should be called after the fetcher was used to read everything this
// interleavedReaderJoiner was supposed to read.
func (irj *interleavedReaderJoiner) sendMisplannedRangesMetadata(ctx context.Context) {
	misplannedRanges := misplannedRanges(ctx, irj.fetcher.GetRangeInfo(), irj.flowCtx.nodeID)

	if len(misplannedRanges) != 0 {
		irj.out.output.Push(nil /* row */, ProducerMetadata{Ranges: misplannedRanges})
	}
}

// Run is part of the processor interface.
func (irj *interleavedReaderJoiner) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "InterleaveReaderJoiner", irj.tableIDs)
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
		irj.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
		irj.out.Close()
		return
	}

	for {
		row, desc, index, err := irj.fetcher.NextRow(ctx)
		if err != nil || row == nil {
			if err != nil {
				irj.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
			}
			break
		}

		var helper postProcHelper
		helperIdx := -1
		for i, h := range irj.tablePosts {
			if desc.ID == h.tableID && index.ID == h.indexID {
				helper = h
				helperIdx = i
				break
			}
		}
		if helperIdx == -1 {
			panic("row fetched does not belong to any tables being scanned")
		}

		// We post-process the intermediate row from either table.
		tableRow, consumerStatus, err := helper.post.ProcessRow(ctx, row)
		if err != nil || consumerStatus != NeedMoreRows {
			if err != nil {
				irj.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
			}
			break
		}

		// Row was filtered out.
		if tableRow == nil {
			continue
		}

		// The first table is assumed to be the parent table.
		if helperIdx == 0 {
			// A new parent row is fetched. We re-assign our reference
			// to the most recent parent row.
			// This is safe because tableRow is a newly alloc'd
			// row.
			irj.parentRow = tableRow
			continue
		}

		// A child row (tableRow) is fetched.

		if irj.parentRow == nil {
			// A child row was fetched before any parent rows. This
			// is fine and shouldn't affect the desired outcome.
			continue
		}

		renderedRow, err := irj.render(irj.parentRow, tableRow)
		if err != nil {
			irj.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
			break
		}

		if renderedRow != nil {
			consumerStatus, err = irj.out.EmitRow(ctx, renderedRow)
			if err != nil || consumerStatus != NeedMoreRows {
				if err != nil {
					irj.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
				}
				break
			}
		}
	}
	irj.sendMisplannedRangesMetadata(ctx)
	sendTraceData(ctx, irj.out.output)
	irj.out.Close()
}
