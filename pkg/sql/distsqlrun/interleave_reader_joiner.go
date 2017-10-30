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
	"fmt"
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

// interleaveReaderJoiner is at the start of a computation flow: it performs KV
// operations to retrieve rows for two tables (parent and child), internally
// filters the rows, performs a merge join with equality constraints.
// See docs/RFCS/20171025_interleaved_table_joins.md
type interleaveReaderJoiner struct {
	joinerBase

	flowCtx *FlowCtx

	// TableIDs for tracing.
	tableIDs []int
	// Post-processing helper for each table-index's rows.
	tablePosts []postProcHelper
	spans      roachpb.Spans
	limitHint  int64

	fetcher sqlbase.MultiRowFetcher
	alloc   sqlbase.DatumAlloc

	// TODO(richardwu): If we need to buffer more than 1 parent row for
	// prefix joins and/or subset joins, we need to buffer an arbitrary
	// number of parent and child rows.
	parentRow sqlbase.EncDatumRow
}

var _ Processor = &interleaveReaderJoiner{}

// newInterleaveReaderJoiner creates a interleaveReaderJoiner.
func newInterleaveReaderJoiner(
	flowCtx *FlowCtx, spec *InterleaveReaderJoinerSpec, post *PostProcessSpec, output RowReceiver,
) (*interleaveReaderJoiner, error) {
	if flowCtx.nodeID == 0 {
		return nil, errors.Errorf("attempting to create an interleaveReaderJoiner with uninitialized NodeID")
	}

	// TODO(richardwu): We can relax this to < 2 (i.e. permit 2+ tables).
	// This will require modifying joinerBase init logic.
	if len(spec.Tables) != 2 {
		return nil, errors.Errorf("interleaveReaderJoiner only reads from two tables in an interleaved hierarchy")
	}

	// TODO(richardwu): Support all types of joins by re-using
	// streamMerger.
	// We give up streaming joined rows for full interleave prefix joins
	// unless we refactor streamMerger to stream rows if the first
	// (left/parent) batch only has one row.
	if spec.Type != JoinType_INNER {
		return nil, errors.Errorf("interleaveReaderJoiner only supports inner joins")
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
	for i, table := range spec.Tables {
		tableIDs[i] = int(table.Desc.ID)
		index, _, err := table.Desc.FindIndexByIndexIdx(table.IndexIdx)
		if err != nil {
			return nil, err
		}

		tablePosts[i].tableID = table.Desc.ID
		tablePosts[i].indexID = index.ID
		if err := tablePosts[i].post.Init(
			&table.Post, table.Desc.ColumnTypes(), &flowCtx.EvalCtx, nil, /*output*/
		); err != nil {
			return nil, errors.Wrapf(err, "failed to initialize post-processing helper")
		}
	}

	irj := &interleaveReaderJoiner{
		flowCtx:    flowCtx,
		tableIDs:   tableIDs,
		tablePosts: tablePosts,
	}

	if err := irj.initMultiRowFetcher(
		spec.Tables, spec.Reverse, &irj.alloc,
	); err != nil {
		return nil, err
	}

	irj.spans = make(roachpb.Spans, len(spec.Spans))
	for i, s := range spec.Spans {
		irj.spans[i] = s.Span
	}

	// Note: spec.LimitHint logic that combines the tables' limit hints
	// is in the planner.
	if post.Limit != 0 && post.Limit <= readerOverflowProtection {
		// In this case the ProcOutputHelper will tell us to stop once
		// we emit enough rows.
		irj.limitHint = int64(post.Limit)
	} else if spec.LimitHint != 0 && spec.LimitHint <= readerOverflowProtection {
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
		irj.limitHint = spec.LimitHint + rowChannelBufSize + 1
	}

	if post.Filter.Expr != "" {
		// We have a filter so we will likely need to read more rows.
		irj.limitHint *= 2
	}

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

func (irj *interleaveReaderJoiner) initMultiRowFetcher(
	tables []InterleaveReaderJoinerTable, reverseScan bool, alloc *sqlbase.DatumAlloc,
) error {
	args := make([]sqlbase.MultiRowFetcherTableArgs, len(tables))

	for i, table := range tables {
		desc := table.Desc
		var err error
		args[i].Index, args[i].IsSecondaryIndex, err = desc.FindIndexByIndexIdx(table.IndexIdx)
		if err != nil {
			return err
		}

		colIdxMap := make(map[sqlbase.ColumnID]int, len(desc.Columns))
		args[i].ValNeededForCol = make([]bool, len(desc.Columns))
		for j, c := range desc.Columns {
			colIdxMap[c.ID] = j
			// We require all values from the tables being read
			// since we do not expect any projections or rendering
			// on a scan before a join.
			args[i].ValNeededForCol[j] = true
		}
		args[i].ColIdxMap = colIdxMap
		args[i].Desc = &desc
		args[i].Cols = desc.Columns
	}

	return irj.fetcher.Init(args, reverseScan, true /* returnRangeInfo */, alloc)
}

// sendMisplannedRangesMetadata sends information about the non-local ranges
// that were read by this interleaveReaderJoiner.
// This should be called after the fetcher was used to read everything this
// interleaveReaderJoiner was supposed to read.
func (irj *interleaveReaderJoiner) sendMisplannedRangesMetadata(ctx context.Context) {
	rangeInfos := irj.fetcher.GetRangeInfo()
	var misplannedRanges []roachpb.RangeInfo
	for _, ri := range rangeInfos {
		if ri.Lease.Replica.NodeID != irj.flowCtx.nodeID {
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
		irj.out.output.Push(nil /* row */, ProducerMetadata{Ranges: misplannedRanges})
	}
}

// Run is part of the processor interface.
func (irj *interleaveReaderJoiner) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "InterleaveReaderJoiner", irj.tableIDs)
	ctx, span := processorSpan(ctx, "interleave reader joiner")
	defer tracing.FinishSpan(span)

	txn := irj.flowCtx.txn
	if txn == nil {
		log.Fatalf(ctx, "interleaveReaderJoiner outside of txn")
	}

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	if err := irj.fetcher.StartScan(
		ctx, txn, irj.spans, true /* limit batches */, irj.limitHint, false, /* traceKV */
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		irj.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
		irj.out.Close()
		return
	}

	for {
		resp, err := irj.fetcher.NextRow(ctx)
		if err != nil || resp.Row == nil {
			if err != nil {
				irj.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
			}
			break
		}

		var helper postProcHelper
		helperIdx := -1
		for i, h := range irj.tablePosts {
			if resp.Desc.ID == h.tableID && resp.Index.ID == h.indexID {
				helper = h
				helperIdx = i
				break
			}
		}
		if helperIdx == -1 {
			panic("row fetched does not belong to any tables being scanned")
		}

		// We post-process the intermediate row from either table.
		tableRow, consumerStatus, err := helper.post.ProcessRow(ctx, resp.Row)
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
			panic("a child row was fetched before any parent rows were fetched")
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
