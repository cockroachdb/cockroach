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
	"context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// colBatchScan is the exec.Operator implementation of TableReader. It reads a table
// from kv, presenting it as ColBatches via the exec.Operator interface.
type colBatchScan struct {
	spans     roachpb.Spans
	flowCtx   *FlowCtx
	rf        *row.CFetcher
	limitHint int64
	ctx       context.Context
}

var _ exec.Operator = &colBatchScan{}

func (s *colBatchScan) Init() {
	s.ctx = context.Background()
	if err := s.rf.StartScan(
		s.ctx, s.flowCtx.txn, s.spans,
		true /* limit batches */, s.limitHint, s.flowCtx.traceKV,
	); err != nil {
		panic(err)
	}
}

func (s *colBatchScan) Next() exec.ColBatch {
	bat, err := s.rf.NextBatch(s.ctx)
	if err != nil {
		panic(err)
	}
	bat.SetSelection(false)
	return bat
}

// newColBatchScan creates a new colBatchScan operator.
func newColBatchScan(
	flowCtx *FlowCtx, spec *TableReaderSpec, post *PostProcessSpec,
) (*colBatchScan, error) {
	if flowCtx.nodeID == 0 {
		return nil, errors.Errorf("attempting to create a colBatchScan with uninitialized NodeID")
	}

	limitHint := limitHint(spec.LimitHint, post)

	numCols := len(spec.Table.Columns)
	returnMutations := spec.Visibility == ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	if returnMutations {
		for i := range spec.Table.Mutations {
			if spec.Table.Mutations[i].GetColumn() != nil {
				numCols++
			}
		}
	}
	typs := spec.Table.ColumnTypesWithMutations(returnMutations)
	helper := ProcOutputHelper{}
	if err := helper.Init(
		post,
		typs,
		flowCtx.NewEvalCtx(),
		nil,
	); err != nil {
		return nil, err
	}

	neededColumns := helper.neededColumns()

	columnIdxMap := spec.Table.ColumnIdxMapWithMutations(returnMutations)
	fetcher := row.CFetcher{}
	if _, _, err := initCRowFetcher(
		&fetcher, &spec.Table, int(spec.IndexIdx), columnIdxMap, spec.Reverse,
		neededColumns, spec.IsCheck, spec.Visibility,
	); err != nil {
		return nil, err
	}

	nSpans := len(spec.Spans)
	spans := make(roachpb.Spans, nSpans)
	for i := range spans {
		spans[i] = spec.Spans[i].Span
	}
	return &colBatchScan{
		spans:     spans,
		flowCtx:   flowCtx,
		rf:        &fetcher,
		limitHint: limitHint,
	}, nil
}

// initCRowFetcher initializes a row.CFetcher. See initRowFetcher.
func initCRowFetcher(
	fetcher *row.CFetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	colIdxMap map[sqlbase.ColumnID]int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	scanVisibility ScanVisibility,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	index, isSecondaryIndex, err = desc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	cols := desc.Columns
	if scanVisibility == ScanVisibility_PUBLIC_AND_NOT_PUBLIC {
		if len(desc.Mutations) > 0 {
			cols = make([]sqlbase.ColumnDescriptor, 0, len(desc.Columns)+len(desc.Mutations))
			cols = append(cols, desc.Columns...)
			for _, mutation := range desc.Mutations {
				if c := mutation.GetColumn(); c != nil {
					col := *c
					// Even if the column is non-nullable it can be null in the
					// middle of a schema change.
					col.Nullable = true
					cols = append(cols, col)
				}
			}
		}
	}
	tableArgs := row.FetcherTableArgs{
		Desc:             sqlbase.NewImmutableTableDescriptor(*desc),
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             cols,
		ValNeededForCol:  valNeededForCol,
	}
	if err := fetcher.Init(
		reverseScan, true /* returnRangeInfo */, isCheck, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}
