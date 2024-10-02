// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"bytes"
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colenc"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type vectorInserter struct {
	colexecop.OneInputHelper
	desc       catalog.TableDescriptor
	insertCols []catalog.Column
	retBatch   coldata.Batch
	flowCtx    *execinfra.FlowCtx
	// checkOrds are the columns containing bool values with check expression
	// results.
	checkOrds intsets.Fast
	// If we have checkOrds we need a sema context to format error messages.
	semaCtx *tree.SemaContext
	// mutationQuota is the number of bytes we'll allow in the kv.Batch before
	// finishing it and starting a new one.
	mutationQuota int
	// If auto commit is true we'll commit the last batch.
	autoCommit bool
}

var _ colexecop.Operator = &vectorInserter{}

// NewInsertOp allocates a new vector insert operator. Currently the only input
// will be a rawBatchOp and only output is row count so this doesn't support
// the full gamut of insert operations.
func NewInsertOp(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.InsertSpec,
	input colexecop.Operator,
	typs []*types.T,
	outputIdx int,
	alloc *colmem.Allocator,
	semaCtx *tree.SemaContext,
) colexecop.Operator {
	// TODO(cucaroach): Should we dispense with the formalities and just pass a
	// pointer to this through with the coldata.Batch?
	desc := flowCtx.TableDescriptor(ctx, &spec.Table)
	insCols := make([]catalog.Column, len(spec.ColumnIDs))
	for i, c := range spec.ColumnIDs {
		col, err := catalog.MustFindColumnByID(desc, c)
		if err != nil {
			colexecerror.InternalError(err)
		}
		insCols[i] = col
	}

	// Empirical testing shows that if ApproximateMutationBytes approaches
	// 32MB we'll hit the command limit. So set limit to a fraction of
	// command limit to be safe.
	mutationQuota := int(kvserverbase.MaxCommandSize.Get(&flowCtx.Cfg.Settings.SV) / 3)

	v := vectorInserter{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		desc:           desc,
		retBatch:       alloc.NewMemBatchWithFixedCapacity(typs, 1),
		flowCtx:        flowCtx,
		insertCols:     insCols,
		mutationQuota:  mutationQuota,
		autoCommit:     spec.AutoCommit,
	}
	if spec.CheckOrds != nil {
		if err := v.checkOrds.Decode(bytes.NewReader(spec.CheckOrds)); err != nil {
			colexecerror.InternalError(err)
		}
		v.semaCtx = semaCtx
	}
	return &v
}

func (v *vectorInserter) getPartialIndexMap(b coldata.Batch) map[catid.IndexID][]bool {
	var partialIndexColMap map[descpb.IndexID][]bool
	// Create a set of partial index IDs to not write to. Indexes should not be
	// written to when they are partial indexes and the row does not satisfy the
	// predicate. This set is passed as a parameter to tableInserter.row below.
	pindexes := v.desc.PartialIndexes()
	if n := len(pindexes); n > 0 {
		colOffset := len(v.insertCols) + v.checkOrds.Len()
		numCols := len(b.ColVecs()) - colOffset
		if numCols != len(pindexes) {
			colexecerror.InternalError(errors.AssertionFailedf("num extra columns didn't match number of partial indexes"))
		}
		for i := 0; i < numCols; i++ {
			if partialIndexColMap == nil {
				partialIndexColMap = make(map[descpb.IndexID][]bool)
			}
			partialIndexColMap[pindexes[i].GetID()] = b.ColVec(i + colOffset).Bool()
		}
	}
	return partialIndexColMap
}

func (v *vectorInserter) Next() coldata.Batch {
	ctx := v.Ctx
	b := v.Input.Next()
	if b.Length() == 0 {
		return coldata.ZeroBatch
	}

	if !v.checkOrds.Empty() {
		if err := v.checkMutationInput(ctx, b); err != nil {
			colexecerror.ExpectedError(err)
		}
	}
	partialIndexColMap := v.getPartialIndexMap(b)

	kvba := row.KVBatchAdapter{}
	var p row.Putter = &kvba
	if v.flowCtx.TraceKV {
		p = &row.TracePutter{Putter: p, Ctx: ctx}
	}
	// In the future we could sort across multiple goroutines, not worth it yet,
	// time here is minimal compared to time spent executing batch.
	p = &row.SortingPutter{Putter: p}
	enc := colenc.MakeEncoder(v.flowCtx.Codec(), v.desc, &v.flowCtx.Cfg.Settings.SV, b, v.insertCols, v.flowCtx.GetRowMetrics(), partialIndexColMap,
		func() error {
			if kvba.Batch.ApproximateMutationBytes() > v.mutationQuota {
				return colenc.ErrOverMemLimit
			}
			return nil
		})
	// PrepareBatch is called in a loop to partially insert till everything is
	// done, if there are a ton of secondary indexes we could hit raft
	// command limit building kv batch so we need to be able to do
	// it in chunks of rows.
	end := b.Length()
	start := 0
	for start < b.Length() {
		kvba.Batch = v.flowCtx.Txn.NewBatch()
		if err := enc.PrepareBatch(ctx, p, start, end); err != nil {
			if errors.Is(err, colenc.ErrOverMemLimit) {
				log.VEventf(ctx, 2, "vector insert memory limit err %d, numrows: %d", start, end)
				end /= 2
				// If one row blows out memory limit, just do one row at a time.
				if end <= start {
					// Disable memory limit, if the system can't handle this row
					// a KV error will be encountered below.
					v.mutationQuota = math.MaxInt
					end = start + 1
				}
				// Throw everything away and start over.
				kvba.Batch = v.flowCtx.Txn.NewBatch()
				continue
			}
			colexecerror.ExpectedError(err)
		}
		log.VEventf(ctx, 2, "copy running batch, autocommit: %v, final: %v, numrows: %d", v.autoCommit, end == b.Length(), end-start)
		var err error
		if v.autoCommit && end == b.Length() {
			err = v.flowCtx.Txn.CommitInBatch(ctx, kvba.Batch)
		} else {
			err = v.flowCtx.Txn.Run(ctx, kvba.Batch)
		}
		if err != nil {
			colexecerror.ExpectedError(row.ConvertBatchError(ctx, v.desc, kvba.Batch))
		}
		numRows := end - start
		start = end
		end += numRows
		if end > b.Length() {
			end = b.Length()
		}
	}

	v.retBatch.ResetInternalBatch()
	v.retBatch.ColVec(0).Int64()[0] = int64(b.Length())
	v.retBatch.SetLength(1)

	v.flowCtx.Cfg.StatsRefresher.NotifyMutation(v.desc, b.Length())

	return v.retBatch
}

func (v *vectorInserter) checkMutationInput(ctx context.Context, b coldata.Batch) error {
	checks := v.desc.EnforcedCheckConstraints()
	colIdx := 0
	for i, ch := range checks {
		if !v.checkOrds.Contains(i) {
			continue
		}
		vec := b.ColVec(colIdx + len(v.insertCols))
		bools := vec.Bool()
		nulls := vec.Nulls()
		for r := 0; r < b.Length(); r++ {
			if !bools[r] && !nulls.NullAt(r) {
				return row.CheckFailed(ctx, v.flowCtx.EvalCtx, v.semaCtx, v.flowCtx.EvalCtx.SessionData(), v.desc, ch)
			}
		}
		colIdx++
	}
	return nil
}
