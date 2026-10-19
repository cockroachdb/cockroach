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
	"github.com/cockroachdb/cockroach/pkg/kv"
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
	// If auto commit is true we'll try to commit the txn along with the last
	// kv.Batch once the input is exhausted.
	autoCommit bool
	// pendingBatch, set only when autoCommit is true, is the most recently
	// built kv.Batch, not yet sent. It is held back because the input may
	// deliver the rows in several coldata.Batches (e.g. a Columnarizer
	// wrapping a row-execution processor grows its batches from one row), so
	// only once the input is exhausted do we know it is the final kv.Batch and
	// can commit with it. It is sent without committing just before the next
	// kv.Batch is built.
	pendingBatch *kv.Batch
	// rowsWritten tracks the number of rows written by the vectorInserter so
	// far.
	rowsWritten int
	// done is set once the input is exhausted and the row count is emitted.
	done bool
}

var _ colexecop.Operator = &vectorInserter{}

// NewInsertOp allocates a new vector insert operator. It is only used by COPY
// and its only output is the row count, so it doesn't support the full gamut
// of insert operations. The input may produce any number of batches.
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

// Next drains the input, writing every row, and then emits a single batch
// with the total row count. Consumers of a mutation's row count expect exactly
// one such row, no matter how many batches the input produced.
func (v *vectorInserter) Next() coldata.Batch {
	if v.done {
		return coldata.ZeroBatch
	}
	for {
		b := v.Input.Next()
		if b.Length() == 0 {
			break
		}
		v.insertBatch(b)
	}
	if v.pendingBatch != nil {
		v.commitPendingBatch()
	}
	v.flowCtx.Cfg.StatsRefresher.NotifyMutation(v.desc, v.rowsWritten)
	v.done = true

	v.retBatch.ResetInternalBatch()
	v.retBatch.ColVec(0).Int64()[0] = int64(v.rowsWritten)
	v.retBatch.SetLength(1)
	return v.retBatch
}

// insertBatch writes the rows of b, possibly across several kv.Batches. When
// autoCommit is true, the last of them is left in pendingBatch.
func (v *vectorInserter) insertBatch(b coldata.Batch) {
	if !v.checkOrds.Empty() {
		if err := v.checkMutationInput(v.Ctx, b); err != nil {
			colexecerror.ExpectedError(err)
		}
	}
	partialIndexColMap := v.getPartialIndexMap(b)

	kvba := row.KVBatchAdapter{}
	var p row.Putter = &kvba
	if v.flowCtx.TraceKV {
		p = &row.TracePutter{Putter: p, Ctx: v.Ctx}
	}
	// In the future we could sort across multiple goroutines, not worth it yet,
	// time here is minimal compared to time spent executing batch.
	p = &row.SortingPutter{Putter: p}
	enc := colenc.MakeEncoder(
		v.flowCtx.Codec(), v.desc, v.flowCtx.EvalCtx.SessionData(), &v.flowCtx.Cfg.Settings.SV,
		b, v.insertCols, v.flowCtx.GetRowMetrics(), partialIndexColMap,
		func() error {
			if kvba.Batch.ApproximateMutationBytes() > v.mutationQuota {
				return colenc.ErrOverMemLimit
			}
			return nil
		},
	)
	// PrepareBatch is called in a loop to partially insert till everything is
	// done, if there are a ton of secondary indexes we could hit raft
	// command limit building kv batch so we need to be able to do
	// it in chunks of rows.
	end := b.Length()
	start := 0
	for start < b.Length() {
		if v.pendingBatch != nil {
			v.runBatch(v.pendingBatch, false /* commit */)
			v.pendingBatch = nil
		}
		kvba.Batch = v.flowCtx.Txn.NewBatch()
		if err := enc.PrepareBatch(v.Ctx, p, start, end); err != nil {
			if errors.Is(err, colenc.ErrOverMemLimit) {
				log.VEventf(v.Ctx, 2, "vector insert memory limit err %d, numrows: %d", start, end)
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
		log.VEventf(v.Ctx, 2, "copy built batch, numrows: %d", end-start)
		if v.autoCommit {
			v.pendingBatch = kvba.Batch
		} else {
			v.runBatch(kvba.Batch, false /* commit */)
		}
		numRows := end - start
		start = end
		end += numRows
		if end > b.Length() {
			end = b.Length()
		}
	}

	v.rowsWritten += b.Length()
}

// commitPendingBatch sends the final kv.Batch and, deadline permitting, commits
// the txn with it.
func (v *vectorInserter) commitPendingBatch() {
	// Similar to tableWriterBase.finalize, we examine whether it's likely that
	// we'll be able to auto-commit. If it seems unlikely based on the deadline,
	// we won't auto-commit which might allow the connExecutor to get a fresh
	// deadline before committing.
	commit := !v.flowCtx.Txn.DeadlineLikelySufficient()
	v.runBatch(v.pendingBatch, commit)
	v.pendingBatch = nil
}

// runBatch sends b in the txn, committing the txn with it if commit is true.
func (v *vectorInserter) runBatch(b *kv.Batch, commit bool) {
	log.VEventf(v.Ctx, 2, "copy running batch, autocommit: %v", commit)
	var err error
	if commit {
		err = v.flowCtx.Txn.CommitInBatch(v.Ctx, b)
	} else {
		err = v.flowCtx.Txn.Run(v.Ctx, b)
	}
	if err != nil {
		colexecerror.ExpectedError(row.ConvertBatchError(
			v.Ctx, v.desc, b, false, /* alwaysConvertCondFailed */
		))
	}
}

func (v *vectorInserter) checkMutationInput(ctx context.Context, b coldata.Batch) error {
	checks := v.desc.EnforcedCheckValidators()
	colIdx := 0
	for i, ch := range checks {
		if !v.checkOrds.Contains(i) {
			continue
		}
		vec := b.ColVec(colIdx + len(v.insertCols))
		bools := vec.Bool()
		nulls := vec.Nulls()
		for r := 0; r < b.Length(); r++ {
			if ch.IsCheckFailed(bools[r], nulls.NullAt(r)) {
				return row.CheckFailed(ctx, v.flowCtx.EvalCtx, v.semaCtx, v.flowCtx.EvalCtx.SessionData(), v.desc, ch)
			}
		}
		colIdx++
	}
	return nil
}
