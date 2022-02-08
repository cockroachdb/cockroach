// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var insertFastPathNodePool = sync.Pool{
	New: func() interface{} {
		return &insertFastPathNode{}
	},
}

// insertFastPathNode is a faster implementation of inserting values in a table
// and performing FK checks. It is used when all the foreign key checks can be
// performed via a direct lookup in an index, and when the input is VALUES of
// limited size (at most mutations.MaxBatchSize).
type insertFastPathNode struct {
	// input values, similar to a valuesNode.
	input [][]tree.TypedExpr

	// columns is set if this INSERT is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns colinfo.ResultColumns

	run insertFastPathRun
}

var _ mutationPlanNode = &insertFastPathNode{}

type insertFastPathRun struct {
	insertRun

	fkChecks []insertFastPathFKCheck

	numInputCols int

	// inputBuf stores the evaluation result of the input rows, linearized into a
	// single slice; see inputRow(). Unfortunately we can't do everything one row
	// at a time, because we need the datums for generating error messages in case
	// an FK check fails.
	inputBuf tree.Datums

	// fkBatch accumulates the FK existence checks.
	fkBatch roachpb.BatchRequest
	// fkSpanInfo keeps track of information for each fkBatch.Request entry.
	fkSpanInfo []insertFastPathFKSpanInfo

	// fkSpanMap is used to de-duplicate FK existence checks. Only used if there
	// is more than one input row.
	fkSpanMap map[string]struct{}
}

// insertFastPathFKSpanInfo records information about each Request in the
// fkBatch, associating it with a specific check and row index.
type insertFastPathFKSpanInfo struct {
	check  *insertFastPathFKCheck
	rowIdx int
}

// insertFastPathFKCheck extends exec.InsertFastPathFKCheck with metadata that
// is computed once and can be reused across rows.
type insertFastPathFKCheck struct {
	exec.InsertFastPathFKCheck

	tabDesc      catalog.TableDescriptor
	idx          catalog.Index
	keyPrefix    []byte
	colMap       catalog.TableColMap
	spanBuilder  span.Builder
	spanSplitter span.Splitter
}

func (c *insertFastPathFKCheck) init(params runParams) error {
	idx := c.ReferencedIndex.(*optIndex)
	c.tabDesc = c.ReferencedTable.(*optTable).desc
	c.idx = idx.idx

	codec := params.ExecCfg().Codec
	c.keyPrefix = rowenc.MakeIndexKeyPrefix(codec, c.tabDesc.GetID(), c.idx.GetID())
	c.spanBuilder.Init(params.EvalContext(), codec, c.tabDesc, c.idx)
	c.spanSplitter = span.MakeSplitter(c.tabDesc, c.idx, util.FastIntSet{} /* neededColOrdinals */)

	if len(c.InsertCols) > idx.numLaxKeyCols {
		return errors.AssertionFailedf(
			"%d FK cols, only %d cols in index", len(c.InsertCols), idx.numLaxKeyCols,
		)
	}
	for i, ord := range c.InsertCols {
		var colID descpb.ColumnID
		if i < c.idx.NumKeyColumns() {
			colID = c.idx.GetKeyColumnID(i)
		} else {
			colID = c.idx.GetKeySuffixColumnID(i - c.idx.NumKeyColumns())
		}

		c.colMap.Set(colID, int(ord))
	}
	return nil
}

// generateSpan returns the span that we need to look up to confirm existence of
// the referenced row.
func (c *insertFastPathFKCheck) generateSpan(inputRow tree.Datums) (roachpb.Span, error) {
	return row.FKCheckSpan(&c.spanBuilder, c.spanSplitter, inputRow, c.colMap, len(c.InsertCols))
}

// errorForRow returns an error indicating failure of this FK check for the
// given row.
func (c *insertFastPathFKCheck) errorForRow(inputRow tree.Datums) error {
	values := make(tree.Datums, len(c.InsertCols))
	for i, ord := range c.InsertCols {
		values[i] = inputRow[ord]
	}
	return c.MkErr(values)
}

func (r *insertFastPathRun) inputRow(rowIdx int) tree.Datums {
	start := rowIdx * r.numInputCols
	end := start + r.numInputCols
	return r.inputBuf[start:end:end]
}

// addFKChecks adds Requests to fkBatch and entries in fkSpanInfo / fkSpanMap as
// needed for checking foreign keys for the given row.
func (r *insertFastPathRun) addFKChecks(
	ctx context.Context, rowIdx int, inputRow tree.Datums,
) error {
	for i := range r.fkChecks {
		c := &r.fkChecks[i]

		// See if we have any nulls.
		numNulls := 0
		for _, ord := range c.InsertCols {
			if inputRow[ord] == tree.DNull {
				numNulls++
			}
		}
		if numNulls > 0 {
			if c.MatchMethod == tree.MatchFull && numNulls != len(c.InsertCols) {
				return c.errorForRow(inputRow)
			}
			// We have a row with only NULLS, or a row with some NULLs and match
			// method PARTIAL. We can skip this FK check for this row.
			continue
		}

		span, err := c.generateSpan(inputRow)
		if err != nil {
			return err
		}
		if r.fkSpanMap != nil {
			_, exists := r.fkSpanMap[string(span.Key)]
			if exists {
				// Duplicate span.
				continue
			}
			r.fkSpanMap[string(span.Key)] = struct{}{}
		}
		if r.traceKV {
			log.VEventf(ctx, 2, "FKScan %s", span)
		}
		reqIdx := len(r.fkBatch.Requests)
		r.fkBatch.Requests = append(r.fkBatch.Requests, roachpb.RequestUnion{})
		r.fkBatch.Requests[reqIdx].MustSetInner(&roachpb.ScanRequest{
			RequestHeader: roachpb.RequestHeaderFromSpan(span),
		})
		r.fkSpanInfo = append(r.fkSpanInfo, insertFastPathFKSpanInfo{
			check:  c,
			rowIdx: rowIdx,
		})
	}
	return nil
}

// runFKChecks runs the fkBatch and checks that all spans return at least one
// key.
func (n *insertFastPathNode) runFKChecks(params runParams) error {
	if len(n.run.fkBatch.Requests) == 0 {
		return nil
	}
	defer n.run.fkBatch.Reset()

	// Run the FK checks batch.
	br, err := params.p.txn.Send(params.ctx, n.run.fkBatch)
	if err != nil {
		return err.GoError()
	}

	for i := range br.Responses {
		resp := br.Responses[i].GetInner().(*roachpb.ScanResponse)
		if len(resp.Rows) == 0 {
			// No results for lookup; generate the violation error.
			info := n.run.fkSpanInfo[i]
			return info.check.errorForRow(n.run.inputRow(info.rowIdx))
		}
	}

	return nil
}

func (n *insertFastPathNode) startExec(params runParams) error {
	// Cache traceKV during execution, to avoid re-evaluating it for every row.
	n.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	n.run.initRowContainer(params, n.columns)

	n.run.numInputCols = len(n.input[0])
	n.run.inputBuf = make(tree.Datums, len(n.input)*n.run.numInputCols)

	if len(n.input) > 1 {
		n.run.fkSpanMap = make(map[string]struct{})
	}

	if len(n.run.fkChecks) > 0 {
		for i := range n.run.fkChecks {
			if err := n.run.fkChecks[i].init(params); err != nil {
				return err
			}
		}
		maxSpans := len(n.run.fkChecks) * len(n.input)
		n.run.fkBatch.Requests = make([]roachpb.RequestUnion, 0, maxSpans)
		n.run.fkSpanInfo = make([]insertFastPathFKSpanInfo, 0, maxSpans)
		if len(n.input) > 1 {
			n.run.fkSpanMap = make(map[string]struct{}, maxSpans)
		}
	}

	return n.run.ti.init(params.ctx, params.p.txn, params.EvalContext(), &params.EvalContext().Settings.SV)
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *insertFastPathNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *insertFastPathNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (n *insertFastPathNode) BatchedNext(params runParams) (bool, error) {
	if n.run.done {
		return false, nil
	}

	// The fast path node does everything in one batch.

	for rowIdx, tupleRow := range n.input {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}
		inputRow := n.run.inputRow(rowIdx)
		for col, typedExpr := range tupleRow {
			var err error
			inputRow[col], err = typedExpr.Eval(params.EvalContext())
			if err != nil {
				err = interceptAlterColumnTypeParseError(n.run.insertCols, col, err)
				return false, err
			}
		}
		// Process the insertion for the current source row, potentially
		// accumulating the result row for later.
		if err := n.run.processSourceRow(params, inputRow); err != nil {
			return false, err
		}

		// Add FK existence checks.
		if len(n.run.fkChecks) > 0 {
			if err := n.run.addFKChecks(params.ctx, rowIdx, inputRow); err != nil {
				return false, err
			}
		}
	}

	// Perform the FK checks.
	// TODO(radu): we could run the FK batch in parallel with the main batch (if
	// we aren't auto-committing).
	if err := n.runFKChecks(params); err != nil {
		return false, err
	}

	n.run.ti.setRowsWrittenLimit(params.extendedEvalCtx.SessionData())
	if err := n.run.ti.finalize(params.ctx); err != nil {
		return false, err
	}
	// Remember we're done for the next call to BatchedNext().
	n.run.done = true

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(n.run.ti.ri.Helper.TableDesc, len(n.input))

	return true, nil
}

// BatchedCount implements the batchedPlanNode interface.
func (n *insertFastPathNode) BatchedCount() int { return len(n.input) }

// BatchedCount implements the batchedPlanNode interface.
func (n *insertFastPathNode) BatchedValues(rowIdx int) tree.Datums { return n.run.ti.rows.At(rowIdx) }

func (n *insertFastPathNode) Close(ctx context.Context) {
	n.run.ti.close(ctx)
	*n = insertFastPathNode{}
	insertFastPathNodePool.Put(n)
}

func (n *insertFastPathNode) rowsWritten() int64 {
	return n.run.ti.rowsWritten
}

// See planner.autoCommit.
func (n *insertFastPathNode) enableAutoCommit() {
	n.run.ti.enableAutoCommit()
}

// interceptAlterColumnTypeParseError wraps a type parsing error with a warning
// about the column undergoing an ALTER COLUMN TYPE schema change.
// If colNum is not -1, only the colNum'th column in insertCols will be checked
// for AlterColumnTypeInProgress, otherwise every column in insertCols will
// be checked.
func interceptAlterColumnTypeParseError(insertCols []catalog.Column, colNum int, err error) error {
	// Only intercept the error if the column being inserted into
	// is an actual column. This is to avoid checking on values that don't
	// correspond to an actual column, for example a check constraint.
	if colNum >= len(insertCols) {
		return err
	}
	var insertCol catalog.Column

	// wrapParseError is a helper function that checks if an insertCol has the
	// AlterColumnTypeInProgress flag and wraps the parse error msg stating
	// that the error may be because the column is being altered.
	// Returns if the error msg has been wrapped and the wrapped error msg.
	wrapParseError := func(insertCol catalog.Column, colNum int, err error) (bool, error) {
		if insertCol.ColumnDesc().AlterColumnTypeInProgress {
			code := pgerror.GetPGCode(err)
			if code == pgcode.InvalidTextRepresentation {
				if colNum != -1 {
					// If a column is specified, we can ensure the parse error
					// is happening because the column is undergoing an alter column type
					// schema change.
					return true, errors.Wrapf(err,
						"This table is still undergoing the ALTER COLUMN TYPE schema change, "+
							"this insert is not supported until the schema change is finalized")
				}
				// If no column is specified, the error message is slightly changed to say
				// that the error MAY be because a column is undergoing an alter column type
				// schema change.
				return true, errors.Wrap(err,
					"This table is still undergoing the ALTER COLUMN TYPE schema change, "+
						"this insert may not be supported until the schema change is finalized")
			}
		}
		return false, err
	}

	// If a colNum is specified, we just check the one column for
	// AlterColumnTypeInProgress and return the error whether it's wrapped or not.
	if colNum != -1 {
		insertCol = insertCols[colNum]
		_, err = wrapParseError(insertCol, colNum, err)
		return err
	}

	// If the colNum is -1, we check every insertCol for AlterColumnTypeInProgress.
	for _, insertCol = range insertCols {
		var changed bool
		changed, err = wrapParseError(insertCol, colNum, err)
		if changed {
			return err
		}
	}

	return err
}
