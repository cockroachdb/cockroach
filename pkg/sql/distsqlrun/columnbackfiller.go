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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// columnBackfiller is a processor for backfilling columns.
type columnBackfiller struct {
	backfiller

	added   []sqlbase.ColumnDescriptor
	dropped []sqlbase.ColumnDescriptor
	// updateCols is a slice of all column descriptors that are being modified.
	updateCols  []sqlbase.ColumnDescriptor
	updateExprs []tree.TypedExpr

	evalCtx *tree.EvalContext
	iv      *sqlbase.RowIndexedVarContainer
}

var _ Processor = &columnBackfiller{}
var _ chunkBackfiller = &columnBackfiller{}

// ColumnMutationFilter is a filter that allows mutations that add or drop
// columns.
func ColumnMutationFilter(m sqlbase.DescriptorMutation) bool {
	return m.GetColumn() != nil && (m.Direction == sqlbase.DescriptorMutation_ADD || m.Direction == sqlbase.DescriptorMutation_DROP)
}

// GetColumnMutations returns the columns being added on dropped in any
// schema mutation.
func GetColumnMutations(desc *sqlbase.TableDescriptor) (added, dropped []sqlbase.ColumnDescriptor) {
	for _, m := range desc.Mutations {
		if ColumnMutationFilter(m) {
			desc := *m.GetColumn()
			switch m.Direction {
			case sqlbase.DescriptorMutation_ADD:
				added = append(added, desc)
			case sqlbase.DescriptorMutation_DROP:
				dropped = append(dropped, desc)
			}
		}
	}
	return added, dropped
}

func newColumnBackfiller(
	flowCtx *FlowCtx, spec BackfillerSpec, post *PostProcessSpec, output RowReceiver,
) (*columnBackfiller, error) {
	cb := &columnBackfiller{
		backfiller: backfiller{
			name:    "Column",
			filter:  ColumnMutationFilter,
			flowCtx: flowCtx,
			output:  output,
			spec:    spec,
		},
	}
	cb.backfiller.chunkBackfiller = cb

	if err := cb.init(); err != nil {
		return nil, err
	}

	return cb, nil
}

func (cb *columnBackfiller) init() error {
	desc := cb.spec.Table

	cb.evalCtx = cb.flowCtx.NewEvalCtx()
	cb.iv = &sqlbase.RowIndexedVarContainer{
		Cols:    desc.Columns,
		Mapping: sqlbase.ColIDtoRowIndexFromCols(desc.Columns),
	}
	cb.evalCtx.IVarContainer = cb.iv

	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	var colIdxMap map[sqlbase.ColumnID]int

	cb.added, cb.dropped = GetColumnMutations(&desc)
	cb.updateCols = append(cb.added, cb.dropped...)

	cb.updateExprs = make([]tree.TypedExpr, len(cb.spec.UpdateExprs))
	semaCtx := &tree.SemaContext{IVarContainer: cb.iv}
	for i, exprSpec := range cb.spec.UpdateExprs {
		expr, err := parser.ParseExpr(exprSpec.Expr)
		if err != nil {
			return err
		}

		cb.updateExprs[i], err = tree.TypeCheck(expr, semaCtx, cb.updateCols[i].Type.ToDatumType())
		if err != nil {
			return err
		}
	}

	// We need all the columns.
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(desc.Columns)-1)

	colIdxMap = make(map[sqlbase.ColumnID]int, len(desc.Columns))
	for i, c := range desc.Columns {
		colIdxMap[c.ID] = i
	}

	tableArgs := sqlbase.RowFetcherTableArgs{
		Desc:            &desc,
		Index:           &desc.PrimaryIndex,
		ColIdxMap:       colIdxMap,
		Cols:            desc.Columns,
		ValNeededForCol: valNeededForCol,
	}
	return cb.fetcher.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, &cb.alloc, tableArgs,
	)
}

// runChunk implements the chunkBackfiller interface.
func (cb *columnBackfiller) runChunk(
	ctx context.Context,
	mutations []sqlbase.DescriptorMutation,
	sp roachpb.Span,
	chunkSize int64,
	readAsOf hlc.Timestamp,
) (roachpb.Key, error) {
	tableDesc := cb.backfiller.spec.Table
	err := cb.flowCtx.clientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if cb.flowCtx.testingKnobs.RunBeforeBackfillChunk != nil {
			if err := cb.flowCtx.testingKnobs.RunBeforeBackfillChunk(sp); err != nil {
				return err
			}
		}
		if cb.flowCtx.testingKnobs.RunAfterBackfillChunk != nil {
			defer cb.flowCtx.testingKnobs.RunAfterBackfillChunk()
		}

		fkTables, _ := sqlbase.TablesNeededForFKs(
			ctx,
			tableDesc,
			sqlbase.CheckUpdates,
			sqlbase.NoLookup,
			sqlbase.NoCheckPrivilege,
			nil, /* AnalyzeExprFunction */
		)
		for _, fkTableDesc := range cb.spec.OtherTables {
			found, ok := fkTables[fkTableDesc.ID]
			if !ok {
				// We got passed an extra table for some reason - just ignore it.
				continue
			}
			found.Table = &fkTableDesc
			fkTables[fkTableDesc.ID] = found
		}
		for id, table := range fkTables {
			if table.Table == nil {
				// We weren't passed all of the tables that we need by the coordinator.
				return errors.Errorf("table %v not sent by coordinator", id)
			}
		}
		// TODO(dan): Tighten up the bound on the requestedCols parameter to
		// makeRowUpdater.
		requestedCols := make([]sqlbase.ColumnDescriptor, 0, len(tableDesc.Columns)+len(cb.added))
		requestedCols = append(requestedCols, tableDesc.Columns...)
		requestedCols = append(requestedCols, cb.added...)
		ru, err := sqlbase.MakeRowUpdater(
			txn,
			&tableDesc,
			fkTables,
			cb.updateCols,
			requestedCols,
			sqlbase.RowUpdaterOnlyColumns,
			&cb.flowCtx.EvalCtx,
			&cb.alloc,
		)
		if err != nil {
			return err
		}

		// TODO(dan): This check is an unfortunate bleeding of the internals of
		// rowUpdater. Extract the sql row to k/v mapping logic out into something
		// usable here.
		if !ru.IsColumnOnlyUpdate() {
			panic("only column data should be modified, but the rowUpdater is configured otherwise")
		}

		// Get the next set of rows.
		//
		// Running the scan and applying the changes in many transactions
		// is fine because the schema change is in the correct state to
		// handle intermediate OLTP commands which delete and add values
		// during the scan. Index entries in the new index are being
		// populated and deleted by the OLTP commands but not otherwise
		// read or used
		if err := cb.fetcher.StartScan(
			ctx, txn, []roachpb.Span{sp}, true /* limitBatches */, chunkSize, false, /* traceKV */
		); err != nil {
			log.Errorf(ctx, "scan error: %s", err)
			return err
		}

		oldValues := make(tree.Datums, len(ru.FetchCols))
		updateValues := make(tree.Datums, len(cb.updateExprs))
		b := txn.NewBatch()
		rowLength := 0
		for i := int64(0); i < chunkSize; i++ {
			datums, _, _, err := cb.fetcher.NextRowDecoded(ctx)
			if err != nil {
				return err
			}
			if datums == nil {
				break
			}
			cb.iv.CurSourceRow = datums

			// Evaluate the new values. This must be done separately for
			// each row so as to handle impure functions correctly.
			for j, e := range cb.updateExprs {
				val, err := e.Eval(cb.evalCtx)
				if err != nil {
					return sqlbase.NewInvalidSchemaDefinitionError(err)
				}
				if j < len(cb.added) && !cb.added[j].Nullable && val == tree.DNull {
					return sqlbase.NewNonNullViolationError(cb.added[j].Name)
				}
				updateValues[j] = val
			}
			copy(oldValues, datums)
			// Update oldValues with NULL values where values weren't found;
			// only update when necessary.
			if rowLength != len(datums) {
				rowLength = len(datums)
				for j := rowLength; j < len(oldValues); j++ {
					oldValues[j] = tree.DNull
				}
			}
			if _, err := ru.UpdateRow(
				ctx, b, oldValues, updateValues, sqlbase.CheckFKs, false, /* traceKV */
			); err != nil {
				return err
			}
		}
		// Write the new row values.
		if err := txn.CommitInBatch(ctx, b); err != nil {
			return ConvertBackfillError(ctx, &cb.spec.Table, b)
		}
		return nil
	})
	return cb.fetcher.Key(), err
}
