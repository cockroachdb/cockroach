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
//
// Author: Jordan Lewis (jordan@cockroachlabs.com)

package distsqlrun

import (
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// columnBackfiller is a processor for backfilling columns.
type columnBackfiller struct {
	backfiller

	added   []sqlbase.ColumnDescriptor
	dropped []sqlbase.ColumnDescriptor
	// updateCols is a slice of all column descriptors that are being modified.
	updateCols  []sqlbase.ColumnDescriptor
	updateExprs []parser.TypedExpr
}

var _ processor = &columnBackfiller{}
var _ chunkBackfiller = &columnBackfiller{}

// ColumnMutationFilter is a filter that allows mutations that add or drop
// columns.
func ColumnMutationFilter(m sqlbase.DescriptorMutation) bool {
	return m.GetColumn() != nil && (m.Direction == sqlbase.DescriptorMutation_ADD || m.Direction == sqlbase.DescriptorMutation_DROP)
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

	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	var colIdxMap map[sqlbase.ColumnID]int

	// Note if there is a new non nullable column with no default value.
	// If that's the case, and we end up reading a non-zero amount of data,
	// we need a throw an error since the old columns will already violate the
	// not null constraint.
	// TODO(jordan): detect this earlier. #14455
	addingNonNullableColumn := false
	if len(desc.Mutations) > 0 {
		for _, m := range desc.Mutations {
			if ColumnMutationFilter(m) {
				switch m.Direction {
				case sqlbase.DescriptorMutation_ADD:
					desc := *m.GetColumn()
					cb.added = append(cb.added, desc)
					if desc.DefaultExpr == nil && !desc.Nullable {
						addingNonNullableColumn = true
					}
				case sqlbase.DescriptorMutation_DROP:
					cb.dropped = append(cb.dropped, *m.GetColumn())
				}
			}
		}
	}
	defaultExprs, err := sqlbase.MakeDefaultExprs(cb.added, &parser.Parser{}, &cb.flowCtx.evalCtx)
	if err != nil {
		return err
	}

	cb.updateCols = append(cb.added, cb.dropped...)
	if len(cb.dropped) > 0 || addingNonNullableColumn || len(defaultExprs) > 0 {
		// Populate default values.
		cb.updateExprs = make([]parser.TypedExpr, len(cb.updateCols))
		for j := range cb.added {
			if defaultExprs == nil || defaultExprs[j] == nil {
				cb.updateExprs[j] = parser.DNull
			} else {
				cb.updateExprs[j] = defaultExprs[j]
			}
		}
		for j := range cb.dropped {
			cb.updateExprs[j+len(cb.added)] = parser.DNull
		}
	}

	// We need all the columns.
	valNeededForCol := make([]bool, len(desc.Columns))
	for i := range valNeededForCol {
		valNeededForCol[i] = true
	}

	colIdxMap = make(map[sqlbase.ColumnID]int, len(desc.Columns))
	for i, c := range desc.Columns {
		colIdxMap[c.ID] = i
	}
	return cb.fetcher.Init(
		&desc, colIdxMap, &desc.PrimaryIndex, false, false, desc.Columns, valNeededForCol, false,
	)
}

// runChunk implements the chunkBackfiller interface.
func (cb *columnBackfiller) runChunk(
	ctx context.Context, mutations []sqlbase.DescriptorMutation, sp roachpb.Span, chunkSize int64,
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

		fkTables := sqlbase.TablesNeededForFKs(tableDesc, sqlbase.CheckUpdates)
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
			txn, &tableDesc, fkTables, cb.updateCols, requestedCols, sqlbase.RowUpdaterOnlyColumns,
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
			ctx, txn, []roachpb.Span{sp}, true /* limitBatches */, chunkSize,
		); err != nil {
			log.Errorf(ctx, "scan error: %s", err)
			return err
		}

		oldValues := make(parser.Datums, len(ru.FetchCols))
		updateValues := make(parser.Datums, len(cb.updateExprs))
		b := txn.NewBatch()
		rowLength := 0
		for i := int64(0); i < chunkSize; i++ {
			row, err := cb.fetcher.NextRowDecoded(ctx)
			if err != nil {
				return err
			}
			if row == nil {
				break
			}
			// Evaluate the new values. This must be done separately for
			// each row so as to handle impure functions correctly.
			for j, e := range cb.updateExprs {
				val, err := e.Eval(&cb.flowCtx.evalCtx)
				if err != nil {
					return sqlbase.NewInvalidSchemaDefinitionError(err)
				}
				if j < len(cb.added) && !cb.added[j].Nullable && val == parser.DNull {
					return sqlbase.NewNonNullViolationError(cb.added[j].Name)
				}
				updateValues[j] = val
			}
			copy(oldValues, row)
			// Update oldValues with NULL values where values weren't found;
			// only update when necessary.
			if rowLength != len(row) {
				rowLength = len(row)
				for j := rowLength; j < len(oldValues); j++ {
					oldValues[j] = parser.DNull
				}
			}
			if _, err := ru.UpdateRow(ctx, b, oldValues, updateValues); err != nil {
				return err
			}
		}
		// Write the new row values.
		if err := txn.CommitInBatch(ctx, b); err != nil {
			return ConvertBackfillError(&cb.spec.Table, b)
		}
		return nil
	})
	return cb.fetcher.Key(), err
}
