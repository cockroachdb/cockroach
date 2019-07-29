// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// KVInserter implements the putter interface.
type KVInserter func(roachpb.KeyValue)

// CPut is not implmented.
func (i KVInserter) CPut(key, value, expValue interface{}) {
	panic("unimplemented")
}

// Del is not implemented.
func (i KVInserter) Del(key ...interface{}) {
	panic("unimplemented")
}

// Put method of the putter interface.
func (i KVInserter) Put(key, value interface{}) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

// InitPut method of the putter interface.
func (i KVInserter) InitPut(key, value interface{}, failOnTombstones bool) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

// GenerateInsertRow prepares a row tuple for insertion. It fills in default
// expressions, verifies non-nullable columns, and checks column widths.
//
// The result is a row tuple providing values for every column in insertCols.
// This results contains:
//
// - the values provided by rowVals, the tuple of source values. The
//   caller ensures this provides values 1-to-1 to the prefix of
//   insertCols that was specified explicitly in the INSERT statement.
// - the default values for any additional columns in insertCols that
//   have default values in defaultExprs.
// - the computed values for any additional columns in insertCols
//   that are computed. The mapping in rowContainerForComputedCols
//   maps the indexes of the comptuedCols/computeExpr slices
//   back into indexes in the result row tuple.
//
func GenerateInsertRow(
	defaultExprs []tree.TypedExpr,
	computeExprs []tree.TypedExpr,
	insertCols []sqlbase.ColumnDescriptor,
	computedCols []sqlbase.ColumnDescriptor,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	rowVals tree.Datums,
	rowContainerForComputedVals *sqlbase.RowIndexedVarContainer,
) (tree.Datums, error) {
	// The values for the row may be shorter than the number of columns being
	// inserted into. Generate default values for those columns using the
	// default expressions. This will not happen if the row tuple was produced
	// by a ValuesClause, because all default expressions will have been populated
	// already by fillDefaults.
	if len(rowVals) < len(insertCols) {
		// It's not cool to append to the slice returned by a node; make a copy.
		oldVals := rowVals
		rowVals = make(tree.Datums, len(insertCols))
		copy(rowVals, oldVals)

		for i := len(oldVals); i < len(insertCols); i++ {
			if defaultExprs == nil {
				rowVals[i] = tree.DNull
				continue
			}
			d, err := defaultExprs[i].Eval(evalCtx)
			if err != nil {
				return nil, err
			}
			rowVals[i] = d
		}
	}

	// Generate the computed values, if needed.
	if len(computeExprs) > 0 {
		rowContainerForComputedVals.CurSourceRow = rowVals
		evalCtx.PushIVarContainer(rowContainerForComputedVals)
		for i := range computedCols {
			// Note that even though the row is not fully constructed at this point,
			// since we disallow computed columns from referencing other computed
			// columns, all the columns which could possibly be referenced *are*
			// available.
			d, err := computeExprs[i].Eval(evalCtx)
			if err != nil {
				return nil, errors.Wrapf(err, "computed column %s", tree.ErrString((*tree.Name)(&computedCols[i].Name)))
			}
			rowVals[rowContainerForComputedVals.Mapping[computedCols[i].ID]] = d
		}
		evalCtx.PopIVarContainer()
	}

	// Verify the column constraints.
	//
	// We would really like to use enforceLocalColumnConstraints() here,
	// but this is not possible because of some brain damage in the
	// Insert() constructor, which causes insertCols to contain
	// duplicate columns descriptors: computed columns are listed twice,
	// one will receive a NULL value and one will receive a comptued
	// value during execution. It "works out in the end" because the
	// latter (non-NULL) value overwrites the earlier, but
	// enforceLocalColumnConstraints() does not know how to reason about
	// this.
	//
	// In the end it does not matter much, this code is going away in
	// favor of the (simpler, correct) code in the CBO.

	// Check to see if NULL is being inserted into any non-nullable column.
	for _, col := range tableDesc.WritableColumns() {
		if !col.Nullable {
			if i, ok := rowContainerForComputedVals.Mapping[col.ID]; !ok || rowVals[i] == tree.DNull {
				return nil, sqlbase.NewNonNullViolationError(col.Name)
			}
		}
	}

	// Ensure that the values honor the specified column widths.
	for i := 0; i < len(insertCols); i++ {
		outVal, err := sqlbase.LimitValueWidth(&insertCols[i].Type, rowVals[i], &insertCols[i].Name)
		if err != nil {
			return nil, err
		}
		rowVals[i] = outVal
	}

	return rowVals, nil
}

// DatumRowConverter converts Datums into kvs and streams it to the destination
// channel.
type DatumRowConverter struct {
	// current row buf
	Datums []tree.Datum

	// kv destination and current batch
	KvCh     chan<- []roachpb.KeyValue
	KvBatch  []roachpb.KeyValue
	BatchCap int

	tableDesc *sqlbase.ImmutableTableDescriptor

	// Tracks which column indices in the set of visible columns are part of the
	// user specified target columns. This can be used before populating Datums
	// to filter out unwanted column data.
	IsTargetCol map[int]struct{}

	// The rest of these are derived from tableDesc, just cached here.
	hidden                int
	ri                    Inserter
	EvalCtx               *tree.EvalContext
	cols                  []sqlbase.ColumnDescriptor
	VisibleCols           []sqlbase.ColumnDescriptor
	VisibleColTypes       []*types.T
	defaultExprs          []tree.TypedExpr
	computedIVarContainer sqlbase.RowIndexedVarContainer
}

const kvDatumRowConverterBatchSize = 5000

// NewDatumRowConverter returns an instance of a DatumRowConverter.
func NewDatumRowConverter(
	tableDesc *sqlbase.TableDescriptor,
	targetColNames tree.NameList,
	evalCtx *tree.EvalContext,
	kvCh chan<- []roachpb.KeyValue,
) (*DatumRowConverter, error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*tableDesc)
	c := &DatumRowConverter{
		tableDesc: immutDesc,
		KvCh:      kvCh,
		EvalCtx:   evalCtx,
	}

	var targetColDescriptors []sqlbase.ColumnDescriptor
	var err error
	// IMPORT INTO allows specifying target columns which could be a subset of
	// immutDesc.VisibleColumns. If no target columns are specified we assume all
	// columns of the table descriptor are to be inserted into.
	if len(targetColNames) != 0 {
		if targetColDescriptors, err = sqlbase.ProcessTargetColumns(immutDesc, targetColNames,
			true /* ensureColumns */, false /* allowMutations */); err != nil {
			return nil, err
		}
	} else {
		targetColDescriptors = immutDesc.VisibleColumns()
	}

	isTargetColID := make(map[sqlbase.ColumnID]struct{})
	for _, col := range targetColDescriptors {
		isTargetColID[col.ID] = struct{}{}
	}

	c.IsTargetCol = make(map[int]struct{})
	for i, col := range immutDesc.VisibleColumns() {
		if _, ok := isTargetColID[col.ID]; !ok {
			continue
		}
		c.IsTargetCol[i] = struct{}{}
	}

	var txCtx transform.ExprTransformContext
	// We do not currently support DEFAULT expressions on target or non-target
	// columns. All non-target columns must be nullable and will be set to NULL
	// during import. We do however support DEFAULT on hidden columns (which is
	// only the default _rowid one). This allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(targetColDescriptors, immutDesc, &txCtx, c.EvalCtx)
	if err != nil {
		return nil, errors.Wrap(err, "process default columns")
	}

	ri, err := MakeInserter(nil /* txn */, immutDesc, nil, /* fkTables */
		cols, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return nil, errors.Wrap(err, "make row inserter")
	}

	c.ri = ri
	c.cols = cols
	c.defaultExprs = defaultExprs

	c.VisibleCols = immutDesc.VisibleColumns()
	c.VisibleColTypes = make([]*types.T, len(c.VisibleCols))
	for i := range c.VisibleCols {
		c.VisibleColTypes[i] = c.VisibleCols[i].DatumType()
	}

	c.Datums = make([]tree.Datum, len(targetColDescriptors), len(cols))

	// Check for a hidden column. This should be the unique_rowid PK if present.
	c.hidden = -1
	for i := range cols {
		col := &cols[i]
		if col.Hidden {
			if col.DefaultExpr == nil || *col.DefaultExpr != "unique_rowid()" || c.hidden != -1 {
				return nil, errors.New("unexpected hidden column")
			}
			c.hidden = i
			c.Datums = append(c.Datums, nil)
		}
	}
	if len(c.Datums) != len(cols) {
		return nil, errors.New("unexpected hidden column")
	}

	padding := 2 * (len(immutDesc.Indexes) + len(immutDesc.Families))
	c.BatchCap = kvDatumRowConverterBatchSize + padding
	c.KvBatch = make([]roachpb.KeyValue, 0, c.BatchCap)

	c.computedIVarContainer = sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    immutDesc.Columns,
	}
	return c, nil
}

// Row inserts kv operations into the current kv batch, and triggers a SendBatch
// if necessary.
func (c *DatumRowConverter) Row(ctx context.Context, fileIndex int32, rowIndex int64) error {
	if c.hidden >= 0 {
		// We don't want to call unique_rowid() for the hidden PK column because
		// it is not idempotent. The sampling from the first stage will be useless
		// during the read phase, producing a single range split with all of the
		// data. Instead, we will call our own function that mimics that function,
		// but more-or-less guarantees that it will not interfere with the numbers
		// that will be produced by it. The lower 15 bits mimic the node id, but as
		// the CSV file number. The upper 48 bits are the line number and mimic the
		// timestamp. It would take a file with many more than 2**32 lines to even
		// begin approaching what unique_rowid would return today, so we assume it
		// to be safe. Since the timestamp is won't overlap, it is safe to use any
		// number in the node id portion. The 15 bits in that portion should account
		// for up to 32k CSV files in a single IMPORT. In the case of > 32k files,
		// the data is xor'd so the final bits are flipped instead of set.
		c.Datums[c.hidden] = tree.NewDInt(builtins.GenerateUniqueID(fileIndex, uint64(rowIndex)))
	}

	// TODO(justin): we currently disallow computed columns in import statements.
	var computeExprs []tree.TypedExpr
	var computedCols []sqlbase.ColumnDescriptor

	insertRow, err := GenerateInsertRow(
		c.defaultExprs, computeExprs, c.cols, computedCols, c.EvalCtx, c.tableDesc, c.Datums, &c.computedIVarContainer)
	if err != nil {
		return errors.Wrap(err, "generate insert row")
	}
	if err := c.ri.InsertRow(
		ctx,
		KVInserter(func(kv roachpb.KeyValue) {
			kv.Value.InitChecksum(kv.Key)
			c.KvBatch = append(c.KvBatch, kv)
		}),
		insertRow,
		true, /* ignoreConflicts */
		SkipFKs,
		false, /* traceKV */
	); err != nil {
		return errors.Wrap(err, "insert row")
	}
	// If our batch is full, flush it and start a new one.
	if len(c.KvBatch) >= kvDatumRowConverterBatchSize {
		if err := c.SendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}

// SendBatch streams kv operations from the current KvBatch to the destination
// channel, and resets the KvBatch to empty.
func (c *DatumRowConverter) SendBatch(ctx context.Context) error {
	if len(c.KvBatch) == 0 {
		return nil
	}
	select {
	case c.KvCh <- c.KvBatch:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.KvBatch = make([]roachpb.KeyValue, 0, c.BatchCap)
	return nil
}
