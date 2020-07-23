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
	"github.com/cockroachdb/errors"
)

// Given that imports can be retried and resumed, we want to
// ensure that the default functions return the same value given
// the same arguments, even on retries. Therfore we decide to support
// only a limited subset of non-immutable functions, which are
// all listed here.
var supportedImportFunctions = map[string]struct{}{
	// These methods can be supported given that we set the statement
	// and transaction timestamp to be equal, i.e. the write timestamp.
	"current_date":          {},
	"current_timestamp":     {},
	"localtimestamp":        {},
	"now":                   {},
	"statement_timestamp":   {},
	"timeofday":             {},
	"transaction_timestamp": {},
}

func unsafeExpressionError(err error, msg string, expr string) error {
	return errors.Wrapf(err, "default expression %q is unsafe for import: %s", expr, msg)
}

// unsafeErrExpr is a wrapper for errors arising from unsafe default
// expression created at row converter stage so that the appropriate
// error can be returned at the Row() stage.
type unsafeErrExpr struct {
	tree.TypedExpr
	err error
}

var _ tree.TypedExpr = &unsafeErrExpr{}

// Eval implements the TypedExpr interface.
func (e *unsafeErrExpr) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	return nil, e.err
}

// importDefaultExprVisitor must be invoked on a typed expression. This
// visitor walks the tree and ensures that any expression in the tree
// that's not immutable is what we explicitly support.
type importDefaultExprVisitor struct {
	err error
}

// VisitPre implements tree.Visitor interface.
func (v *importDefaultExprVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	return v.err == nil, expr
}

// VisitPost implements tree.Visitor interface.
func (v *importDefaultExprVisitor) VisitPost(expr tree.Expr) (newExpr tree.Expr) {
	if v.err != nil {
		return expr
	}
	switch fn := expr.(type) {
	case *tree.FuncExpr:
		if fn.ResolvedOverload().Volatility > tree.VolatilityImmutable {
			resolvedFnName := fn.Func.FunctionReference.(*tree.FunctionDefinition).Name
			if _, ok := supportedImportFunctions[resolvedFnName]; !ok {
				v.err = errors.Newf(`function %s unsupported by IMPORT INTO`, resolvedFnName)
			}
		}
	}
	return expr
}

// SanitizeExprsForImport checks whether default expressions are supported
// for import.
func SanitizeExprsForImport(
	ctx context.Context, evalCtx *tree.EvalContext, expr tree.Expr, targetType *types.T,
) (tree.TypedExpr, error) {
	semaCtx := tree.MakeSemaContext()

	// If we have immutable expressions, then we can just return it right away.
	typedExpr, err := sqlbase.SanitizeVarFreeExpr(
		ctx, expr, targetType, "import_default", &semaCtx, tree.VolatilityImmutable)
	if err == nil {
		return typedExpr, nil
	}
	// Now that the expressions are not immutable, we first check that they
	// are of the correct type before checking for any unsupported functions
	// for import.
	typedExpr, err = tree.TypeCheck(ctx, expr, &semaCtx, targetType)
	if err != nil {
		return nil, unsafeExpressionError(err, "type checking error", expr.String())
	}
	v := &importDefaultExprVisitor{}
	newExpr, _ := tree.WalkExpr(v, typedExpr)
	if v.err != nil {
		return nil, unsafeExpressionError(v.err, "expr walking error", expr.String())
	}
	return newExpr.(tree.TypedExpr), nil
}

// KVInserter implements the putter interface.
type KVInserter func(roachpb.KeyValue)

// CPut is not implmented.
func (i KVInserter) CPut(key, value interface{}, expValue []byte) {
	panic("unimplemented")
}

// Del is not implemented.
func (i KVInserter) Del(key ...interface{}) {
	// This is called when there are multiple column families to ensure that
	// existing data is cleared. With the exception of IMPORT INTO, the entire
	// existing keyspace in any IMPORT is guaranteed to be empty, so we don't have
	// to worry about it.
	//
	// IMPORT INTO disallows overwriting an existing row, so we're also okay here.
	// The reason this works is that row existence is precisely defined as whether
	// column family 0 exists, meaning that we write column family 0 even if all
	// the non-pk columns in it are NULL. It follows that either the row does
	// exist and the imported column family 0 will conflict (and the IMPORT INTO
	// will fail) or the row does not exist (and thus the column families are all
	// empty).
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
		outVal, err := sqlbase.AdjustValueToColumnType(insertCols[i].Type, rowVals[i], &insertCols[i].Name)
		if err != nil {
			return nil, err
		}
		rowVals[i] = outVal
	}

	return rowVals, nil
}

// KVBatch represents a batch of KVs generated from converted rows.
type KVBatch struct {
	// Source is where the row data in the batch came from.
	Source int32
	// LastRow is the index of the last converted row in source in this batch.
	LastRow int64
	// Progress represents the fraction of the input that generated this row.
	Progress float32
	// KVs is the actual converted KV data.
	KVs []roachpb.KeyValue
}

// DatumRowConverter converts Datums into kvs and streams it to the destination
// channel.
type DatumRowConverter struct {
	// current row buf
	Datums []tree.Datum

	// kv destination and current batch
	KvCh     chan<- KVBatch
	KvBatch  KVBatch
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
	defaultCache          []tree.TypedExpr
	computedIVarContainer sqlbase.RowIndexedVarContainer

	// FractionFn is used to set the progress header in KVBatches.
	CompletedRowFn func() int64
	FractionFn     func() float32
}

var kvDatumRowConverterBatchSize = 5000

// TestingSetDatumRowConverterBatchSize sets kvDatumRowConverterBatchSize and returns function to
// reset this setting back to its old value.
func TestingSetDatumRowConverterBatchSize(newSize int) func() {
	kvDatumRowConverterBatchSize = newSize
	return func() {
		kvDatumRowConverterBatchSize = 5000
	}
}

// NewDatumRowConverter returns an instance of a DatumRowConverter.
func NewDatumRowConverter(
	ctx context.Context,
	tableDesc *sqlbase.TableDescriptor,
	targetColNames tree.NameList,
	evalCtx *tree.EvalContext,
	kvCh chan<- KVBatch,
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
	for i, col := range targetColDescriptors {
		if _, ok := isTargetColID[col.ID]; !ok {
			continue
		}
		c.IsTargetCol[i] = struct{}{}
	}

	var txCtx transform.ExprTransformContext
	semaCtx := tree.MakeSemaContext()
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(ctx, targetColDescriptors, immutDesc, &txCtx, c.EvalCtx, &semaCtx)
	if err != nil {
		return nil, errors.Wrap(err, "process default columns")
	}

	ri, err := MakeInserter(
		ctx,
		nil, /* txn */
		evalCtx.Codec,
		immutDesc,
		cols,
		&sqlbase.DatumAlloc{},
	)
	if err != nil {
		return nil, errors.Wrap(err, "make row inserter")
	}

	c.ri = ri
	c.cols = cols

	c.VisibleCols = targetColDescriptors
	c.VisibleColTypes = make([]*types.T, len(c.VisibleCols))
	for i := range c.VisibleCols {
		c.VisibleColTypes[i] = c.VisibleCols[i].DatumType()
	}

	c.Datums = make([]tree.Datum, len(targetColDescriptors), len(cols))
	c.defaultCache = make([]tree.TypedExpr, len(cols))

	// Check for a hidden column. This should be the unique_rowid PK if present.
	// In addition, check for non-targeted columns with non-null DEFAULT expressions.
	// If the DEFAULT expression is immutable, we can store it in the cache so that it
	// doesn't have to be reevaluated for every row.
	isTargetCol := func(col *sqlbase.ColumnDescriptor) bool {
		_, ok := isTargetColID[col.ID]
		return ok
	}
	c.hidden = -1
	for i := range cols {
		col := &cols[i]
		if col.Hidden {
			if col.DefaultExpr == nil || *col.DefaultExpr != "unique_rowid()" || c.hidden != -1 {
				return nil, errors.New("unexpected hidden column")
			}
			c.hidden = i
			c.Datums = append(c.Datums, nil)
		} else {
			if col.DefaultExpr != nil {
				// Placeholder for columns with default values that will be evaluated when
				// each import row is being created.
				c.defaultCache[i], err = SanitizeExprsForImport(ctx, evalCtx, defaultExprs[i], col.Type)
				if err != nil {
					// This expression may not be safe for import but we don't want to
					// call the user out at this stage: targeted columns may not have
					// been identified now (e.g. "IMPORT PGDUMP...") and we want to
					// throw an error only at the "Row" stage when the targeted columns
					// have been identified.
					c.defaultCache[i] = &unsafeErrExpr{
						err: errors.Wrapf(err, "default expression %s unsafe for import", defaultExprs[i].String()),
					}
				} else {
					// TODO (anzoteh96): currently, all the functions we support are immutable
					// or stable, so it's okay for defaultCache to be evaluated here. However,
					// when we add support for unique_rowid, it changes for every row and
					// therefore can no longer be evaluated right away. We therefore
					// need a flag to determine when we can evaluate here.
					c.defaultCache[i], err = c.defaultCache[i].Eval(evalCtx)
					if err != nil {
						return nil, errors.Wrapf(err, "error evaluating default expression")
					}
				}
				if !isTargetCol(col) {
					c.Datums = append(c.Datums, nil)
				}
			}
		}
	}
	if len(c.Datums) != len(cols) {
		return nil, errors.New("unexpected hidden column")
	}

	padding := 2 * (len(immutDesc.Indexes) + len(immutDesc.Families))
	c.BatchCap = kvDatumRowConverterBatchSize + padding
	c.KvBatch.KVs = make([]roachpb.KeyValue, 0, c.BatchCap)

	c.computedIVarContainer = sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    immutDesc.Columns,
	}
	return c, nil
}

const rowIDBits = 64 - builtins.NodeIDBits

// Row inserts kv operations into the current kv batch, and triggers a SendBatch
// if necessary.
func (c *DatumRowConverter) Row(ctx context.Context, sourceID int32, rowIndex int64) error {
	if c.hidden >= 0 {
		// We don't want to call unique_rowid() for the hidden PK column because it
		// is not idempotent and has unfortunate overlapping of output spans since
		// it puts the uniqueness-ensuring per-generator part (nodeID) in the
		// low-bits. Instead, make our own IDs that attempt to keep each generator
		// (sourceID) writing to its own key-space with sequential rowIndexes
		// mapping to sequential unique IDs, by putting the rowID in the lower
		// bits. To avoid collisions with the SQL-genenerated IDs (at least for a
		// very long time) we also flip the top bit to 1.
		//
		// Producing sequential keys in non-overlapping spans for each source yields
		// observed improvements in ingestion performance of ~2-3x and even more
		// significant reductions in required compactions during IMPORT.
		//
		// TODO(dt): Note that currently some callers (e.g. CSV IMPORT, which can be
		// used on a table more than once) offset their rowIndex by a wall-time at
		// which their overall job is run, so that subsequent ingestion jobs pick
		// different row IDs for the i'th row and don't collide. However such
		// time-offset rowIDs mean each row imported consumes some unit of time that
		// must then elapse before the next IMPORT could run without colliding e.g.
		// a 100m row file would use 10µs/row or ~17min worth of IDs. For now it is
		// likely that IMPORT's write-rate is still the limiting factor, but this
		// scheme means rowIndexes are very large (1 yr in 10s of µs is about 2^42).
		// Finding an alternative scheme for avoiding collisions (like sourceID *
		// fileIndex*desc.Version) could improve on this. For now, if this
		// best-effort collision avoidance scheme doesn't work in some cases we can
		// just recommend an explicit PK as a workaround.
		avoidCollisionsWithSQLsIDs := uint64(1 << 63)
		rowID := (uint64(sourceID) << rowIDBits) ^ uint64(rowIndex)
		c.Datums[c.hidden] = tree.NewDInt(tree.DInt(avoidCollisionsWithSQLsIDs | rowID))
	}

	for i := range c.cols {
		col := &c.cols[i]
		if _, ok := c.IsTargetCol[i]; !ok && !col.Hidden && col.DefaultExpr != nil {
			datum, err := c.defaultCache[i].Eval(c.EvalCtx)
			if err != nil {
				return errors.Wrapf(
					err, "error evaluating default expression %q", col.DefaultExprStr())
			}
			c.Datums[i] = datum
		}
	}

	// TODO(justin): we currently disallow computed columns in import statements.
	var computeExprs []tree.TypedExpr
	var computedCols []sqlbase.ColumnDescriptor

	insertRow, err := GenerateInsertRow(
		c.defaultCache, computeExprs, c.cols, computedCols, c.EvalCtx,
		c.tableDesc, c.Datums, &c.computedIVarContainer)
	if err != nil {
		return errors.Wrap(err, "generate insert row")
	}
	// TODO(mgartner): Add partial index IDs to ignoreIndexes that we should
	// not delete entries from.
	var pm PartialIndexUpdateHelper
	if err := c.ri.InsertRow(
		ctx,
		KVInserter(func(kv roachpb.KeyValue) {
			kv.Value.InitChecksum(kv.Key)
			c.KvBatch.KVs = append(c.KvBatch.KVs, kv)
		}),
		insertRow,
		pm,
		true,  /* ignoreConflicts */
		false, /* traceKV */
	); err != nil {
		return errors.Wrap(err, "insert row")
	}
	// If our batch is full, flush it and start a new one.
	if len(c.KvBatch.KVs) >= kvDatumRowConverterBatchSize {
		if err := c.SendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}

// SendBatch streams kv operations from the current KvBatch to the destination
// channel, and resets the KvBatch to empty.
func (c *DatumRowConverter) SendBatch(ctx context.Context) error {
	if len(c.KvBatch.KVs) == 0 {
		return nil
	}
	if c.FractionFn != nil {
		c.KvBatch.Progress = c.FractionFn()
	}
	if c.CompletedRowFn != nil {
		c.KvBatch.LastRow = c.CompletedRowFn()
	}
	select {
	case c.KvCh <- c.KvBatch:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.KvBatch.KVs = make([]roachpb.KeyValue, 0, c.BatchCap)
	return nil
}
