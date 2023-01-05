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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

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
//   - the values provided by rowVals, the tuple of source values. The
//     caller ensures this provides values 1-to-1 to the prefix of
//     insertCols that was specified explicitly in the INSERT statement.
//   - the default values for any additional columns in insertCols that
//     have default values in defaultExprs.
//   - the computed values for any additional columns in insertCols
//     that are computed. The mapping in rowContainerForComputedCols
//     maps the indexes of the comptuedCols/computeExpr slices
//     back into indexes in the result row tuple.
func GenerateInsertRow(
	ctx context.Context,
	defaultExprs []tree.TypedExpr,
	computeExprs []tree.TypedExpr,
	insertCols []catalog.Column,
	computedColsLookup []catalog.Column,
	evalCtx *eval.Context,
	tableDesc catalog.TableDescriptor,
	rowVals tree.Datums,
	rowContainerForComputedVals *schemaexpr.RowIndexedVarContainer,
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
			d, err := eval.Expr(ctx, evalCtx, defaultExprs[i])
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
		for i := range computedColsLookup {
			// Note that even though the row is not fully constructed at this point,
			// since we disallow computed columns from referencing other computed
			// columns, all the columns which could possibly be referenced *are*
			// available.
			col := computedColsLookup[i]
			computeIdx := rowContainerForComputedVals.Mapping.GetDefault(col.GetID())
			if !col.IsComputed() {
				continue
			}
			d, err := eval.Expr(ctx, evalCtx, computeExprs[computeIdx])
			if err != nil {
				name := col.GetName()
				return nil, errors.Wrapf(err,
					"computed column %s",
					tree.ErrString((*tree.Name)(&name)))
			}
			rowVals[computeIdx] = d
		}
		evalCtx.PopIVarContainer()
	}

	// Verify the column constraints.
	//
	// During mutations (INSERT, UPDATE, UPSERT), this is checked by
	// sql.enforceLocalColumnConstraints. These checks are required for IMPORT
	// statements.

	// Check to see if NULL is being inserted into any non-nullable column.
	for _, col := range tableDesc.WritableColumns() {
		if !col.IsNullable() {
			if i, ok := rowContainerForComputedVals.Mapping.Get(col.GetID()); !ok || rowVals[i] == tree.DNull {
				return nil, sqlerrors.NewNonNullViolationError(col.GetName())
			}
		}
	}

	// Ensure that the values honor the specified column widths.
	for i := 0; i < len(insertCols); i++ {
		outVal, err := tree.AdjustValueToType(insertCols[i].GetType(), rowVals[i])
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
	KVs     []roachpb.KeyValue
	MemSize int64
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

	tableDesc catalog.TableDescriptor

	// Tracks which column indices in the set of visible columns are part of the
	// user specified target columns. This can be used before populating Datums
	// to filter out unwanted column data.
	TargetColOrds intsets.Fast

	// The rest of these are derived from tableDesc, just cached here.
	ri                        Inserter
	EvalCtx                   *eval.Context
	cols                      []catalog.Column
	VisibleCols               []catalog.Column
	VisibleColTypes           []*types.T
	computedExprs             []tree.TypedExpr
	partialIndexExprs         map[descpb.IndexID]tree.TypedExpr
	defaultCache              []tree.TypedExpr
	computedIVarContainer     schemaexpr.RowIndexedVarContainer
	partialIndexIVarContainer schemaexpr.RowIndexedVarContainer

	// FractionFn is used to set the progress header in KVBatches.
	CompletedRowFn func() int64
	FractionFn     func() float32

	db *kv.DB
}

var kvDatumRowConverterBatchSize = util.ConstantWithMetamorphicTestValue(
	"datum-row-converter-batch-size",
	5000, /* defaultValue */
	1,    /* metamorphicValue */
)

const kvDatumRowConverterBatchMemSize = 4 << 20

// TestingSetDatumRowConverterBatchSize sets kvDatumRowConverterBatchSize and
// returns function to reset this setting back to its old value.
func TestingSetDatumRowConverterBatchSize(newSize int) func() {
	oldSize := kvDatumRowConverterBatchSize
	kvDatumRowConverterBatchSize = newSize
	return func() {
		kvDatumRowConverterBatchSize = oldSize
	}
}

// getSequenceAnnotation returns a mapping from sequence name to metadata
// related to the sequence which will be used when evaluating the default
// expression using the sequence.
func (c *DatumRowConverter) getSequenceAnnotation(
	ctx context.Context, evalCtx *eval.Context, cols []catalog.Column,
) (map[string]*SequenceMetadata, map[descpb.ID]*SequenceMetadata, error) {
	// Identify the sequences used in all the columns.
	var sequenceIDs catalog.DescriptorIDSet
	for _, col := range cols {
		for i := 0; i < col.NumUsesSequences(); i++ {
			id := col.GetUsesSequenceID(i)
			sequenceIDs.Add(id)
		}
	}

	if sequenceIDs.Empty() {
		return nil, nil, nil
	}

	var seqNameToMetadata map[string]*SequenceMetadata
	var seqIDToMetadata map[descpb.ID]*SequenceMetadata
	// TODO(postamar): give the eval.Context a useful interface
	// instead of cobbling a descs.Collection in this way.
	cf := descs.NewBareBonesCollectionFactory(evalCtx.Settings, evalCtx.Codec)
	dsdp := catsessiondata.NewDescriptorSessionDataStackProvider(evalCtx.SessionDataStack)
	descsCol := cf.NewCollection(ctx, descs.WithDescriptorSessionDataProvider(dsdp))
	err := c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		seqNameToMetadata = make(map[string]*SequenceMetadata)
		seqIDToMetadata = make(map[descpb.ID]*SequenceMetadata)
		if err := txn.SetFixedTimestamp(ctx, hlc.Timestamp{WallTime: evalCtx.TxnTimestamp.UnixNano()}); err != nil {
			return err
		}
		seqs, err := descsCol.ByID(txn).Get().Descs(ctx, sequenceIDs.Ordered())
		if err != nil {
			return err
		}
		for _, desc := range seqs {
			seqDesc, err := catalog.AsTableDescriptor(desc)
			if err != nil {
				return err
			}
			if seqDesc.GetSequenceOpts() == nil {
				return errors.Errorf("relation %q (%d) is not a sequence", seqDesc.GetName(), seqDesc.GetID())
			}
			seqMetadata := &SequenceMetadata{SeqDesc: seqDesc}
			seqNameToMetadata[seqDesc.GetName()] = seqMetadata
			seqIDToMetadata[seqDesc.GetID()] = seqMetadata
		}
		return nil
	})
	return seqNameToMetadata, seqIDToMetadata, err
}

// NewDatumRowConverter returns an instance of a DatumRowConverter.
func NewDatumRowConverter(
	ctx context.Context,
	baseSemaCtx *tree.SemaContext,
	tableDesc catalog.TableDescriptor,
	targetColNames tree.NameList,
	evalCtx *eval.Context,
	kvCh chan<- KVBatch,
	seqChunkProvider *SeqChunkProvider,
	metrics *rowinfra.Metrics,
	db *kv.DB,
) (*DatumRowConverter, error) {
	c := &DatumRowConverter{
		tableDesc: tableDesc,
		KvCh:      kvCh,
		EvalCtx:   evalCtx.Copy(),
		db:        db,
	}

	var targetCols []catalog.Column
	var err error
	// IMPORT INTO allows specifying target columns which could be a subset of
	// immutDesc.VisibleColumns. If no target columns are specified we assume all
	// columns of the table descriptor are to be inserted into.
	if len(targetColNames) != 0 {
		if targetCols, err = colinfo.ProcessTargetColumns(tableDesc, targetColNames,
			true /* ensureColumns */, false /* allowMutations */); err != nil {
			return nil, err
		}
	} else {
		targetCols = tableDesc.VisibleColumns()
	}

	var targetColIDs catalog.TableColSet
	for i, col := range targetCols {
		c.TargetColOrds.Add(i)
		targetColIDs.Add(col.GetID())
	}

	var txCtx transform.ExprTransformContext
	relevantColumns := func(col catalog.Column) bool {
		return col.HasDefault() || col.IsComputed()
	}

	// We take a copy of the baseSemaCtx since this method is called by the parallel
	// import workers.
	semaCtxCopy := *baseSemaCtx
	cols := schemaexpr.ProcessColumnSet(targetCols, tableDesc, relevantColumns)
	defaultExprs, err := schemaexpr.MakeDefaultExprs(ctx, cols, &txCtx, c.EvalCtx, &semaCtxCopy)
	if err != nil {
		return nil, errors.Wrap(err, "process default and computed columns")
	}

	ri, err := MakeInserter(
		ctx,
		nil, /* txn */
		evalCtx.Codec,
		tableDesc,
		cols,
		&tree.DatumAlloc{},
		&evalCtx.Settings.SV,
		evalCtx.SessionData().Internal,
		metrics,
	)
	if err != nil {
		return nil, errors.Wrap(err, "make row inserter")
	}

	c.ri = ri
	c.cols = cols

	c.VisibleCols = targetCols
	c.VisibleColTypes = make([]*types.T, len(c.VisibleCols))
	for i := range c.VisibleCols {
		c.VisibleColTypes[i] = c.VisibleCols[i].GetType()
	}

	c.Datums = make([]tree.Datum, len(targetCols), len(cols))
	c.defaultCache = make([]tree.TypedExpr, len(cols))

	annot := make(tree.Annotations, 1)
	var cellInfoAnnot CellInfoAnnotation
	// Currently, this is only true for an IMPORT INTO CSV.
	if seqChunkProvider != nil {
		seqNameToMetadata, seqIDToMetadata, err := c.getSequenceAnnotation(ctx, evalCtx, c.cols)
		if err != nil {
			return nil, err
		}
		cellInfoAnnot.seqNameToMetadata = seqNameToMetadata
		cellInfoAnnot.seqIDToMetadata = seqIDToMetadata
		cellInfoAnnot.seqChunkProvider = seqChunkProvider
	}
	cellInfoAnnot.uniqueRowIDInstance = 0
	annot.Set(cellInfoAddr, &cellInfoAnnot)
	c.EvalCtx.Annotations = &annot

	// Check for a hidden column. This should be the unique_rowid PK if present.
	// In addition, check for non-targeted columns with non-null DEFAULT expressions.
	// If the DEFAULT expression is immutable, we can store it in the cache so that it
	// doesn't have to be reevaluated for every row.
	for i, col := range cols {
		if col.HasDefault() {
			// Placeholder for columns with default values that will be evaluated when
			// each import row is being created.
			typedExpr, volatile, err := sanitizeExprsForImport(ctx, c.EvalCtx, defaultExprs[i], col.GetType())
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
				c.defaultCache[i] = typedExpr
				if volatile == overrideImmutable {
					// This default expression isn't volatile, so we can evaluate once
					// here and memoize it.
					c.defaultCache[i], err = eval.Expr(ctx, c.EvalCtx, c.defaultCache[i])
					if err != nil {
						return nil, errors.Wrapf(err, "error evaluating default expression")
					}
				}
			}
			if !targetColIDs.Contains(col.GetID()) {
				c.Datums = append(c.Datums, nil)
			}
		}
		if col.IsComputed() && !targetColIDs.Contains(col.GetID()) {
			c.Datums = append(c.Datums, nil)
		}
	}
	if len(c.Datums) != len(cols) {
		return nil, errors.New("unexpected hidden column")
	}

	padding := 2 * (len(tableDesc.PublicNonPrimaryIndexes()) + len(tableDesc.GetFamilies()))
	c.BatchCap = kvDatumRowConverterBatchSize + padding
	c.KvBatch.KVs = make([]roachpb.KeyValue, 0, c.BatchCap)
	c.KvBatch.MemSize = 0

	colsOrdered := make([]catalog.Column, len(cols))
	for _, col := range c.tableDesc.PublicColumns() {
		// We prefer to have the order of columns that will be sent into
		// MakeComputedExprs to map that of Datums.
		colsOrdered[ri.InsertColIDtoRowIndex.GetDefault(col.GetID())] = col
	}
	// Here, computeExprs will be nil if there's no computed column, or
	// the list of computed expressions (including nil, for those columns
	// that are not computed) otherwise, according to colsOrdered.
	c.computedExprs, _, err = schemaexpr.MakeComputedExprs(
		ctx,
		colsOrdered,
		c.tableDesc.PublicColumns(),
		c.tableDesc,
		tree.NewUnqualifiedTableName(tree.Name(c.tableDesc.GetName())),
		c.EvalCtx,
		&semaCtxCopy)
	if err != nil {
		return nil, errors.Wrapf(err, "error type checking and building computed expression for IMPORT INTO")
	}

	// Here, partialIndexExprs will be nil if there are no partial indexes, or a
	// map of predicate expressions for each partial index in the input list of
	// indexes.
	c.partialIndexExprs, _, err = schemaexpr.MakePartialIndexExprs(ctx, c.tableDesc.PartialIndexes(),
		c.tableDesc.PublicColumns(), c.tableDesc, c.EvalCtx, &semaCtxCopy)
	if err != nil {
		return nil, errors.Wrapf(err, "error type checking and building partial index expression for IMPORT INTO")
	}

	c.partialIndexIVarContainer = schemaexpr.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    tableDesc.PublicColumns(),
	}

	c.computedIVarContainer = schemaexpr.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    tableDesc.PublicColumns(),
	}
	return c, nil
}

const rowIDBits = 64 - builtinconstants.NodeIDBits

// Row inserts kv operations into the current kv batch, and triggers a SendBatch
// if necessary.
func (c *DatumRowConverter) Row(ctx context.Context, sourceID int32, rowIndex int64) error {
	getCellInfoAnnotation(c.EvalCtx.Annotations).reset(sourceID, rowIndex)
	for i, col := range c.cols {
		if col.HasDefault() {
			// If this column is targeted, then the evaluation is a no-op except to
			// make one evaluation just in case we have random() default expression
			// to ensure that the positions we advance in a row is the same as the
			// number of instances the function random() appears in a row.
			// TODO (anzoteh96): Optimize this part of code when there's no expression
			// involving random(), gen_random_uuid(), or anything like that.
			datum, err := eval.Expr(ctx, c.EvalCtx, c.defaultCache[i])
			if !c.TargetColOrds.Contains(i) {
				if err != nil {
					return errors.Wrapf(
						err, "error evaluating default expression %q", col.GetDefaultExpr())
				}
				c.Datums[i] = datum
			}
		}
	}

	var computedColsLookup []catalog.Column
	if len(c.computedExprs) > 0 {
		computedColsLookup = c.tableDesc.PublicColumns()
	}

	insertRow, err := GenerateInsertRow(
		ctx, c.defaultCache, c.computedExprs, c.cols, computedColsLookup, c.EvalCtx,
		c.tableDesc, c.Datums, &c.computedIVarContainer)
	if err != nil {
		return errors.Wrap(err, "generate insert row")
	}

	// Initialize the PartialIndexUpdateHelper with evaluated predicates for
	// partial indexes.
	var pm PartialIndexUpdateHelper
	{
		c.partialIndexIVarContainer.CurSourceRow = insertRow
		c.EvalCtx.PushIVarContainer(&c.partialIndexIVarContainer)
		partialIndexPutVals := make(tree.Datums, len(c.tableDesc.PartialIndexes()))
		if len(partialIndexPutVals) > 0 {
			for i, idx := range c.tableDesc.PartialIndexes() {
				texpr := c.partialIndexExprs[idx.GetID()]
				val, err := eval.Expr(ctx, c.EvalCtx, texpr)
				if err != nil {
					return errors.Wrap(err, "evaluate partial index expression")
				}
				partialIndexPutVals[i] = val
			}
		}
		err = pm.Init(partialIndexPutVals, nil /* partialIndexDelVals */, c.tableDesc)
		if err != nil {
			return errors.Wrap(err, "error init'ing PartialIndexUpdateHelper")
		}
		c.EvalCtx.PopIVarContainer()
	}

	if err := c.ri.InsertRow(
		ctx,
		KVInserter(func(kv roachpb.KeyValue) {
			kv.Value.InitChecksum(kv.Key)
			c.KvBatch.KVs = append(c.KvBatch.KVs, kv)
			c.KvBatch.MemSize += int64(cap(kv.Key) + cap(kv.Value.RawBytes))
		}),
		insertRow,
		pm,
		true,  /* ignoreConflicts */
		false, /* traceKV */
	); err != nil {
		return errors.Wrap(err, "insert row")
	}
	// If our batch is full, flush it and start a new one.
	if len(c.KvBatch.KVs) >= kvDatumRowConverterBatchSize || c.KvBatch.MemSize > kvDatumRowConverterBatchMemSize {
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
	c.KvBatch.MemSize = 0
	return nil
}
