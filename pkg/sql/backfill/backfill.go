// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// The Column and Index backfill primitives.

package backfill

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// MutationFilter is the type of a simple predicate on a mutation.
type MutationFilter func(sqlbase.DescriptorMutation) bool

// ColumnMutationFilter is a filter that allows mutations that add or drop
// columns.
func ColumnMutationFilter(m sqlbase.DescriptorMutation) bool {
	return m.GetColumn() != nil &&
		(m.Direction == sqlbase.DescriptorMutation_ADD || m.Direction == sqlbase.DescriptorMutation_DROP)
}

// IndexMutationFilter is a filter that allows mutations that add indexes.
func IndexMutationFilter(m sqlbase.DescriptorMutation) bool {
	return m.GetIndex() != nil && m.Direction == sqlbase.DescriptorMutation_ADD
}

// backfiller is common to a ColumnBackfiller or an IndexBackfiller.
type backfiller struct {
	fetcher row.Fetcher
	alloc   sqlbase.DatumAlloc
}

// ColumnBackfiller is capable of running a column backfill for all
// updateCols.
type ColumnBackfiller struct {
	backfiller

	added   []sqlbase.ColumnDescriptor
	dropped []sqlbase.ColumnDescriptor

	// updateCols is a slice of all column descriptors that are being modified.
	updateCols  []sqlbase.ColumnDescriptor
	updateExprs []tree.TypedExpr
	evalCtx     *tree.EvalContext
}

// initCols is a helper to populate some column metadata on a ColumnBackfiller.
func (cb *ColumnBackfiller) initCols(desc *sqlbase.ImmutableTableDescriptor) {
	if len(desc.Mutations) > 0 {
		for _, m := range desc.Mutations {
			if ColumnMutationFilter(m) {
				desc := *m.GetColumn()
				switch m.Direction {
				case sqlbase.DescriptorMutation_ADD:
					cb.added = append(cb.added, desc)
				case sqlbase.DescriptorMutation_DROP:
					cb.dropped = append(cb.dropped, desc)
				}
			}
		}
	}
}

// init performs initialization operations that are shared across the local
// and distributed initialization procedures for the ColumnBackfiller.
func (cb *ColumnBackfiller) init(
	evalCtx *tree.EvalContext,
	defaultExprs []tree.TypedExpr,
	computedExprs []tree.TypedExpr,
	desc *sqlbase.ImmutableTableDescriptor,
) error {
	cb.evalCtx = evalCtx
	cb.updateCols = append(cb.added, cb.dropped...)
	// Populate default or computed values.
	cb.updateExprs = make([]tree.TypedExpr, len(cb.updateCols))
	for j := range cb.added {
		col := &cb.added[j]
		if col.IsComputed() {
			cb.updateExprs[j] = computedExprs[j]
		} else if defaultExprs == nil || defaultExprs[j] == nil {
			cb.updateExprs[j] = tree.DNull
		} else {
			cb.updateExprs[j] = defaultExprs[j]
		}
	}
	for j := range cb.dropped {
		cb.updateExprs[j+len(cb.added)] = tree.DNull
	}

	// We need all the columns.
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(desc.Columns)-1)

	tableArgs := row.FetcherTableArgs{
		Desc:            desc,
		Index:           &desc.PrimaryIndex,
		ColIdxMap:       desc.ColumnIdxMap(),
		Cols:            desc.Columns,
		ValNeededForCol: valNeededForCol,
	}
	return cb.fetcher.Init(
		evalCtx.Codec,
		false, /* reverse */
		sqlbase.ScanLockingStrength_FOR_NONE,
		false, /* isCheck */
		&cb.alloc,
		tableArgs,
	)
}

// InitForLocalUse initializes a ColumnBackfiller for use during local
// execution within a transaction. In this case, the entire backfill process
// is occuring on the gateway as part of the user's transaction.
func (cb *ColumnBackfiller) InitForLocalUse(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	desc *sqlbase.ImmutableTableDescriptor,
) error {
	cb.initCols(desc)
	defaultExprs, err := sqlbase.MakeDefaultExprs(
		ctx, cb.added, &transform.ExprTransformContext{}, evalCtx, semaCtx,
	)
	if err != nil {
		return err
	}
	var txCtx transform.ExprTransformContext
	computedExprs, err := schemaexpr.MakeComputedExprs(
		ctx,
		cb.added,
		desc,
		tree.NewUnqualifiedTableName(tree.Name(desc.Name)),
		&txCtx,
		evalCtx,
		semaCtx,
		true, /* addingCols */
	)
	if err != nil {
		return err
	}
	return cb.init(evalCtx, defaultExprs, computedExprs, desc)
}

// InitForDistributedUse initializes a ColumnBackfiller for use as part of a
// backfill operation executing as part of a distributed flow. In this use,
// the backfill operation manages its own transactions. This separation is
// necessary due to the different procedure for accessing user defined type
// metadata as part of a distributed flow.
func (cb *ColumnBackfiller) InitForDistributedUse(
	ctx context.Context, flowCtx *execinfra.FlowCtx, desc *sqlbase.ImmutableTableDescriptor,
) error {
	cb.initCols(desc)
	evalCtx := flowCtx.NewEvalCtx()
	var defaultExprs, computedExprs []tree.TypedExpr
	// Install type metadata in the target descriptors, as well as resolve any
	// user defined types in the column expressions.
	if err := flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		resolver := flowCtx.TypeResolverFactory.NewTypeResolver(txn)
		// Hydrate all the types present in the table.
		if err := sqlbase.HydrateTypesInTableDescriptor(ctx, desc.TableDesc(), resolver); err != nil {
			return err
		}
		// Set up a SemaContext to type check the default and computed expressions.
		semaCtx := tree.MakeSemaContext()
		semaCtx.TypeResolver = resolver
		var err error
		defaultExprs, err = sqlbase.MakeDefaultExprs(
			ctx, cb.added, &transform.ExprTransformContext{}, evalCtx, &semaCtx,
		)
		if err != nil {
			return err
		}
		var txCtx transform.ExprTransformContext
		computedExprs, err = schemaexpr.MakeComputedExprs(
			ctx,
			cb.added,
			desc,
			tree.NewUnqualifiedTableName(tree.Name(desc.Name)),
			&txCtx,
			evalCtx,
			&semaCtx,
			true, /* addingCols */
		)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	// Release leases on any accessed types now that type metadata is installed.
	// We do this so that leases on any accessed types are not held for the
	// entire backfill process.
	flowCtx.TypeResolverFactory.Descriptors.ReleaseAll(ctx)

	return cb.init(evalCtx, defaultExprs, computedExprs, desc)
}

// RunColumnBackfillChunk runs column backfill over a chunk of the table using
// the span sp provided, for all updateCols.
func (cb *ColumnBackfiller) RunColumnBackfillChunk(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	sp roachpb.Span,
	chunkSize int64,
	alsoCommit bool,
	traceKV bool,
) (roachpb.Key, error) {
	// TODO(dan): Tighten up the bound on the requestedCols parameter to
	// makeRowUpdater.
	requestedCols := make([]sqlbase.ColumnDescriptor, 0, len(tableDesc.Columns)+len(cb.added))
	requestedCols = append(requestedCols, tableDesc.Columns...)
	requestedCols = append(requestedCols, cb.added...)
	ru, err := row.MakeUpdater(
		ctx,
		txn,
		cb.evalCtx.Codec,
		tableDesc,
		cb.updateCols,
		requestedCols,
		row.UpdaterOnlyColumns,
		&cb.alloc,
	)
	if err != nil {
		return roachpb.Key{}, err
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
		ctx, txn, []roachpb.Span{sp}, true /* limitBatches */, chunkSize, traceKV,
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return roachpb.Key{}, err
	}

	oldValues := make(tree.Datums, len(ru.FetchCols))
	updateValues := make(tree.Datums, len(cb.updateExprs))
	b := txn.NewBatch()
	rowLength := 0
	iv := &sqlbase.RowIndexedVarContainer{
		Cols:    append(tableDesc.Columns, cb.added...),
		Mapping: ru.FetchColIDtoRowIndex,
	}
	cb.evalCtx.IVarContainer = iv
	for i := int64(0); i < chunkSize; i++ {
		datums, _, _, err := cb.fetcher.NextRowDecoded(ctx)
		if err != nil {
			return roachpb.Key{}, err
		}
		if datums == nil {
			break
		}
		iv.CurSourceRow = datums

		// Evaluate the new values. This must be done separately for
		// each row so as to handle impure functions correctly.
		for j, e := range cb.updateExprs {
			val, err := e.Eval(cb.evalCtx)
			if err != nil {
				return roachpb.Key{}, sqlbase.NewInvalidSchemaDefinitionError(err)
			}
			if j < len(cb.added) && !cb.added[j].Nullable && val == tree.DNull {
				return roachpb.Key{}, sqlbase.NewNonNullViolationError(cb.added[j].Name)
			}

			// Added computed column values should be usable for the next
			// added columns being backfilled. They have already been type
			// checked.
			if j < len(cb.added) {
				iv.CurSourceRow = append(iv.CurSourceRow, val)
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
		// TODO(mgartner): Add partial index IDs to ignoreIndexes that we should
		// not add entries to or delete entries from.
		var ignoreIndexes util.FastIntSet
		if _, err := ru.UpdateRow(
			ctx, b, oldValues, updateValues, ignoreIndexes, ignoreIndexes, traceKV,
		); err != nil {
			return roachpb.Key{}, err
		}
	}
	// Write the new row values.
	writeBatch := txn.Run
	if alsoCommit {
		writeBatch = txn.CommitInBatch
	}
	if err := writeBatch(ctx, b); err != nil {
		return roachpb.Key{}, ConvertBackfillError(ctx, tableDesc, b)
	}
	return cb.fetcher.Key(), nil
}

// ConvertBackfillError returns a cleaner SQL error for a failed Batch.
func ConvertBackfillError(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor, b *kv.Batch,
) error {
	// A backfill on a new schema element has failed and the batch contains
	// information useful in printing a sensible error. However
	// ConvertBatchError() will only work correctly if the schema elements
	// are "live" in the tableDesc.
	desc, err := tableDesc.MakeFirstMutationPublic(sqlbase.IncludeConstraints)
	if err != nil {
		return err
	}
	return row.ConvertBatchError(ctx, sqlbase.NewImmutableTableDescriptor(*desc.TableDesc()), b)
}

// IndexBackfiller is capable of backfilling all the added index.
type IndexBackfiller struct {
	backfiller

	added []sqlbase.IndexDescriptor
	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	colIdxMap map[sqlbase.ColumnID]int

	types   []*types.T
	rowVals tree.Datums
	evalCtx *tree.EvalContext
}

// ContainsInvertedIndex returns true if backfilling an inverted index.
func (ib *IndexBackfiller) ContainsInvertedIndex() bool {
	for _, idx := range ib.added {
		if idx.Type == sqlbase.IndexDescriptor_INVERTED {
			return true
		}
	}
	return false
}

// Init initializes an IndexBackfiller.
func (ib *IndexBackfiller) Init(
	evalCtx *tree.EvalContext, desc *sqlbase.ImmutableTableDescriptor,
) error {
	ib.evalCtx = evalCtx
	numCols := len(desc.Columns)
	cols := desc.Columns
	if len(desc.Mutations) > 0 {
		cols = make([]sqlbase.ColumnDescriptor, 0, numCols+len(desc.Mutations))
		cols = append(cols, desc.Columns...)
		for _, m := range desc.Mutations {
			if column := m.GetColumn(); column != nil &&
				m.Direction == sqlbase.DescriptorMutation_ADD &&
				m.State == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
				cols = append(cols, *column)
			}
		}
	}

	var valNeededForCol util.FastIntSet
	mutationID := desc.Mutations[0].MutationID
	for _, m := range desc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		if IndexMutationFilter(m) {
			idx := m.GetIndex()
			ib.added = append(ib.added, *idx)
			for i := range cols {
				id := cols[i].ID
				if idx.ContainsColumnID(id) ||
					idx.GetEncodingType(desc.PrimaryIndex.ID) == sqlbase.PrimaryIndexEncoding {
					valNeededForCol.Add(i)
				}
			}
		}
	}

	ib.types = make([]*types.T, len(cols))
	for i := range cols {
		ib.types[i] = cols[i].Type
	}
	ib.colIdxMap = make(map[sqlbase.ColumnID]int, len(cols))
	for i := range cols {
		ib.colIdxMap[cols[i].ID] = i
	}

	tableArgs := row.FetcherTableArgs{
		Desc:            desc,
		Index:           &desc.PrimaryIndex,
		ColIdxMap:       ib.colIdxMap,
		Cols:            cols,
		ValNeededForCol: valNeededForCol,
	}
	return ib.fetcher.Init(
		evalCtx.Codec,
		false, /* reverse */
		sqlbase.ScanLockingStrength_FOR_NONE,
		false, /* isCheck */
		&ib.alloc,
		tableArgs,
	)
}

// BuildIndexEntriesChunk reads a chunk of rows from a table using the span sp
// provided, and builds all the added indexes.
func (ib *IndexBackfiller) BuildIndexEntriesChunk(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	sp roachpb.Span,
	chunkSize int64,
	traceKV bool,
) ([]sqlbase.IndexEntry, roachpb.Key, error) {
	// This ought to be chunkSize but in most tests we are actually building smaller
	// indexes so use a smaller value.
	const initBufferSize = 1000
	entries := make([]sqlbase.IndexEntry, 0, initBufferSize*int64(len(ib.added)))

	// Get the next set of rows.
	//
	// Running the scan and applying the changes in many transactions
	// is fine because the schema change is in the correct state to
	// handle intermediate OLTP commands which delete and add values
	// during the scan. Index entries in the new index are being
	// populated and deleted by the OLTP commands but not otherwise
	// read or used
	if err := ib.fetcher.StartScan(
		ctx, txn, []roachpb.Span{sp}, true /* limitBatches */, initBufferSize, traceKV,
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return nil, nil, err
	}

	buffer := make([]sqlbase.IndexEntry, len(ib.added))
	for i := int64(0); i < chunkSize; i++ {
		encRow, _, _, err := ib.fetcher.NextRow(ctx)
		if err != nil {
			return nil, nil, err
		}
		if encRow == nil {
			break
		}
		if len(ib.rowVals) == 0 {
			ib.rowVals = make(tree.Datums, len(encRow))
		}
		if err := sqlbase.EncDatumRowToDatums(ib.types, ib.rowVals, encRow, &ib.alloc); err != nil {
			return nil, nil, err
		}

		// We're resetting the length of this slice for variable length indexes such as inverted
		// indexes which can append entries to the end of the slice. If we don't do this, then everything
		// EncodeSecondaryIndexes appends to secondaryIndexEntries for a row, would stay in the slice for
		// subsequent rows and we would then have duplicates in entries on output. Additionally, we do
		// not want to include empty k/v pairs while backfilling.
		buffer = buffer[:0]
		if buffer, err = sqlbase.EncodeSecondaryIndexes(
			ib.evalCtx.Codec,
			tableDesc.TableDesc(),
			ib.added,
			ib.colIdxMap,
			ib.rowVals,
			buffer,
			false, /* includeEmpty */
		); err != nil {
			return nil, nil, err
		}
		entries = append(entries, buffer...)
	}
	return entries, ib.fetcher.Key(), nil
}

// RunIndexBackfillChunk runs an index backfill over a chunk of the table
// by tracversing the span sp provided. The backfill is run for the added
// indexes.
func (ib *IndexBackfiller) RunIndexBackfillChunk(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	sp roachpb.Span,
	chunkSize int64,
	alsoCommit bool,
	traceKV bool,
) (roachpb.Key, error) {
	entries, key, err := ib.BuildIndexEntriesChunk(ctx, txn, tableDesc, sp, chunkSize, traceKV)
	if err != nil {
		return nil, err
	}
	batch := txn.NewBatch()

	for _, entry := range entries {
		if traceKV {
			log.VEventf(ctx, 2, "InitPut %s -> %s", entry.Key, entry.Value.PrettyPrint())
		}
		batch.InitPut(entry.Key, &entry.Value, false /* failOnTombstones */)
	}
	writeBatch := txn.Run
	if alsoCommit {
		writeBatch = txn.CommitInBatch
	}
	if err := writeBatch(ctx, batch); err != nil {
		return nil, ConvertBackfillError(ctx, tableDesc, batch)
	}
	return key, nil
}
