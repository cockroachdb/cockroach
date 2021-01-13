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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// MutationFilter is the type of a simple predicate on a mutation.
type MutationFilter func(descpb.DescriptorMutation) bool

// ColumnMutationFilter is a filter that allows mutations that add or drop
// columns.
func ColumnMutationFilter(m descpb.DescriptorMutation) bool {
	return m.GetColumn() != nil &&
		(m.Direction == descpb.DescriptorMutation_ADD || m.Direction == descpb.DescriptorMutation_DROP)
}

// IndexMutationFilter is a filter that allows mutations that add indexes.
func IndexMutationFilter(m descpb.DescriptorMutation) bool {
	return m.GetIndex() != nil && m.Direction == descpb.DescriptorMutation_ADD
}

// backfiller is common to a ColumnBackfiller or an IndexBackfiller.
type backfiller struct {
	fetcher row.Fetcher
	alloc   rowenc.DatumAlloc
}

// ColumnBackfiller is capable of running a column backfill for all
// updateCols.
type ColumnBackfiller struct {
	backfiller

	added   []descpb.ColumnDescriptor
	dropped []descpb.ColumnDescriptor

	// updateCols is a slice of all column descriptors that are being modified.
	updateCols  []descpb.ColumnDescriptor
	updateExprs []tree.TypedExpr
	evalCtx     *tree.EvalContext

	// mon is a memory monitor linked with the ColumnBackfiller on creation.
	mon *mon.BytesMonitor
}

// initCols is a helper to populate some column metadata on a ColumnBackfiller.
func (cb *ColumnBackfiller) initCols(desc *tabledesc.Immutable) {
	if len(desc.Mutations) > 0 {
		for _, m := range desc.Mutations {
			if ColumnMutationFilter(m) {
				desc := *m.GetColumn()
				switch m.Direction {
				case descpb.DescriptorMutation_ADD:
					cb.added = append(cb.added, desc)
				case descpb.DescriptorMutation_DROP:
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
	desc *tabledesc.Immutable,
	mon *mon.BytesMonitor,
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
		Index:           desc.GetPrimaryIndex().IndexDesc(),
		ColIdxMap:       desc.ColumnIdxMap(),
		Cols:            desc.Columns,
		ValNeededForCol: valNeededForCol,
	}

	// Create a bound account associated with the column backfiller.
	if mon == nil {
		return errors.AssertionFailedf("no memory monitor linked to ColumnBackfiller during init")
	}
	cb.mon = mon

	return cb.fetcher.Init(
		evalCtx.Context,
		evalCtx.Codec,
		false, /* reverse */
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		false, /* isCheck */
		&cb.alloc,
		cb.mon,
		tableArgs,
	)
}

// InitForLocalUse initializes a ColumnBackfiller for use during local
// execution within a transaction. In this case, the entire backfill process
// is occurring on the gateway as part of the user's transaction.
func (cb *ColumnBackfiller) InitForLocalUse(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	desc *tabledesc.Immutable,
	mon *mon.BytesMonitor,
) error {
	cb.initCols(desc)
	defaultExprs, err := schemaexpr.MakeDefaultExprs(
		ctx, cb.added, &transform.ExprTransformContext{}, evalCtx, semaCtx,
	)
	if err != nil {
		return err
	}
	computedExprs, err := schemaexpr.MakeComputedExprs(
		ctx,
		cb.added,
		desc,
		tree.NewUnqualifiedTableName(tree.Name(desc.Name)),
		evalCtx,
		semaCtx,
	)
	if err != nil {
		return err
	}
	return cb.init(evalCtx, defaultExprs, computedExprs, desc, mon)
}

// InitForDistributedUse initializes a ColumnBackfiller for use as part of a
// backfill operation executing as part of a distributed flow. In this use,
// the backfill operation manages its own transactions. This separation is
// necessary due to the different procedure for accessing user defined type
// metadata as part of a distributed flow.
func (cb *ColumnBackfiller) InitForDistributedUse(
	ctx context.Context, flowCtx *execinfra.FlowCtx, desc *tabledesc.Immutable, mon *mon.BytesMonitor,
) error {
	cb.initCols(desc)
	evalCtx := flowCtx.NewEvalCtx()
	var defaultExprs, computedExprs []tree.TypedExpr
	// Install type metadata in the target descriptors, as well as resolve any
	// user defined types in the column expressions.
	if err := flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		resolver := flowCtx.TypeResolverFactory.NewTypeResolver(txn)
		// Hydrate all the types present in the table.
		if err := typedesc.HydrateTypesInTableDescriptor(ctx, desc.TableDesc(), resolver); err != nil {
			return err
		}
		// Set up a SemaContext to type check the default and computed expressions.
		semaCtx := tree.MakeSemaContext()
		semaCtx.TypeResolver = resolver
		var err error
		defaultExprs, err = schemaexpr.MakeDefaultExprs(
			ctx, cb.added, &transform.ExprTransformContext{}, evalCtx, &semaCtx,
		)
		if err != nil {
			return err
		}
		computedExprs, err = schemaexpr.MakeComputedExprs(
			ctx,
			cb.added,
			desc,
			tree.NewUnqualifiedTableName(tree.Name(desc.Name)),
			evalCtx,
			&semaCtx,
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

	return cb.init(evalCtx, defaultExprs, computedExprs, desc, mon)
}

// Close frees the resources used by the ColumnBackfiller.
func (cb *ColumnBackfiller) Close(ctx context.Context) {
	cb.fetcher.Close(ctx)
	if cb.mon != nil {
		cb.mon.Stop(ctx)
	}
}

// RunColumnBackfillChunk runs column backfill over a chunk of the table using
// the span sp provided, for all updateCols.
func (cb *ColumnBackfiller) RunColumnBackfillChunk(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *tabledesc.Immutable,
	sp roachpb.Span,
	chunkSize int64,
	alsoCommit bool,
	traceKV bool,
) (roachpb.Key, error) {
	// TODO(dan): Tighten up the bound on the requestedCols parameter to
	// makeRowUpdater.
	requestedCols := make([]descpb.ColumnDescriptor, 0, len(tableDesc.Columns)+len(cb.added)+len(cb.dropped))
	requestedCols = append(requestedCols, tableDesc.Columns...)
	requestedCols = append(requestedCols, cb.added...)
	requestedCols = append(requestedCols, cb.dropped...)
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
	iv := &schemaexpr.RowIndexedVarContainer{
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
				return roachpb.Key{}, sqlerrors.NewInvalidSchemaDefinitionError(err)
			}
			if j < len(cb.added) && !cb.added[j].Nullable && val == tree.DNull {
				return roachpb.Key{}, sqlerrors.NewNonNullViolationError(cb.added[j].Name)
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
		// No existing secondary indexes will be updated by adding or dropping a
		// column. It is safe to use an empty PartialIndexUpdateHelper in this
		// case.
		var pm row.PartialIndexUpdateHelper
		if _, err := ru.UpdateRow(
			ctx, b, oldValues, updateValues, pm, traceKV,
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
func ConvertBackfillError(ctx context.Context, tableDesc *tabledesc.Immutable, b *kv.Batch) error {
	// A backfill on a new schema element has failed and the batch contains
	// information useful in printing a sensible error. However
	// ConvertBatchError() will only work correctly if the schema elements
	// are "live" in the tableDesc.
	desc, err := tableDesc.MakeFirstMutationPublic(tabledesc.IncludeConstraints)
	if err != nil {
		return err
	}
	return row.ConvertBatchError(ctx, tabledesc.NewImmutable(*desc.TableDesc()), b)
}

type muBoundAccount struct {
	// mu protects the boundAccount which may be updated asynchronously during
	// ingestion and index creation.
	syncutil.Mutex
	// boundAccount is associated with mon and is used to track allocations during
	// an	index backfill.
	boundAccount mon.BoundAccount
}

// IndexBackfiller is capable of backfilling all the added index.
type IndexBackfiller struct {
	backfiller

	added []*descpb.IndexDescriptor
	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	colIdxMap catalog.TableColMap

	types   []*types.T
	rowVals tree.Datums
	evalCtx *tree.EvalContext
	cols    []descpb.ColumnDescriptor

	// predicates is a map of indexes to partial index predicate expressions. It
	// includes entries for partial indexes only.
	predicates map[descpb.IndexID]tree.TypedExpr
	// indexesToEncode is a list of indexes to encode entries for a given row.
	// It is a field of IndexBackfiller to avoid allocating a slice for each row
	// backfilled.
	indexesToEncode []*descpb.IndexDescriptor

	// mon is a memory monitor linked with the IndexBackfiller on creation.
	mon            *mon.BytesMonitor
	muBoundAccount muBoundAccount
}

// ContainsInvertedIndex returns true if backfilling an inverted index.
func (ib *IndexBackfiller) ContainsInvertedIndex() bool {
	for _, idx := range ib.added {
		if idx.Type == descpb.IndexDescriptor_INVERTED {
			return true
		}
	}
	return false
}

// InitForLocalUse initializes an IndexBackfiller for use during local execution
// within a transaction. In this case, the entire backfill process is occurring
// on the gateway as part of the user's transaction.
func (ib *IndexBackfiller) InitForLocalUse(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	desc *tabledesc.Immutable,
	mon *mon.BytesMonitor,
) error {
	// Initialize ib.cols and ib.colIdxMap.
	ib.initCols(desc)

	// Initialize ib.added.
	valNeededForCol := ib.initIndexes(desc)

	// Convert any partial index predicate strings into expressions.
	predicates, predicateRefColIDs, err := schemaexpr.MakePartialIndexExprs(
		ctx,
		ib.added,
		ib.cols,
		desc,
		evalCtx,
		semaCtx,
	)
	if err != nil {
		return err
	}

	// Add the columns referenced in the predicate to valNeededForCol so that
	// columns necessary to evaluate the predicate expression are fetched.
	predicateRefColIDs.ForEach(func(col descpb.ColumnID) {
		valNeededForCol.Add(ib.colIdxMap.GetDefault(col))
	})

	return ib.init(evalCtx, predicates, valNeededForCol, desc, mon)
}

// InitForDistributedUse initializes an IndexBackfiller for use as part of a
// backfill operation executing as part of a distributed flow. In this use, the
// backfill operation manages its own transactions. This separation is necessary
// due to the different procedure for accessing user defined type metadata as
// part of a distributed flow.
func (ib *IndexBackfiller) InitForDistributedUse(
	ctx context.Context, flowCtx *execinfra.FlowCtx, desc *tabledesc.Immutable, mon *mon.BytesMonitor,
) error {
	// Initialize ib.cols and ib.colIdxMap.
	ib.initCols(desc)

	// Initialize ib.added.
	valNeededForCol := ib.initIndexes(desc)

	evalCtx := flowCtx.NewEvalCtx()
	var predicates map[descpb.IndexID]tree.TypedExpr
	var predicateRefColIDs catalog.TableColSet

	// Install type metadata in the target descriptors, as well as resolve any
	// user defined types in partial index predicate expressions.
	if err := flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		resolver := flowCtx.TypeResolverFactory.NewTypeResolver(txn)
		// Hydrate all the types present in the table.
		if err := typedesc.HydrateTypesInTableDescriptor(ctx, desc.TableDesc(), resolver); err != nil {
			return err
		}
		// Set up a SemaContext to type check the default and computed expressions.
		semaCtx := tree.MakeSemaContext()
		semaCtx.TypeResolver = resolver

		// Convert any partial index predicate strings into expressions.
		var err error
		predicates, predicateRefColIDs, err =
			schemaexpr.MakePartialIndexExprs(ctx, ib.added, ib.cols, desc, evalCtx, &semaCtx)
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

	// Add the columns referenced in the predicate to valNeededForCol so that
	// columns necessary to evaluate the predicate expression are fetched.
	predicateRefColIDs.ForEach(func(col descpb.ColumnID) {
		valNeededForCol.Add(ib.colIdxMap.GetDefault(col))
	})

	return ib.init(evalCtx, predicates, valNeededForCol, desc, mon)
}

// Close releases the resources used by the IndexBackfiller.
func (ib *IndexBackfiller) Close(ctx context.Context) {
	ib.fetcher.Close(ctx)
	if ib.mon != nil {
		ib.muBoundAccount.Lock()
		ib.muBoundAccount.boundAccount.Close(ctx)
		ib.muBoundAccount.Unlock()
		ib.mon.Stop(ctx)
	}
}

// GrowBoundAccount grows the mutex protected bound account backing the
// index backfiller.
func (ib *IndexBackfiller) GrowBoundAccount(ctx context.Context, growBy int64) error {
	defer ib.muBoundAccount.Unlock()
	ib.muBoundAccount.Lock()
	err := ib.muBoundAccount.boundAccount.Grow(ctx, growBy)
	return err
}

// ShrinkBoundAccount shrinks the mutex protected bound account backing the
// index backfiller.
func (ib *IndexBackfiller) ShrinkBoundAccount(ctx context.Context, shrinkBy int64) {
	defer ib.muBoundAccount.Unlock()
	ib.muBoundAccount.Lock()
	ib.muBoundAccount.boundAccount.Shrink(ctx, shrinkBy)
}

// initCols is a helper to populate column metadata of an IndexBackfiller. It
// populates the cols and colIdxMap fields.
func (ib *IndexBackfiller) initCols(desc *tabledesc.Immutable) {
	ib.cols = desc.Columns

	// If there are ongoing mutations, add columns that are being added and in
	// the DELETE_AND_WRITE_ONLY state.
	if len(desc.Mutations) > 0 {
		ib.cols = make([]descpb.ColumnDescriptor, 0, len(desc.Columns)+len(desc.Mutations))
		ib.cols = append(ib.cols, desc.Columns...)
		for _, m := range desc.Mutations {
			if column := m.GetColumn(); column != nil &&
				m.Direction == descpb.DescriptorMutation_ADD &&
				m.State == descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY {
				ib.cols = append(ib.cols, *column)
			}
		}
	}

	// Create a map of each column's ID to its ordinal.
	for i := range ib.cols {
		ib.colIdxMap.Set(ib.cols[i].ID, i)
	}
}

// initIndexes is a helper to populate index metadata of an IndexBackfiller. It
// populates the added field. It returns a set of column ordinals that must be
// fetched in order to backfill the added indexes.
func (ib *IndexBackfiller) initIndexes(desc *tabledesc.Immutable) util.FastIntSet {
	var valNeededForCol util.FastIntSet
	mutationID := desc.Mutations[0].MutationID

	// Mutations in the same transaction have the same ID. Loop through the
	// mutations and collect all index mutations.
	for _, m := range desc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		if IndexMutationFilter(m) {
			idx := m.GetIndex()
			ib.added = append(ib.added, idx)
			for i := range ib.cols {
				id := ib.cols[i].ID
				if idx.ContainsColumnID(id) ||
					idx.GetEncodingType(desc.GetPrimaryIndexID()) == descpb.PrimaryIndexEncoding {
					valNeededForCol.Add(i)
				}
			}
		}
	}

	return valNeededForCol
}

// init completes the initialization of an IndexBackfiller.
func (ib *IndexBackfiller) init(
	evalCtx *tree.EvalContext,
	predicateExprs map[descpb.IndexID]tree.TypedExpr,
	valNeededForCol util.FastIntSet,
	desc *tabledesc.Immutable,
	mon *mon.BytesMonitor,
) error {
	ib.evalCtx = evalCtx
	ib.predicates = predicateExprs

	// Initialize a list of index descriptors to encode entries for. If there
	// are no partial indexes, the list is equivalent to the list of indexes
	// being added. If there are partial indexes, allocate a new list that is
	// reset in BuildIndexEntriesChunk for every row added.
	ib.indexesToEncode = ib.added
	if len(ib.predicates) > 0 {
		ib.indexesToEncode = make([]*descpb.IndexDescriptor, 0, len(ib.added))
	}

	ib.types = make([]*types.T, len(ib.cols))
	for i := range ib.cols {
		ib.types[i] = ib.cols[i].Type
	}

	tableArgs := row.FetcherTableArgs{
		Desc:            desc,
		Index:           desc.GetPrimaryIndex().IndexDesc(),
		ColIdxMap:       ib.colIdxMap,
		Cols:            ib.cols,
		ValNeededForCol: valNeededForCol,
	}

	// Create a bound account associated with the index backfiller monitor.
	if mon == nil {
		return errors.AssertionFailedf("no memory monitor linked to IndexBackfiller during init")
	}
	ib.mon = mon
	ib.muBoundAccount.boundAccount = mon.MakeBoundAccount()

	return ib.fetcher.Init(
		evalCtx.Context,
		evalCtx.Codec,
		false, /* reverse */
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		false, /* isCheck */
		&ib.alloc,
		mon,
		tableArgs,
	)
}

// BuildIndexEntriesChunk reads a chunk of rows from a table using the span sp
// provided, and builds all the added indexes.
// The method accounts for the memory used by the index entries for this chunk
// using the memory monitor associated with ib and returns the amount of memory
// that needs to be freed once the returned IndexEntry slice is freed.
// It is the callers responsibility to clear the associated bound account when
// appropriate.
func (ib *IndexBackfiller) BuildIndexEntriesChunk(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *tabledesc.Immutable,
	sp roachpb.Span,
	chunkSize int64,
	traceKV bool,
) ([]rowenc.IndexEntry, roachpb.Key, int64, error) {
	// This ought to be chunkSize but in most tests we are actually building smaller
	// indexes so use a smaller value.
	const initBufferSize = 1000
	const sizeOfIndexEntry = int64(unsafe.Sizeof(rowenc.IndexEntry{}))
	var memUsedPerChunk int64

	indexEntriesInChunkInitialBufferSize :=
		sizeOfIndexEntry * initBufferSize * int64(len(ib.added))
	if err := ib.GrowBoundAccount(ctx, indexEntriesInChunkInitialBufferSize); err != nil {
		return nil, nil, 0, errors.Wrap(err,
			"failed to initialize empty buffer to store the index entries of all rows in the chunk")
	}
	memUsedPerChunk += indexEntriesInChunkInitialBufferSize
	entries := make([]rowenc.IndexEntry, 0, initBufferSize*int64(len(ib.added)))

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
		return nil, nil, 0, err
	}

	iv := &schemaexpr.RowIndexedVarContainer{
		Cols:    ib.cols,
		Mapping: ib.colIdxMap,
	}
	ib.evalCtx.IVarContainer = iv

	indexEntriesPerRowInitialBufferSize := int64(len(ib.added)) * sizeOfIndexEntry
	if err := ib.GrowBoundAccount(ctx, indexEntriesPerRowInitialBufferSize); err != nil {
		return nil, nil, 0, errors.Wrap(err,
			"failed to initialize empty buffer to store the index entries of a single row")
	}
	memUsedPerChunk += indexEntriesPerRowInitialBufferSize
	buffer := make([]rowenc.IndexEntry, len(ib.added))
	for i := int64(0); i < chunkSize; i++ {
		encRow, _, _, err := ib.fetcher.NextRow(ctx)
		if err != nil {
			return nil, nil, 0, err
		}
		if encRow == nil {
			break
		}
		if len(ib.rowVals) == 0 {
			ib.rowVals = make(tree.Datums, len(encRow))
		}
		if err := rowenc.EncDatumRowToDatums(ib.types, ib.rowVals, encRow, &ib.alloc); err != nil {
			return nil, nil, 0, err
		}

		iv.CurSourceRow = ib.rowVals

		// If there are any partial indexes being added, make a list of the
		// indexes that the current row should be added to.
		if len(ib.predicates) > 0 {
			ib.indexesToEncode = ib.indexesToEncode[:0]
			for _, idx := range ib.added {
				if !idx.IsPartial() {
					// If the index is not a partial index, all rows should have
					// an entry.
					ib.indexesToEncode = append(ib.indexesToEncode, idx)
					continue
				}

				// If the index is a partial index, only include it if the
				// predicate expression evaluates to true.
				texpr := ib.predicates[idx.ID]

				val, err := texpr.Eval(ib.evalCtx)
				if err != nil {
					return nil, nil, 0, err
				}

				if val == tree.DBoolTrue {
					ib.indexesToEncode = append(ib.indexesToEncode, idx)
				}
			}
		}

		// We're resetting the length of this slice for variable length indexes such as inverted
		// indexes which can append entries to the end of the slice. If we don't do this, then everything
		// EncodeSecondaryIndexes appends to secondaryIndexEntries for a row, would stay in the slice for
		// subsequent rows and we would then have duplicates in entries on output. Additionally, we do
		// not want to include empty k/v pairs while backfilling.
		buffer = buffer[:0]
		// We lock the bound account for the duration of this method as it could
		// attempt to Grow() it while encoding secondary indexes.
		var memUsedDuringEncoding int64
		ib.muBoundAccount.Lock()
		if buffer, memUsedDuringEncoding, err = rowenc.EncodeSecondaryIndexes(
			ctx,
			ib.evalCtx.Codec,
			tableDesc,
			ib.indexesToEncode,
			ib.colIdxMap,
			ib.rowVals,
			buffer,
			false, /* includeEmpty */
			&ib.muBoundAccount.boundAccount,
		); err != nil {
			ib.muBoundAccount.Unlock()
			return nil, nil, 0, err
		}
		ib.muBoundAccount.Unlock()
		memUsedPerChunk += memUsedDuringEncoding

		// The memory monitor has already accounted for cap(entries). If the number
		// of index entries are going to cause the entries buffer to re-slice, then
		// it will very likely double in capacity. Therefore, we must account for
		// another cap(entries) in the index memory account.
		if cap(entries)-len(entries) < len(buffer) {
			resliceSize := sizeOfIndexEntry * int64(cap(entries))
			if err := ib.GrowBoundAccount(ctx, resliceSize); err != nil {
				return nil, nil, 0, err
			}
			memUsedPerChunk += resliceSize
		}

		entries = append(entries, buffer...)
	}

	// We can release the memory which was allocated for `buffer` since all its
	// contents have been copied to `entries`.
	shrinkSize := sizeOfIndexEntry * int64(cap(buffer))
	ib.ShrinkBoundAccount(ctx, shrinkSize)
	memUsedPerChunk -= shrinkSize

	return entries, ib.fetcher.Key(), memUsedPerChunk, nil
}

// RunIndexBackfillChunk runs an index backfill over a chunk of the table
// by traversing the span sp provided. The backfill is run for the added
// indexes.
func (ib *IndexBackfiller) RunIndexBackfillChunk(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *tabledesc.Immutable,
	sp roachpb.Span,
	chunkSize int64,
	alsoCommit bool,
	traceKV bool,
) (roachpb.Key, error) {
	entries, key, memUsedBuildingChunk, err := ib.BuildIndexEntriesChunk(ctx, txn, tableDesc, sp,
		chunkSize, traceKV)
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

	// After the chunk entries have been written, we must clear the bound account
	// tracking the memory usage for the chunk.
	entries = nil
	ib.ShrinkBoundAccount(ctx, memUsedBuildingChunk)

	return key, nil
}
