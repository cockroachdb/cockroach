// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// The Column and Index backfill primitives.

package backfill

import (
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// IndexBackfillCheckpointInterval is the duration between backfill detail updates.
//
// Note: it might be surprising to find this defined here given this layer does
// not actually perform any checkpointing. The reason it has been moved here from
// sql is to avoid any dependency cycles inside the declarative schema changer.
var IndexBackfillCheckpointInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"bulkio.index_backfill.checkpoint_interval",
	"the amount of time between index backfill checkpoint updates",
	30*time.Second,
	settings.NonNegativeDuration,
)

// MutationFilter is the type of a simple predicate on a mutation.
type MutationFilter func(catalog.Mutation) bool

// ColumnMutationFilter is a filter that allows mutations that add or drop
// columns.
func ColumnMutationFilter(m catalog.Mutation) bool {
	return m.AsColumn() != nil && (m.Adding() || m.Dropped())
}

// IndexMutationFilter is a filter that allows mutations that add indexes.
func IndexMutationFilter(m catalog.Mutation) bool {
	idx := m.AsIndex()
	return idx != nil && !idx.IsTemporaryIndexForBackfill() && m.Adding()
}

// ColumnBackfiller is capable of running a column backfill for all
// updateCols.
type ColumnBackfiller struct {
	added   []catalog.Column
	dropped []catalog.Column

	// updateCols is a slice of all column descriptors that are being modified.
	updateCols  []catalog.Column
	updateExprs []tree.TypedExpr
	evalCtx     *eval.Context

	fetcher     row.Fetcher
	fetcherCols []descpb.ColumnID
	colIdxMap   catalog.TableColMap
	alloc       tree.DatumAlloc

	// mon is a memory monitor linked with the ColumnBackfiller on creation.
	mon *mon.BytesMonitor

	rowMetrics *rowinfra.Metrics
}

// initCols is a helper to populate some column metadata on a ColumnBackfiller.
func (cb *ColumnBackfiller) initCols(desc catalog.TableDescriptor) {
	for _, m := range desc.AllMutations() {
		if ColumnMutationFilter(m) {
			col := m.AsColumn()
			if m.Adding() {
				cb.added = append(cb.added, col)
			} else if m.Dropped() {
				cb.dropped = append(cb.dropped, col)
			}
		}
	}
}

// init performs initialization operations that are shared across the local
// and distributed initialization procedures for the ColumnBackfiller.
//
// txn might be nil, in which case it will need to be set on the fetcher later.
func (cb *ColumnBackfiller) init(
	ctx context.Context,
	txn *kv.Txn,
	evalCtx *eval.Context,
	defaultExprs []tree.TypedExpr,
	computedExprs []tree.TypedExpr,
	desc catalog.TableDescriptor,
	mon *mon.BytesMonitor,
	rowMetrics *rowinfra.Metrics,
	traceKV bool,
) error {
	cb.evalCtx = evalCtx
	cb.updateCols = append(cb.added, cb.dropped...)
	// Populate default or computed values.
	cb.updateExprs = make([]tree.TypedExpr, len(cb.updateCols))
	for j, col := range cb.added {
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

	// We need all the non-virtual columns and any primary key virtual columns.
	// Note that hash-sharded primary indexes use a virtual column in their
	// primary key.
	keyColumns := desc.GetPrimaryIndex().CollectKeyColumnIDs()
	for _, c := range desc.PublicColumns() {
		if !c.IsVirtual() || keyColumns.Contains(c.GetID()) {
			cb.fetcherCols = append(cb.fetcherCols, c.GetID())
		}
	}

	cb.colIdxMap = catalog.ColumnIDToOrdinalMap(desc.PublicColumns())
	var spec fetchpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(&spec, evalCtx.Codec, desc, desc.GetPrimaryIndex(), cb.fetcherCols); err != nil {
		return err
	}

	// Create a bound account associated with the column backfiller.
	if mon == nil {
		return errors.AssertionFailedf("no memory monitor linked to ColumnBackfiller during init")
	}
	cb.mon = mon
	cb.rowMetrics = rowMetrics

	return cb.fetcher.Init(
		ctx,
		row.FetcherInitArgs{
			Txn:                        txn,
			Alloc:                      &cb.alloc,
			MemMonitor:                 cb.mon,
			Spec:                       &spec,
			TraceKV:                    traceKV,
			ForceProductionKVBatchSize: cb.evalCtx.TestingKnobs.ForceProductionValues,
		},
	)
}

// InitForLocalUse initializes a ColumnBackfiller for use during local
// execution within a transaction. In this case, the entire backfill process
// is occurring on the gateway as part of the user's transaction.
func (cb *ColumnBackfiller) InitForLocalUse(
	ctx context.Context,
	txn *kv.Txn,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	desc catalog.TableDescriptor,
	mon *mon.BytesMonitor,
	rowMetrics *rowinfra.Metrics,
	traceKV bool,
) error {
	cb.initCols(desc)
	defaultExprs, err := schemaexpr.MakeDefaultExprs(
		ctx, cb.added, &transform.ExprTransformContext{}, evalCtx, semaCtx,
	)
	if err != nil {
		return err
	}
	computedExprs, _, err := schemaexpr.MakeComputedExprs(
		ctx,
		cb.added,
		desc.PublicColumns(),
		desc,
		tree.NewUnqualifiedTableName(tree.Name(desc.GetName())),
		evalCtx,
		semaCtx,
	)
	if err != nil {
		return err
	}
	return cb.init(ctx, txn, evalCtx, defaultExprs, computedExprs, desc, mon, rowMetrics, traceKV)
}

// InitForDistributedUse initializes a ColumnBackfiller for use as part of a
// backfill operation executing as part of a distributed flow. In this use,
// the backfill operation manages its own transactions. This separation is
// necessary due to the different procedure for accessing user defined type
// metadata as part of a distributed flow.
func (cb *ColumnBackfiller) InitForDistributedUse(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	desc catalog.TableDescriptor,
	mon *mon.BytesMonitor,
) error {
	cb.initCols(desc)
	evalCtx := flowCtx.NewEvalCtx()
	var defaultExprs, computedExprs []tree.TypedExpr
	// Install type metadata in the target descriptors, as well as resolve any
	// user defined types in the column expressions.
	if err := flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		resolver := flowCtx.NewTypeResolver(txn.KV())
		// Hydrate all the types present in the table.
		if err := typedesc.HydrateTypesInDescriptor(ctx, desc, &resolver); err != nil {
			return err
		}
		// Set up a SemaContext to type check the default and computed expressions.
		semaCtx := tree.MakeSemaContext()
		semaCtx.TypeResolver = &resolver
		semaCtx.UnsupportedTypeChecker = eval.NewUnsupportedTypeChecker(evalCtx.Settings.Version)
		var err error
		defaultExprs, err = schemaexpr.MakeDefaultExprs(
			ctx, cb.added, &transform.ExprTransformContext{}, evalCtx, &semaCtx,
		)
		if err != nil {
			return err
		}
		computedExprs, _, err = schemaexpr.MakeComputedExprs(
			ctx,
			cb.added,
			desc.PublicColumns(),
			desc,
			tree.NewUnqualifiedTableName(tree.Name(desc.GetName())),
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
	flowCtx.Descriptors.ReleaseAll(ctx)

	rowMetrics := flowCtx.GetRowMetrics()
	// The txn will be set on the fetcher in RunColumnBackfillChunk.
	return cb.init(ctx, nil /* txn */, evalCtx, defaultExprs, computedExprs, desc, mon, rowMetrics, flowCtx.TraceKV)
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
	tableDesc catalog.TableDescriptor,
	sp roachpb.Span,
	chunkSize rowinfra.RowLimit,
	updateChunkSizeThresholdBytes rowinfra.BytesLimit,
	alsoCommit bool,
	traceKV bool,
) (roachpb.Key, error) {
	// TODO(dan): Tighten up the bound on the requestedCols parameter to
	// makeRowUpdater.
	requestedCols := make([]catalog.Column, 0, len(tableDesc.PublicColumns())+len(cb.added)+len(cb.dropped))
	requestedCols = append(requestedCols, tableDesc.PublicColumns()...)
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
		&cb.evalCtx.Settings.SV,
		cb.evalCtx.SessionData().Internal,
		cb.rowMetrics,
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

	// Update the fetcher to use the new txn.
	if err := cb.fetcher.SetTxn(txn); err != nil {
		log.Errorf(ctx, "scan error during SetTxn: %s", err)
		return roachpb.Key{}, err
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
		ctx, []roachpb.Span{sp}, nil, /* spanIDs */
		rowinfra.GetDefaultBatchBytesLimit(cb.evalCtx.TestingKnobs.ForceProductionValues),
		chunkSize,
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return roachpb.Key{}, err
	}

	updateValues := make(tree.Datums, len(cb.updateExprs))
	b := txn.NewBatch()
	iv := &schemaexpr.RowIndexedVarContainer{
		Cols:    make([]catalog.Column, 0, len(tableDesc.PublicColumns())+len(cb.added)),
		Mapping: ru.FetchColIDtoRowIndex,
	}
	iv.Cols = append(iv.Cols, tableDesc.PublicColumns()...)
	iv.Cols = append(iv.Cols, cb.added...)
	cb.evalCtx.IVarContainer = iv

	fetchedValues := make(tree.Datums, cb.colIdxMap.Len())
	iv.CurSourceRow = make(tree.Datums, len(iv.Cols))
	// We can have more FetchCols than public columns; fill the rest with NULLs.
	oldValues := make(tree.Datums, len(ru.FetchCols))
	for i := range oldValues {
		oldValues[i] = tree.DNull
	}

	for i := int64(0); i < int64(chunkSize); i++ {
		ok, err := cb.fetcher.NextRowDecodedInto(ctx, fetchedValues, cb.colIdxMap)
		if err != nil {
			return roachpb.Key{}, err
		}
		if !ok {
			break
		}

		iv.CurSourceRow = append(iv.CurSourceRow[:0], fetchedValues...)

		// Evaluate the new values. This must be done separately for
		// each row so as to handle impure functions correctly.
		for j, e := range cb.updateExprs {
			val, err := eval.Expr(ctx, cb.evalCtx, e)
			if err != nil {
				if errors.Is(err, eval.ErrNilTxnInClusterContext) {
					// Cannot use expressions that depend on the transaction of the
					// evaluation context as the default value for backfill.
					return roachpb.Key{}, pgerror.WithCandidateCode(err, pgcode.FeatureNotSupported)
				}
				return roachpb.Key{}, sqlerrors.NewInvalidSchemaDefinitionError(err)
			}
			if j < len(cb.added) && !cb.added[j].IsNullable() && val == tree.DNull {
				return roachpb.Key{}, sqlerrors.NewNonNullViolationError(cb.added[j].GetName())
			}

			// Added computed column values should be usable for the next
			// added columns being backfilled. They have already been type
			// checked.
			if j < len(cb.added) {
				iv.CurSourceRow = append(iv.CurSourceRow, val)
			}
			updateValues[j] = val
		}
		copy(oldValues, fetchedValues)

		// No existing secondary indexes will be updated by adding or dropping a
		// column. It is safe to use an empty PartialIndexUpdateHelper in this
		// case.
		var pm row.PartialIndexUpdateHelper
		if _, err := ru.UpdateRow(
			ctx, b, oldValues, updateValues, pm, traceKV,
		); err != nil {
			return roachpb.Key{}, err
		}

		// Exit early to flush if the batch byte size exceeds a predefined
		// threshold. This can happen when table rows are more on the "fat" side,
		// typically with large BYTES or JSONB columns.
		//
		// This helps prevent exceedingly large raft commands which will
		// for instance cause schema changes to be unable to either proceed or to
		// roll back.
		//
		// The threshold is ignored when zero.
		//
		if updateChunkSizeThresholdBytes > 0 && b.ApproximateMutationBytes() >= int(updateChunkSizeThresholdBytes) {
			break
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
	ctx context.Context, tableDesc catalog.TableDescriptor, b *kv.Batch,
) error {
	// A backfill on a new schema element has failed and the batch contains
	// information useful in printing a sensible error. However
	// ConvertBatchError() will only work correctly if the schema elements
	// are "live" in the tableDesc.
	desc, err := tableDesc.MakeFirstMutationPublic()
	if err != nil {
		return err
	}
	return row.ConvertBatchError(ctx, desc, b)
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
	indexBackfillerCols

	added []catalog.Index

	types   []*types.T
	rowVals tree.Datums
	evalCtx *eval.Context

	// Map of columns which need to be evaluated to their expressions.
	colExprs map[descpb.ColumnID]tree.TypedExpr

	// predicates is a map of indexes to partial index predicate expressions. It
	// includes entries for partial indexes only.
	predicates map[descpb.IndexID]tree.TypedExpr

	// indexesToEncode is a list of indexes to encode entries for a given row.
	// It is a field of IndexBackfiller to avoid allocating a slice for each row
	// backfilled.
	indexesToEncode []catalog.Index

	alloc tree.DatumAlloc

	// mon is a memory monitor linked with the IndexBackfiller on creation.
	mon            *mon.BytesMonitor
	muBoundAccount muBoundAccount
}

// ContainsInvertedIndex returns true if backfilling an inverted index.
func (ib *IndexBackfiller) ContainsInvertedIndex() bool {
	for _, idx := range ib.added {
		if idx.GetType() == descpb.IndexDescriptor_INVERTED {
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
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	desc catalog.TableDescriptor,
	mon *mon.BytesMonitor,
) error {

	// Initialize ib.added.
	ib.initIndexes(desc, nil /* allowList */)

	// Initialize ib.cols and ib.colIdxMap.
	if err := ib.initCols(desc); err != nil {
		return err
	}

	predicates, colExprs, referencedColumns, err := constructExprs(
		ctx, desc, ib.added, ib.cols, ib.addedCols, ib.computedCols, evalCtx, semaCtx,
	)
	if err != nil {
		return err
	}

	// Add the columns referenced in the predicate to valNeededForCol so that
	// columns necessary to evaluate the predicate expression are fetched.
	referencedColumns.ForEach(func(col descpb.ColumnID) {
		ib.valNeededForCol.Add(ib.colIdxMap.GetDefault(col))
	})

	return ib.init(evalCtx, predicates, colExprs, mon)
}

// constructExprs is a helper to construct the index and column expressions
// required for an index backfill. It also returns the set of non-virtual
// columns referenced by any of these exprs that should be fetched from the
// primary index. Virtual columns are not included because they don't exist in
// the primary index.
//
// The cols argument is the full set of cols in the table (including those being
// added). The addedCols argument is the set of non-public, non-computed
// columns. The computedCols argument is the set of computed columns in the
// index.
func constructExprs(
	ctx context.Context,
	desc catalog.TableDescriptor,
	addedIndexes []catalog.Index,
	cols, addedCols, computedCols []catalog.Column,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
) (
	predicates map[descpb.IndexID]tree.TypedExpr,
	colExprs map[descpb.ColumnID]tree.TypedExpr,
	referencedColumns catalog.TableColSet,
	_ error,
) {
	// Convert any partial index predicate strings into expressions.
	predicates, predicateRefColIDs, err := schemaexpr.MakePartialIndexExprs(
		ctx,
		addedIndexes,
		cols,
		desc,
		evalCtx,
		semaCtx,
	)
	if err != nil {
		return nil, nil, catalog.TableColSet{}, err
	}

	// Determine the exprs for newly added, non-computed columns.
	defaultExprs, err := schemaexpr.MakeDefaultExprs(
		ctx, addedCols, &transform.ExprTransformContext{}, evalCtx, semaCtx,
	)
	if err != nil {
		return nil, nil, catalog.TableColSet{}, err
	}

	// TODO(ajwerner): Rethink this table name.
	tn := tree.NewUnqualifiedTableName(tree.Name(desc.GetName()))
	computedExprs, computedExprRefColIDs, err := schemaexpr.MakeComputedExprs(
		ctx,
		computedCols,
		cols,
		desc,
		tn,
		evalCtx,
		semaCtx,
	)
	if err != nil {
		return nil, nil, catalog.TableColSet{}, err
	}

	numColExprs := len(addedCols) + len(computedCols)
	colExprs = make(map[descpb.ColumnID]tree.TypedExpr, numColExprs)
	var addedColSet catalog.TableColSet
	for i := range defaultExprs {
		id := addedCols[i].GetID()
		colExprs[id] = defaultExprs[i]
		addedColSet.Add(id)
	}
	for i := range computedCols {
		id := computedCols[i].GetID()
		colExprs[id] = computedExprs[i]
	}

	// Ensure that only existing, non-virtual columns are added to the needed
	// set. Otherwise the fetcher may complain that the columns don't exist.
	// There's a somewhat subtle invariant that if any dependencies exist
	// between computed columns and default values that the computed column be a
	// later column and thus the default value will have been populated.
	// Computed columns are not permitted to reference each other.
	addToReferencedColumns := func(cols catalog.TableColSet) error {
		for colID, ok := cols.Next(0); ok; colID, ok = cols.Next(colID + 1) {
			if addedColSet.Contains(colID) {
				continue
			}
			col, err := catalog.MustFindColumnByID(desc, colID)
			if err != nil {
				return errors.HandleAsAssertionFailure(err)
			}
			if col.IsVirtual() {
				continue
			}
			referencedColumns.Add(colID)
		}
		return nil
	}
	if err := addToReferencedColumns(predicateRefColIDs); err != nil {
		return nil, nil, catalog.TableColSet{}, err
	}
	if err := addToReferencedColumns(computedExprRefColIDs); err != nil {
		return nil, nil, catalog.TableColSet{}, err
	}
	return predicates, colExprs, referencedColumns, nil
}

// InitForDistributedUse initializes an IndexBackfiller for use as part of a
// backfill operation executing as part of a distributed flow. In this use, the
// backfill operation manages its own transactions. This separation is necessary
// due to the different procedure for accessing user defined type metadata as
// part of a distributed flow.
func (ib *IndexBackfiller) InitForDistributedUse(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	desc catalog.TableDescriptor,
	allowList []catid.IndexID,
	mon *mon.BytesMonitor,
) error {

	// Initialize ib.added.
	ib.initIndexes(desc, allowList)

	// Initialize ib.indexBackfillerCols.
	if err := ib.initCols(desc); err != nil {
		return err
	}

	evalCtx := flowCtx.NewEvalCtx()
	var predicates map[descpb.IndexID]tree.TypedExpr
	var colExprs map[descpb.ColumnID]tree.TypedExpr
	var referencedColumns catalog.TableColSet

	// Install type metadata in the target descriptors, as well as resolve any
	// user defined types in partial index predicate expressions.
	if err := flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		resolver := flowCtx.NewTypeResolver(txn.KV())
		// Hydrate all the types present in the table.
		if err = typedesc.HydrateTypesInDescriptor(ctx, desc, &resolver); err != nil {
			return err
		}
		// Set up a SemaContext to type check the default and computed expressions.
		semaCtx := tree.MakeSemaContext()
		semaCtx.TypeResolver = &resolver
		semaCtx.UnsupportedTypeChecker = eval.NewUnsupportedTypeChecker(evalCtx.Settings.Version)
		// Convert any partial index predicate strings into expressions.
		predicates, colExprs, referencedColumns, err = constructExprs(
			ctx, desc, ib.added, ib.cols, ib.addedCols, ib.computedCols, evalCtx, &semaCtx,
		)
		return err
	}); err != nil {
		return err
	}
	// Release leases on any accessed types now that type metadata is installed.
	// We do this so that leases on any accessed types are not held for the
	// entire backfill process.
	flowCtx.Descriptors.ReleaseAll(ctx)

	// Add the columns referenced in the predicate to valNeededForCol so that
	// columns necessary to evaluate the predicate expression are fetched.
	referencedColumns.ForEach(func(col descpb.ColumnID) {
		ib.valNeededForCol.Add(ib.colIdxMap.GetDefault(col))
	})

	return ib.init(evalCtx, predicates, colExprs, mon)
}

// Close releases the resources used by the IndexBackfiller.
func (ib *IndexBackfiller) Close(ctx context.Context) {
	if ib.mon != nil {
		func() {
			ib.muBoundAccount.Lock()
			defer ib.muBoundAccount.Unlock()
			ib.muBoundAccount.boundAccount.Close(ctx)
		}()
		ib.mon.Stop(ctx)
	}
}

// GrowBoundAccount grows the mutex protected bound account backing the
// index backfiller.
func (ib *IndexBackfiller) GrowBoundAccount(ctx context.Context, growBy int64) error {
	ib.muBoundAccount.Lock()
	defer ib.muBoundAccount.Unlock()
	return ib.muBoundAccount.boundAccount.Grow(ctx, growBy)
}

// ShrinkBoundAccount shrinks the mutex protected bound account backing the
// index backfiller.
func (ib *IndexBackfiller) ShrinkBoundAccount(ctx context.Context, shrinkBy int64) {
	ib.muBoundAccount.Lock()
	defer ib.muBoundAccount.Unlock()
	ib.muBoundAccount.boundAccount.Shrink(ctx, shrinkBy)
}

// initCols is a helper to populate column metadata of an IndexBackfiller. It
// populates the cols and colIdxMap fields.
func (ib *IndexBackfiller) initCols(desc catalog.TableDescriptor) (err error) {
	ib.indexBackfillerCols, err = makeIndexBackfillColumns(
		desc.DeletableColumns(), desc.GetPrimaryIndex(), ib.added,
	)
	return err
}

// initIndexes is a helper to populate index metadata of an IndexBackfiller. It
// populates the added field to be all adding index mutations.
// If `allowList` is non-nil, we only add those in this list.
// If `allowList` is nil, we add all adding index mutations.
func (ib *IndexBackfiller) initIndexes(desc catalog.TableDescriptor, allowList []catid.IndexID) {
	var allowListAsSet catid.IndexSet
	if len(allowList) > 0 {
		allowListAsSet = catid.MakeIndexIDSet(allowList...)
	}

	mutations := desc.AllMutations()
	mutationID := mutations[0].MutationID()
	// Mutations in the same transaction have the same ID. Loop through the
	// mutations and collect all index mutations.
	for _, m := range mutations {
		if m.MutationID() != mutationID {
			break
		}
		if IndexMutationFilter(m) &&
			(allowListAsSet.Empty() || allowListAsSet.Contains(m.AsIndex().GetID())) {
			idx := m.AsIndex()
			ib.added = append(ib.added, idx)
		}
	}
}

// init completes the initialization of an IndexBackfiller.
func (ib *IndexBackfiller) init(
	evalCtx *eval.Context,
	predicateExprs map[descpb.IndexID]tree.TypedExpr,
	colExprs map[descpb.ColumnID]tree.TypedExpr,
	mon *mon.BytesMonitor,
) error {
	ib.evalCtx = evalCtx
	ib.predicates = predicateExprs
	ib.colExprs = colExprs

	// Initialize a list of index descriptors to encode entries for. If there
	// are no partial indexes, the list is equivalent to the list of indexes
	// being added. If there are partial indexes, allocate a new list that is
	// reset in BuildIndexEntriesChunk for every row added.
	ib.indexesToEncode = ib.added
	if len(ib.predicates) > 0 {
		ib.indexesToEncode = make([]catalog.Index, 0, len(ib.added))
	}

	ib.types = make([]*types.T, len(ib.cols))
	for i := range ib.cols {
		ib.types[i] = ib.cols[i].GetType()
	}

	// Create a bound account associated with the index backfiller monitor.
	if mon == nil {
		return errors.AssertionFailedf("no memory monitor linked to IndexBackfiller during init")
	}
	ib.mon = mon
	ib.muBoundAccount.boundAccount = mon.MakeBoundAccount()
	return nil
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
	tableDesc catalog.TableDescriptor,
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

	var fetcherCols []descpb.ColumnID
	for i, c := range ib.cols {
		if ib.valNeededForCol.Contains(i) {
			fetcherCols = append(fetcherCols, c.GetID())
		}
	}
	if ib.rowVals == nil {
		ib.rowVals = make(tree.Datums, len(ib.cols))
		// We don't produce values for all columns, so initialize with NULLs.
		for i := range ib.rowVals {
			ib.rowVals[i] = tree.DNull
		}
	}
	// Get the next set of rows.
	//
	// Running the scan and applying the changes in many transactions
	// is fine because the schema change is in the correct state to
	// handle intermediate OLTP commands which delete and add values
	// during the scan. Index entries in the new index are being
	// populated and deleted by the OLTP commands but not otherwise
	// read or used
	var spec fetchpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(
		&spec, ib.evalCtx.Codec, tableDesc, tableDesc.GetPrimaryIndex(), fetcherCols,
	); err != nil {
		return nil, nil, 0, err
	}
	var fetcher row.Fetcher
	if err := fetcher.Init(
		ctx,
		row.FetcherInitArgs{
			Txn:                        txn,
			Alloc:                      &ib.alloc,
			MemMonitor:                 ib.mon,
			Spec:                       &spec,
			TraceKV:                    traceKV,
			ForceProductionKVBatchSize: ib.evalCtx.TestingKnobs.ForceProductionValues,
		},
	); err != nil {
		return nil, nil, 0, err
	}
	defer fetcher.Close(ctx)
	if err := fetcher.StartScan(
		ctx, []roachpb.Span{sp}, nil, /* spanIDs */
		rowinfra.GetDefaultBatchBytesLimit(ib.evalCtx.TestingKnobs.ForceProductionValues),
		initBufferSize,
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
	evaluateExprs := func(cols []catalog.Column) error {
		for i := range cols {
			colID := cols[i].GetID()
			texpr, ok := ib.colExprs[colID]
			if !ok {
				continue
			}
			val, err := eval.Expr(ctx, ib.evalCtx, texpr)
			if err != nil {
				if errors.Is(err, eval.ErrNilTxnInClusterContext) {
					// Cannot use expressions that depend on the transaction of the
					// evaluation context as the default value for backfill.
					err = pgerror.WithCandidateCode(err, pgcode.FeatureNotSupported)
				}
				// Explicitly mark with user errors for codes that we know
				// cannot be retried.
				if code := pgerror.GetPGCode(err); code == pgcode.FeatureNotSupported ||
					code == pgcode.InvalidParameterValue {
					return scerrors.SchemaChangerUserError(err)
				}
				return err
			}
			colIdx, ok := ib.colIdxMap.Get(colID)
			if !ok {
				return errors.AssertionFailedf(
					"failed to find index for column %d in %d",
					colID, tableDesc.GetID(),
				)
			}
			// Note that if this is a computed expr which is not being added,
			// then this should generally be an assertion failure.
			if val == tree.DNull && !cols[i].IsNullable() {
				return sqlerrors.NewNonNullViolationError(cols[i].GetName())
			}
			ib.rowVals[colIdx] = val
		}
		return nil
	}
	for i := int64(0); i < chunkSize; i++ {
		ok, err := fetcher.NextRowDecodedInto(ctx, ib.rowVals, ib.colIdxMap)
		if err != nil {
			return nil, nil, 0, err
		}
		if !ok {
			break
		}
		iv.CurSourceRow = ib.rowVals

		// First populate default values, then populate computed expressions which
		// may reference default values.
		if len(ib.colExprs) > 0 {
			if err := evaluateExprs(ib.addedCols); err != nil {
				return nil, nil, 0, err
			}
			if err := evaluateExprs(ib.computedCols); err != nil {
				return nil, nil, 0, err
			}
		}

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
				texpr := ib.predicates[idx.GetID()]

				val, err := eval.Expr(ctx, ib.evalCtx, texpr)
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
		buffer, memUsedDuringEncoding, err = func(buffer []rowenc.IndexEntry) ([]rowenc.IndexEntry, int64, error) {
			ib.muBoundAccount.Lock()
			defer ib.muBoundAccount.Unlock()
			return rowenc.EncodeSecondaryIndexes(
				ctx,
				ib.evalCtx.Codec,
				tableDesc,
				ib.indexesToEncode,
				ib.colIdxMap,
				ib.rowVals,
				buffer,
				false, /* includeEmpty */
				&ib.muBoundAccount.boundAccount,
			)
		}(buffer)
		if err != nil {
			return nil, nil, 0, err
		}
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

	var resumeKey roachpb.Key
	if fetcher.Key() != nil {
		resumeKey = make(roachpb.Key, len(fetcher.Key()))
		copy(resumeKey, fetcher.Key())
	}
	return entries, resumeKey, memUsedPerChunk, nil
}

// RunIndexBackfillChunk runs an index backfill over a chunk of the table
// by traversing the span sp provided. The backfill is run for the added
// indexes.
func (ib *IndexBackfiller) RunIndexBackfillChunk(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc catalog.TableDescriptor,
	sp roachpb.Span,
	chunkSize int64,
	alsoCommit bool,
	traceKV bool,
) (roachpb.Key, error) {
	entries, key, memUsedBuildingChunk, err := ib.BuildIndexEntriesChunk(
		ctx, txn, tableDesc, sp, chunkSize, traceKV,
	)
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
