// Copyright 2015 The Cockroach Authors.
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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// TODO(vivek): Replace these constants with a runtime budget for the
	// operation chunk involved.

	// columnTruncateAndBackfillChunkSize is the maximum number of columns
	// processed per chunk during column truncate or backfill.
	columnTruncateAndBackfillChunkSize = 200

	// indexTruncateChunkSize is the maximum number of index entries truncated
	// per chunk during an index truncation. This value is larger than the
	// other chunk constants because the operation involves only running a
	// DeleteRange().
	indexTruncateChunkSize = row.TableTruncateChunkSize

	// indexTxnBackfillChunkSize is the maximum number index entries backfilled
	// per chunk during an index backfill done in a txn. The index backfill
	// involves a table scan, and a number of individual ops presented in a batch.
	// This value is smaller than ColumnTruncateAndBackfillChunkSize, because it
	// involves a number of individual index row updates that can be scattered
	// over many ranges.
	indexTxnBackfillChunkSize = 100

	// indexBackfillBatchSize is the maximum number of index entries we attempt to
	// fill in a single index batch before queueing it up for ingestion and
	// progress reporting in the index backfiller processor.
	//
	// TODO(adityamaru): This should live with the index backfiller processor
	// logic once the column backfiller is reworked. The only reason this variable
	// is initialized here is to maintain a single testing knob
	// `BackfillChunkSize` to control both the index and column backfill chunking
	// behavior, and minimize test complexity. Should this be a cluster setting? I
	// would hope we can do a dynamic memory based adjustment of this number in
	// the processor.
	indexBackfillBatchSize = 5000

	// checkpointInterval is the interval after which a checkpoint of the
	// schema change is posted.
	checkpointInterval = 2 * time.Minute
)

var _ sort.Interface = columnsByID{}
var _ sort.Interface = indexesByID{}

type columnsByID []descpb.ColumnDescriptor

func (cds columnsByID) Len() int {
	return len(cds)
}
func (cds columnsByID) Less(i, j int) bool {
	return cds[i].ID < cds[j].ID
}
func (cds columnsByID) Swap(i, j int) {
	cds[i], cds[j] = cds[j], cds[i]
}

type indexesByID []descpb.IndexDescriptor

func (ids indexesByID) Len() int {
	return len(ids)
}
func (ids indexesByID) Less(i, j int) bool {
	return ids[i].ID < ids[j].ID
}
func (ids indexesByID) Swap(i, j int) {
	ids[i], ids[j] = ids[j], ids[i]
}

func (sc *SchemaChanger) getChunkSize(chunkSize int64) int64 {
	if sc.testingKnobs.BackfillChunkSize > 0 {
		return sc.testingKnobs.BackfillChunkSize
	}
	return chunkSize
}

// scTxnFn is the type of functions that operates using transactions in the backfiller.
type scTxnFn func(ctx context.Context, txn *kv.Txn, evalCtx *extendedEvalContext) error

// historicalTxnRunner is the type of the callback used by the various
// helper functions to run checks at a fixed timestamp (logically, at
// the start of the backfill).
type historicalTxnRunner func(ctx context.Context, fn scTxnFn) error

// makeFixedTimestampRunner creates a historicalTxnRunner suitable for use by the helpers.
func (sc *SchemaChanger) makeFixedTimestampRunner(readAsOf hlc.Timestamp) historicalTxnRunner {
	runner := func(ctx context.Context, retryable scTxnFn) error {
		return sc.fixedTimestampTxn(ctx, readAsOf, func(ctx context.Context, txn *kv.Txn) error {
			// We need to re-create the evalCtx since the txn may retry.
			evalCtx := createSchemaChangeEvalCtx(ctx, sc.execCfg, readAsOf, sc.ieFactory)
			return retryable(ctx, txn, &evalCtx)
		})
	}
	return runner
}

func (sc *SchemaChanger) fixedTimestampTxn(
	ctx context.Context,
	readAsOf hlc.Timestamp,
	retryable func(ctx context.Context, txn *kv.Txn) error,
) error {
	return sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetFixedTimestamp(ctx, readAsOf)
		return retryable(ctx, txn)
	})
}

// runBackfill runs the backfill for the schema changer.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely. The various
// function that it calls make their own txns.
func (sc *SchemaChanger) runBackfill(ctx context.Context) error {
	if sc.testingKnobs.RunBeforeBackfill != nil {
		if err := sc.testingKnobs.RunBeforeBackfill(); err != nil {
			return err
		}
	}

	// Mutations are applied in a FIFO order. Only apply the first set of
	// mutations. Collect the elements that are part of the mutation.
	var droppedIndexDescs []descpb.IndexDescriptor
	var addedIndexSpans []roachpb.Span

	var constraintsToDrop []descpb.ConstraintToUpdate
	var constraintsToAddBeforeValidation []descpb.ConstraintToUpdate
	var constraintsToValidate []descpb.ConstraintToUpdate

	var viewToRefresh *descpb.MaterializedViewRefresh

	// Note that this descriptor is intentionally not leased. If the schema change
	// held the lease, certain non-mutation related schema changes would not be
	// able to proceed. That might be okay and even desirable. The bigger reason
	// to not hold a lease throughout the duration of this schema change stage
	// is more practical. The lease manager (and associated descriptor
	// infrastructure) does not provide a mechanism to hold a lease over a long
	// period of time and update the transaction commit deadline. As such, when
	// the schema change job attempts to mutate the descriptor later in this
	// method, the descriptor will need to be re-read and the operation should be
	// revalidated against the new state of the descriptor. Any work to hold
	// leases during mutations will need to consider the user experience when the
	// user would like to issue schema changes to be applied asynchronously.
	// Perhaps such schema changes could avoid waiting for a single version and
	// thus avoid blocked. This will get ironed out in the context of
	// transactional schema changes. In all likelihood, not holding a lease here
	// is the right thing to do as we would never want this operation to fail
	// because a new mutation was enqueued.
	tableDesc, err := sc.updateJobRunningStatus(ctx, RunningStatusBackfill)
	if err != nil {
		return err
	}

	// Short circuit the backfill if the table has been deleted.
	if tableDesc.Dropped() {
		return nil
	}
	version := tableDesc.Version

	log.Infof(ctx, "running backfill for %q, v=%d", tableDesc.Name, tableDesc.Version)

	needColumnBackfill := false
	for _, m := range tableDesc.Mutations {
		if m.MutationID != sc.mutationID {
			break
		}
		switch m.Direction {
		case descpb.DescriptorMutation_ADD:
			switch t := m.Descriptor_.(type) {
			case *descpb.DescriptorMutation_Column:
				if tabledesc.ColumnNeedsBackfill(m.GetColumn()) {
					needColumnBackfill = true
				}
			case *descpb.DescriptorMutation_Index:
				addedIndexSpans = append(addedIndexSpans, tableDesc.IndexSpan(sc.execCfg.Codec, t.Index.ID))
			case *descpb.DescriptorMutation_Constraint:
				switch t.Constraint.ConstraintType {
				case descpb.ConstraintToUpdate_CHECK:
					if t.Constraint.Check.Validity == descpb.ConstraintValidity_Validating {
						constraintsToAddBeforeValidation = append(constraintsToAddBeforeValidation, *t.Constraint)
						constraintsToValidate = append(constraintsToValidate, *t.Constraint)
					}
				case descpb.ConstraintToUpdate_FOREIGN_KEY:
					if t.Constraint.ForeignKey.Validity == descpb.ConstraintValidity_Validating {
						constraintsToAddBeforeValidation = append(constraintsToAddBeforeValidation, *t.Constraint)
						constraintsToValidate = append(constraintsToValidate, *t.Constraint)
					}
				case descpb.ConstraintToUpdate_NOT_NULL:
					// NOT NULL constraints are always validated before they can be added
					constraintsToAddBeforeValidation = append(constraintsToAddBeforeValidation, *t.Constraint)
					constraintsToValidate = append(constraintsToValidate, *t.Constraint)
				}
			case *descpb.DescriptorMutation_PrimaryKeySwap, *descpb.DescriptorMutation_ComputedColumnSwap:
				// The backfiller doesn't need to do anything here.
			case *descpb.DescriptorMutation_MaterializedViewRefresh:
				viewToRefresh = t.MaterializedViewRefresh
			default:
				return errors.AssertionFailedf(
					"unsupported mutation: %+v", m)
			}

		case descpb.DescriptorMutation_DROP:
			switch t := m.Descriptor_.(type) {
			case *descpb.DescriptorMutation_Column:
				needColumnBackfill = true
			case *descpb.DescriptorMutation_Index:
				if !canClearRangeForDrop(t.Index) {
					droppedIndexDescs = append(droppedIndexDescs, *t.Index)
				}
			case *descpb.DescriptorMutation_Constraint:
				constraintsToDrop = append(constraintsToDrop, *t.Constraint)
			case *descpb.DescriptorMutation_PrimaryKeySwap,
				*descpb.DescriptorMutation_ComputedColumnSwap,
				*descpb.DescriptorMutation_MaterializedViewRefresh:
				// The backfiller doesn't need to do anything here.
			default:
				return errors.AssertionFailedf(
					"unsupported mutation: %+v", m)
			}
		}
	}

	// If we were requested to refresh a view, then do so.
	if viewToRefresh != nil {
		if err := sc.refreshMaterializedView(ctx, tableDesc, viewToRefresh); err != nil {
			return err
		}
	}

	// First drop constraints and indexes, then add/drop columns, and only then
	// add indexes and constraints.

	// Drop constraints.
	if len(constraintsToDrop) > 0 {
		descs, err := sc.dropConstraints(ctx, constraintsToDrop)
		if err != nil {
			return err
		}
		version = descs[tableDesc.ID].Version
	}

	// Drop indexes not to be removed by `ClearRange`.
	if len(droppedIndexDescs) > 0 {
		if err := sc.truncateIndexes(ctx, version, droppedIndexDescs); err != nil {
			return err
		}
	}

	// Add and drop columns.
	if needColumnBackfill {
		if err := sc.truncateAndBackfillColumns(ctx, version); err != nil {
			return err
		}
	}

	// Add new indexes.
	if len(addedIndexSpans) > 0 {
		// Check if bulk-adding is enabled and supported by indexes (ie non-unique).
		if err := sc.backfillIndexes(ctx, version, addedIndexSpans); err != nil {
			return err
		}
	}

	// Add check and foreign key constraints, publish the new version of the table descriptor,
	// and wait until the entire cluster is on the new version. This is basically
	// a state transition for the schema change, which must happen after the
	// columns are backfilled and before constraint validation begins. This
	// ensures that 1) all columns are writable and backfilled when the constraint
	// starts being enforced on insert/update (which is relevant in the case where
	// a constraint references both public and non-public columns), and 2) the
	// validation occurs only when the entire cluster is already enforcing the
	// constraint on insert/update.
	if len(constraintsToAddBeforeValidation) > 0 {
		if err := sc.addConstraints(ctx, constraintsToAddBeforeValidation); err != nil {
			return err
		}
	}

	// Validate check and foreign key constraints.
	if len(constraintsToValidate) > 0 {
		if err := sc.validateConstraints(ctx, constraintsToValidate); err != nil {
			return err
		}
	}

	log.Infof(ctx, "completed backfill for %q, v=%d", tableDesc.Name, tableDesc.Version)

	if sc.testingKnobs.RunAfterBackfill != nil {
		if err := sc.testingKnobs.RunAfterBackfill(*sc.job.ID()); err != nil {
			return err
		}
	}

	return nil
}

// dropConstraints publishes a new version of the given table descriptor with
// the given constraint removed from it, and waits until the entire cluster is
// on the new version of the table descriptor. It returns the new table descs.
func (sc *SchemaChanger) dropConstraints(
	ctx context.Context, constraints []descpb.ConstraintToUpdate,
) (map[descpb.ID]*tabledesc.Immutable, error) {
	log.Infof(ctx, "dropping %d constraints", len(constraints))

	fksByBackrefTable := make(map[descpb.ID][]*descpb.ConstraintToUpdate)
	for i := range constraints {
		c := &constraints[i]
		if c.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY &&
			c.ForeignKey.ReferencedTableID != sc.descID {
			fksByBackrefTable[c.ForeignKey.ReferencedTableID] = append(fksByBackrefTable[c.ForeignKey.ReferencedTableID], c)
		}
	}

	// Create update closure for the table and all other tables with backreferences.
	if err := sc.txn(ctx, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		scTable, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
			return err
		}
		b := txn.NewBatch()
		for i := range constraints {
			constraint := &constraints[i]
			switch constraint.ConstraintType {
			case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
				found := false
				for j, c := range scTable.Checks {
					if c.Name == constraint.Name {
						scTable.Checks = append(scTable.Checks[:j], scTable.Checks[j+1:]...)
						found = true
						break
					}
				}
				if !found {
					log.VEventf(
						ctx, 2,
						"backfiller tried to drop constraint %+v but it was not found, "+
							"presumably due to a retry or rollback",
						constraint,
					)
				}
			case descpb.ConstraintToUpdate_FOREIGN_KEY:
				var foundExisting bool
				for j := range scTable.OutboundFKs {
					def := &scTable.OutboundFKs[j]
					if def.Name != constraint.Name {
						continue
					}
					backrefTable, err := descsCol.GetMutableTableVersionByID(ctx,
						constraint.ForeignKey.ReferencedTableID, txn)
					if err != nil {
						return err
					}
					if err := removeFKBackReferenceFromTable(
						backrefTable, def.Name, scTable,
					); err != nil {
						return err
					}
					if err := descsCol.WriteDescToBatch(
						ctx, true /* kvTrace */, backrefTable, b,
					); err != nil {
						return err
					}
					scTable.OutboundFKs = append(scTable.OutboundFKs[:j], scTable.OutboundFKs[j+1:]...)
					foundExisting = true
					break
				}
				if !foundExisting {
					log.VEventf(
						ctx, 2,
						"backfiller tried to drop constraint %+v but it was not found, "+
							"presumably due to a retry or rollback",
						constraint,
					)
				}
			}
		}
		if err := descsCol.WriteDescToBatch(
			ctx, true /* kvTrace */, scTable, b,
		); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	}); err != nil {
		return nil, err
	}

	if err := WaitToUpdateLeases(ctx, sc.leaseMgr, sc.descID); err != nil {
		return nil, err
	}
	for id := range fksByBackrefTable {
		if err := WaitToUpdateLeases(ctx, sc.leaseMgr, id); err != nil {
			return nil, err
		}
	}

	log.Info(ctx, "finished dropping constraints")
	tableDescs := make(map[descpb.ID]*tabledesc.Immutable, len(fksByBackrefTable)+1)
	if err := sc.txn(ctx, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) (err error) {
		if tableDescs[sc.descID], err = descsCol.GetImmutableTableByID(
			ctx, txn, sc.descID, tree.ObjectLookupFlags{},
		); err != nil {
			return err
		}
		for id := range fksByBackrefTable {
			if tableDescs[id], err = descsCol.GetImmutableTableByID(
				ctx, txn, id, tree.ObjectLookupFlags{},
			); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return tableDescs, nil
}

// addConstraints publishes a new version of the given table descriptor with the
// given constraint added to it, and waits until the entire cluster is on
// the new version of the table descriptor.
func (sc *SchemaChanger) addConstraints(
	ctx context.Context, constraints []descpb.ConstraintToUpdate,
) error {
	log.Infof(ctx, "adding %d constraints", len(constraints))

	fksByBackrefTable := make(map[descpb.ID][]*descpb.ConstraintToUpdate)
	for i := range constraints {
		c := &constraints[i]
		if c.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY &&
			c.ForeignKey.ReferencedTableID != sc.descID {
			fksByBackrefTable[c.ForeignKey.ReferencedTableID] = append(fksByBackrefTable[c.ForeignKey.ReferencedTableID], c)
		}
	}

	// Create update closure for the table and all other tables with backreferences
	if err := sc.txn(ctx, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		scTable, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
			return err
		}

		b := txn.NewBatch()
		for i := range constraints {
			constraint := &constraints[i]
			switch constraint.ConstraintType {
			case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
				found := false
				for _, c := range scTable.Checks {
					if c.Name == constraint.Name {
						log.VEventf(
							ctx, 2,
							"backfiller tried to add constraint %+v but found existing constraint %+v, "+
								"presumably due to a retry or rollback",
							constraint, c,
						)
						// Ensure the constraint on the descriptor is set to Validating, in
						// case we're in the middle of rolling back DROP CONSTRAINT
						c.Validity = descpb.ConstraintValidity_Validating
						found = true
						break
					}
				}
				if !found {
					scTable.Checks = append(scTable.Checks, &constraints[i].Check)
				}
			case descpb.ConstraintToUpdate_FOREIGN_KEY:
				var foundExisting bool
				for j := range scTable.OutboundFKs {
					def := &scTable.OutboundFKs[j]
					if def.Name == constraint.Name {
						if log.V(2) {
							log.VEventf(
								ctx, 2,
								"backfiller tried to add constraint %+v but found existing constraint %+v, "+
									"presumably due to a retry or rollback",
								constraint, def,
							)
						}
						// Ensure the constraint on the descriptor is set to Validating, in
						// case we're in the middle of rolling back DROP CONSTRAINT
						def.Validity = descpb.ConstraintValidity_Validating
						foundExisting = true
						break
					}
				}
				if !foundExisting {
					scTable.OutboundFKs = append(scTable.OutboundFKs, constraint.ForeignKey)
					backrefTable, err := descsCol.GetMutableTableVersionByID(ctx, constraint.ForeignKey.ReferencedTableID, txn)
					if err != nil {
						return err
					}
					// Check that a unique constraint for the FK still exists on the
					// referenced table. It's possible for the unique index found during
					// planning to have been dropped in the meantime, since only the
					// presence of the backreference prevents it.
					_, err = tabledesc.FindFKReferencedUniqueConstraint(backrefTable, constraint.ForeignKey.ReferencedColumnIDs)
					if err != nil {
						return err
					}
					backrefTable.InboundFKs = append(backrefTable.InboundFKs, constraint.ForeignKey)

					// Note that this code may add the same descriptor to the batch
					// multiple times if it is referenced multiple times. That's fine as
					// the last put will win but it's perhaps not ideal. We could add
					// code to deduplicate but it doesn't seem worth the hassle.
					if backrefTable != scTable {
						if err := descsCol.WriteDescToBatch(
							ctx, true /* kvTrace */, backrefTable, b,
						); err != nil {
							return err
						}
					}
				}
			}
		}
		if err := descsCol.WriteDescToBatch(
			ctx, true /* kvTrace */, scTable, b,
		); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	}); err != nil {
		return err
	}

	if err := WaitToUpdateLeases(ctx, sc.leaseMgr, sc.descID); err != nil {
		return err
	}
	for id := range fksByBackrefTable {
		if err := WaitToUpdateLeases(ctx, sc.leaseMgr, id); err != nil {
			return err
		}
	}
	log.Info(ctx, "finished adding constraints")
	return nil
}

// validateConstraints checks that the current table data obeys the
// provided constraints.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely, so it makes its own.
func (sc *SchemaChanger) validateConstraints(
	ctx context.Context, constraints []descpb.ConstraintToUpdate,
) error {
	if lease.TestingTableLeasesAreDisabled() {
		return nil
	}
	log.Infof(ctx, "validating %d new constraints", len(constraints))

	_, err := sc.updateJobRunningStatus(ctx, RunningStatusValidation)
	if err != nil {
		return err
	}

	if fn := sc.testingKnobs.RunBeforeConstraintValidation; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}

	readAsOf := sc.clock.Now()
	var tableDesc *tabledesc.Immutable

	if err := sc.fixedTimestampTxn(ctx, readAsOf, func(ctx context.Context, txn *kv.Txn) error {
		tableDesc, err = catalogkv.MustGetTableDescByID(ctx, txn, sc.execCfg.Codec, sc.descID)
		return err
	}); err != nil {
		return err
	}

	grp := ctxgroup.WithContext(ctx)
	// The various checks below operate at a fixed timestamp.
	runHistoricalTxn := sc.makeFixedTimestampRunner(readAsOf)

	for i := range constraints {
		c := constraints[i]
		grp.GoCtx(func(ctx context.Context) error {
			// Make the mutations public in a private copy of the descriptor
			// and add it to the Collection, so that we can use SQL below to perform
			// the validation. We wouldn't have needed to do this if we could have
			// updated the descriptor and run validation in the same transaction. However,
			// our current system is incapable of running long running schema changes
			// (the validation can take many minutes). So we pretend that the schema
			// has been updated and actually update it in a separate transaction that
			// follows this one.
			desc, err := tableDesc.MakeFirstMutationPublic(tabledesc.IgnoreConstraints)
			if err != nil {
				return err
			}
			// Each check operates at the historical timestamp.
			return runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, evalCtx *extendedEvalContext) error {
				// If the constraint is a check constraint that fails validation, we
				// need a semaContext set up that can resolve types in order to pretty
				// print the check expression back to the user.
				evalCtx.Txn = txn
				// Use the DistSQLTypeResolver because we need to resolve types by ID.
				semaCtx := tree.MakeSemaContext()
				collection := descs.NewCollection(sc.settings, sc.leaseMgr, nil /* hydratedTables */)
				semaCtx.TypeResolver = descs.NewDistSQLTypeResolver(collection, txn)
				// TODO (rohany): When to release this? As of now this is only going to get released
				//  after the check is validated.
				defer func() { collection.ReleaseAll(ctx) }()
				switch c.ConstraintType {
				case descpb.ConstraintToUpdate_CHECK:
					if err := validateCheckInTxn(ctx, sc.leaseMgr, &semaCtx, &evalCtx.EvalContext, desc, txn, c.Check.Expr); err != nil {
						return err
					}
				case descpb.ConstraintToUpdate_FOREIGN_KEY:
					if err := validateFkInTxn(ctx, sc.leaseMgr, &evalCtx.EvalContext, desc, txn, c.Name); err != nil {
						return err
					}
				case descpb.ConstraintToUpdate_NOT_NULL:
					if err := validateCheckInTxn(ctx, sc.leaseMgr, &semaCtx, &evalCtx.EvalContext, desc, txn, c.Check.Expr); err != nil {
						// TODO (lucy): This should distinguish between constraint
						// validation errors and other types of unexpected errors, and
						// return a different error code in the former case
						return errors.Wrap(err, "validation of NOT NULL constraint failed")
					}
				default:
					return errors.Errorf("unsupported constraint type: %d", c.ConstraintType)
				}
				return nil
			})
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	log.Info(ctx, "finished validating new constraints")
	return nil
}

// getTableVersion retrieves the descriptor for the table being
// targeted by the schema changer using the provided txn, and asserts
// that the retrieved descriptor is at the given version. An error is
// returned otherwise.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func (sc *SchemaChanger) getTableVersion(
	ctx context.Context, txn *kv.Txn, tc *descs.Collection, version descpb.DescriptorVersion,
) (*tabledesc.Immutable, error) {
	tableDesc, err := tc.GetImmutableTableByID(ctx, txn, sc.descID, tree.ObjectLookupFlags{})
	if err != nil {
		return nil, err
	}
	if version != tableDesc.Version {
		return nil, makeErrTableVersionMismatch(tableDesc.Version, version)
	}
	return tableDesc, nil
}

// TruncateInterleavedIndexes truncates the input set of indexes from the given
// table. It is used in the schema change GC job to delete interleaved index
// data as part of a TRUNCATE statement. Note that we cannot use
// SchemaChanger.truncateIndexes instead because that accesses the most recent
// version of the table when deleting. In this case, we need to use the version
// of the table before truncation, which is passed in.
func TruncateInterleavedIndexes(
	ctx context.Context,
	execCfg *ExecutorConfig,
	table *tabledesc.Immutable,
	indexes []descpb.IndexDescriptor,
) error {
	log.Infof(ctx, "truncating %d interleaved indexes", len(indexes))
	chunkSize := int64(indexTruncateChunkSize)
	alloc := &rowenc.DatumAlloc{}
	codec, db := execCfg.Codec, execCfg.DB
	for _, desc := range indexes {
		var resume roachpb.Span
		for rowIdx, done := int64(0), false; !done; rowIdx += chunkSize {
			log.VEventf(ctx, 2, "truncate interleaved index (%d) at row: %d, span: %s", table.ID, rowIdx, resume)
			resumeAt := resume
			// Make a new txn just to drop this chunk.
			if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				rd := row.MakeDeleter(codec, table, nil /* requestedCols */)
				td := tableDeleter{rd: rd, alloc: alloc}
				if err := td.init(ctx, txn, nil /* *tree.EvalContext */); err != nil {
					return err
				}
				resume, err := td.deleteIndex(
					ctx,
					&desc,
					resumeAt,
					chunkSize,
					false, /* traceKV */
				)
				done = resume.Key == nil
				return err
			}); err != nil {
				return err
			}
		}
		// All the data chunks have been removed. Now also removed the
		// zone configs for the dropped indexes, if any.
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return RemoveIndexZoneConfigs(ctx, txn, execCfg, table.ParentID, indexes)
		}); err != nil {
			return err
		}
	}
	log.Infof(ctx, "finished truncating interleaved indexes")
	return nil
}

// truncateIndexes truncate the KV ranges corresponding to dropped indexes.
//
// The indexes are dropped chunk by chunk, each chunk being deleted in
// its own txn.
func (sc *SchemaChanger) truncateIndexes(
	ctx context.Context, version descpb.DescriptorVersion, dropped []descpb.IndexDescriptor,
) error {
	log.Infof(ctx, "clearing data for %d indexes", len(dropped))

	chunkSize := sc.getChunkSize(indexTruncateChunkSize)
	if sc.testingKnobs.BackfillChunkSize > 0 {
		chunkSize = sc.testingKnobs.BackfillChunkSize
	}
	alloc := &rowenc.DatumAlloc{}
	for _, desc := range dropped {
		var resume roachpb.Span
		for rowIdx, done := int64(0), false; !done; rowIdx += chunkSize {
			resumeAt := resume
			if log.V(2) {
				log.Infof(ctx, "drop index (%d, %d) at row: %d, span: %s",
					sc.descID, sc.mutationID, rowIdx, resume)
			}

			// Make a new txn just to drop this chunk.
			if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				if fn := sc.execCfg.DistSQLRunTestingKnobs.RunBeforeBackfillChunk; fn != nil {
					if err := fn(resume); err != nil {
						return err
					}
				}
				if fn := sc.execCfg.DistSQLRunTestingKnobs.RunAfterBackfillChunk; fn != nil {
					defer fn()
				}

				// Retrieve a lease for this table inside the current txn.
				tc := descs.NewCollection(sc.settings, sc.leaseMgr, nil /* hydratedTables */)
				defer tc.ReleaseAll(ctx)
				tableDesc, err := sc.getTableVersion(ctx, txn, tc, version)
				if err != nil {
					return err
				}
				rd := row.MakeDeleter(sc.execCfg.Codec, tableDesc, nil /* requestedCols */)
				td := tableDeleter{rd: rd, alloc: alloc}
				if err := td.init(ctx, txn, nil /* *tree.EvalContext */); err != nil {
					return err
				}
				if !canClearRangeForDrop(&desc) {
					resume, err = td.deleteIndex(
						ctx,
						&desc,
						resumeAt,
						chunkSize,
						false, /* traceKV */
					)
					done = resume.Key == nil
					return err
				}
				done = true
				return td.clearIndex(ctx, &desc)
			}); err != nil {
				return err
			}
		}

		// All the data chunks have been removed. Now also removed the
		// zone configs for the dropped indexes, if any.
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return RemoveIndexZoneConfigs(ctx, txn, sc.execCfg, sc.descID, dropped)
		}); err != nil {
			return err
		}
	}
	log.Info(ctx, "finished clearing data for indexes")
	return nil
}

type backfillType int

const (
	_ backfillType = iota
	columnBackfill
	indexBackfill
)

// getJobIDForMutationWithDescriptor returns a job id associated with a mutation given
// a table descriptor. Unlike getJobIDForMutation this doesn't need transaction.
// TODO (lucy): This is not a good way to look up all schema change jobs
// associated with a table. We should get rid of MutationJobs and start looking
// up the jobs in the jobs table instead.
func getJobIDForMutationWithDescriptor(
	ctx context.Context, tableDesc catalog.TableDescriptor, mutationID descpb.MutationID,
) (int64, error) {
	for _, job := range tableDesc.GetMutationJobs() {
		if job.MutationID == mutationID {
			return job.JobID, nil
		}
	}

	return 0, errors.AssertionFailedf(
		"job not found for table id %d, mutation %d", tableDesc.GetID(), mutationID)
}

// nRanges returns the number of ranges that cover a set of spans.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func (sc *SchemaChanger) nRanges(
	ctx context.Context, txn *kv.Txn, spans []roachpb.Span,
) (int, error) {
	spanResolver := sc.distSQLPlanner.spanResolver.NewSpanResolverIterator(txn)
	rangeIds := make(map[int64]struct{})
	for _, span := range spans {
		// For each span, iterate the spanResolver until it's exhausted, storing
		// the found range ids in the map to de-duplicate them.
		spanResolver.Seek(ctx, span, kvcoord.Ascending)
		for {
			if !spanResolver.Valid() {
				return 0, spanResolver.Error()
			}
			rangeIds[int64(spanResolver.Desc().RangeID)] = struct{}{}
			if !spanResolver.NeedAnother() {
				break
			}
			spanResolver.Next(ctx)
		}
	}

	return len(rangeIds), nil
}

// TODO(adityamaru): Consider moving this to sql/backfill. It has a lot of
// schema changer dependencies which will need to be passed around.
func (sc *SchemaChanger) distIndexBackfill(
	ctx context.Context,
	version descpb.DescriptorVersion,
	targetSpans []roachpb.Span,
	filter backfill.MutationFilter,
	indexBackfillBatchSize int64,
) error {
	readAsOf := sc.clock.Now()

	// Variables to track progress of the index backfill.
	origNRanges := -1
	origFractionCompleted := sc.job.FractionCompleted()
	fractionLeft := 1 - origFractionCompleted

	// Index backfilling ingests SSTs that don't play nicely with running txns
	// since they just add their keys blindly. Running a Scan of the target
	// spans at the time the SSTs' keys will be written will calcify history up
	// to then since the scan will resolve intents and populate tscache to keep
	// anything else from sneaking under us. Since these are new indexes, these
	// spans should be essentially empty, so this should be a pretty quick and
	// cheap scan.
	const pageSize = 10000
	noop := func(_ []kv.KeyValue) error { return nil }
	if err := sc.fixedTimestampTxn(ctx, readAsOf, func(ctx context.Context, txn *kv.Txn) error {
		for _, span := range targetSpans {
			// TODO(dt): a Count() request would be nice here if the target isn't
			// empty, since we don't need to drag all the results back just to
			// then ignore them -- we just need the iteration on the far end.
			if err := txn.Iterate(ctx, span.Key, span.EndKey, pageSize, noop); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Gather the initial resume spans for the table.
	var todoSpans []roachpb.Span
	var mutationIdx int
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		todoSpans, _, mutationIdx, err = rowexec.GetResumeSpans(
			ctx, sc.jobRegistry, txn, sc.execCfg.Codec, sc.descID, sc.mutationID, filter)
		return err
	}); err != nil {
		return err
	}

	log.VEventf(ctx, 2, "indexbackfill: initial resume spans %+v", todoSpans)

	if todoSpans == nil {
		return nil
	}

	var p *PhysicalPlan
	var evalCtx extendedEvalContext
	var planCtx *PlanningCtx
	// The txn is used to fetch a tableDesc, partition the spans and set the
	// evalCtx ts all of which is during planning of the DistSQL flow.
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		tc := descs.NewCollection(sc.settings, sc.leaseMgr, nil /* hydratedTables */)
		// It is okay to release the lease on the descriptor before running the
		// index backfill flow because any schema change that would invalidate the
		// index being backfilled, would be queued behind the backfill in the
		// mutations slice.
		// NB: There are tradeoffs to holding the lease throughout the backfill. It
		// results in disallowing certain kinds of schema changes to complete eg:
		// changing privileges. There might be a more principled solution in
		// dropping and acquiring fresh leases at regular checkpoint but it is not
		// clear what this buys us in terms of checking the descriptors validity.
		// Thus, in favor of simpler code and no correctness concerns we release
		// the lease once the flow is planned.
		defer tc.ReleaseAll(ctx)
		tableDesc, err := sc.getTableVersion(ctx, txn, tc, version)
		if err != nil {
			return err
		}
		evalCtx = createSchemaChangeEvalCtx(ctx, sc.execCfg, txn.ReadTimestamp(), sc.ieFactory)
		planCtx = sc.distSQLPlanner.NewPlanningCtx(ctx, &evalCtx, nil /* planner */, txn,
			true /* distribute */)
		chunkSize := sc.getChunkSize(indexBackfillBatchSize)
		spec, err := initIndexBackfillerSpec(*tableDesc.TableDesc(), readAsOf, chunkSize)
		if err != nil {
			return err
		}
		p, err = sc.distSQLPlanner.createBackfillerPhysicalPlan(planCtx, spec, todoSpans)
		return err
	}); err != nil {
		return err
	}

	// Processors stream back the completed spans via metadata.
	//
	// mu synchronizes reads and writes to updatedTodoSpans between the processor
	// streaming back progress and the updates to the job details/progress
	// fraction.
	mu := struct {
		syncutil.Mutex
		updatedTodoSpans []roachpb.Span
	}{}
	var updateJobProgress func() error
	var updateJobDetails func() error
	metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			todoSpans = roachpb.SubtractSpans(todoSpans,
				meta.BulkProcessorProgress.CompletedSpans)
			mu.Lock()
			mu.updatedTodoSpans = make([]roachpb.Span, len(todoSpans))
			copy(mu.updatedTodoSpans, todoSpans)
			mu.Unlock()

			if sc.testingKnobs.AlwaysUpdateIndexBackfillDetails {
				if err := updateJobDetails(); err != nil {
					return err
				}
			}

			if sc.testingKnobs.AlwaysUpdateIndexBackfillProgress {
				if err := updateJobProgress(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	cbw := MetadataCallbackWriter{rowResultWriter: &errOnlyResultWriter{}, fn: metaFn}
	recv := MakeDistSQLReceiver(
		ctx,
		&cbw,
		tree.Rows, /* stmtType - doesn't matter here since no result are produced */
		sc.rangeDescriptorCache,
		nil, /* txn - the flow does not run wholly in a txn */
		sc.clock,
		evalCtx.Tracing,
	)
	defer recv.Release()

	updateJobProgress = func() error {
		// Report schema change progress. We define progress at this point as the
		// the fraction of fully-backfilled ranges of the primary index of the
		// table being scanned. Since we may have already modified the fraction
		// completed of our job from the 10% allocated to completing the schema
		// change state machine or from a previous backfill attempt, we scale that
		// fraction of ranges completed by the remaining fraction of the job's
		// progress bar.
		err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			mu.Lock()
			// No processor has returned completed spans yet.
			if mu.updatedTodoSpans == nil {
				mu.Unlock()
				return nil
			}
			nRanges, err := sc.nRanges(ctx, txn, mu.updatedTodoSpans)
			mu.Unlock()
			if err != nil {
				return err
			}
			if origNRanges == -1 {
				origNRanges = nRanges
			}

			if nRanges < origNRanges {
				fractionRangesFinished := float32(origNRanges-nRanges) / float32(origNRanges)
				fractionCompleted := origFractionCompleted + fractionLeft*fractionRangesFinished
				if err := sc.job.WithTxn(txn).FractionProgressed(ctx,
					jobs.FractionUpdater(fractionCompleted)); err != nil {
					return jobs.SimplifyInvalidStatusError(err)
				}
			}
			return nil
		})
		return err
	}

	updateJobDetails = func() error {
		err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			mu.Lock()
			defer mu.Unlock()
			// No processor has returned completed spans yet.
			if mu.updatedTodoSpans == nil {
				return nil
			}
			log.VEventf(ctx, 2, "writing todo spans to job details: %+v", mu.updatedTodoSpans)
			return rowexec.SetResumeSpansInJob(ctx, mu.updatedTodoSpans, mutationIdx, txn, sc.job)
		})
		return err
	}

	// Setup periodic progress update.
	stopProgress := make(chan struct{})
	duration := 10 * time.Second
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(duration)
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-stopProgress:
				return nil
			case <-done:
				return ctx.Err()
			case <-tick.C:
				if err := updateJobProgress(); err != nil {
					return err
				}
			}
		}
	})

	// Setup periodic job details update.
	stopJobDetailsUpdate := make(chan struct{})
	detailsDuration := 10 * time.Second
	g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(detailsDuration)
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-stopJobDetailsUpdate:
				return nil
			case <-done:
				return ctx.Err()
			case <-tick.C:
				if err := updateJobDetails(); err != nil {
					return err
				}
			}
		}
	})

	// Run index backfill physical plan.
	g.GoCtx(func(ctx context.Context) error {
		defer close(stopProgress)
		defer close(stopJobDetailsUpdate)
		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := evalCtx
		sc.distSQLPlanner.Run(
			planCtx,
			nil, /* txn - the processors manage their own transactions */
			p, recv, &evalCtxCopy,
			nil, /* finishedSetupFn */
		)()
		return cbw.Err()
	})

	if err := g.Wait(); err != nil {
		return err
	}

	// Update progress and details to mark a completed job.
	if err := updateJobDetails(); err != nil {
		return err
	}
	if err := updateJobProgress(); err != nil {
		return err
	}

	return nil
}

// distBackfill runs (or continues) a backfill for the first mutation
// enqueued on the SchemaChanger's table descriptor that passes the input
// MutationFilter.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely, so it makes its own.
func (sc *SchemaChanger) distBackfill(
	ctx context.Context,
	version descpb.DescriptorVersion,
	backfillType backfillType,
	backfillChunkSize int64,
	filter backfill.MutationFilter,
	targetSpans []roachpb.Span,
) error {
	duration := checkpointInterval
	if sc.testingKnobs.WriteCheckpointInterval > 0 {
		duration = sc.testingKnobs.WriteCheckpointInterval
	}
	chunkSize := sc.getChunkSize(backfillChunkSize)

	origNRanges := -1
	origFractionCompleted := sc.job.FractionCompleted()
	fractionLeft := 1 - origFractionCompleted
	readAsOf := sc.clock.Now()
	// Gather the initial resume spans for the table.
	var todoSpans []roachpb.Span
	var mutationIdx int
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		todoSpans, _, mutationIdx, err = rowexec.GetResumeSpans(
			ctx, sc.jobRegistry, txn, sc.execCfg.Codec, sc.descID, sc.mutationID, filter)
		return err
	}); err != nil {
		return err
	}

	for len(todoSpans) > 0 {
		log.VEventf(ctx, 2, "backfill: process %+v spans", todoSpans)
		// Make sure not to update todoSpans inside the transaction closure as it
		// may not commit. Instead write the updated value for todoSpans to this
		// variable and assign to todoSpans after committing.
		var updatedTodoSpans []roachpb.Span
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			updatedTodoSpans = todoSpans
			// Report schema change progress. We define progress at this point
			// as the fraction of fully-backfilled ranges of the primary index of
			// the table being scanned. Since we may have already modified the
			// fraction completed of our job from the 10% allocated to completing the
			// schema change state machine or from a previous backfill attempt,
			// we scale that fraction of ranges completed by the remaining fraction
			// of the job's progress bar.
			nRanges, err := sc.nRanges(ctx, txn, todoSpans)
			if err != nil {
				return err
			}
			if origNRanges == -1 {
				origNRanges = nRanges
			}

			if nRanges < origNRanges {
				fractionRangesFinished := float32(origNRanges-nRanges) / float32(origNRanges)
				fractionCompleted := origFractionCompleted + fractionLeft*fractionRangesFinished
				if err := sc.job.FractionProgressed(ctx, jobs.FractionUpdater(fractionCompleted)); err != nil {
					return jobs.SimplifyInvalidStatusError(err)
				}
			}

			tc := descs.NewCollection(sc.settings, sc.leaseMgr, nil /* hydratedTables */)
			// Use a leased table descriptor for the backfill.
			defer tc.ReleaseAll(ctx)
			tableDesc, err := sc.getTableVersion(ctx, txn, tc, version)
			if err != nil {
				return err
			}
			metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
				if meta.BulkProcessorProgress != nil {
					updatedTodoSpans = roachpb.SubtractSpans(updatedTodoSpans,
						meta.BulkProcessorProgress.CompletedSpans)
				}
				return nil
			}
			cbw := MetadataCallbackWriter{rowResultWriter: &errOnlyResultWriter{}, fn: metaFn}
			evalCtx := createSchemaChangeEvalCtx(ctx, sc.execCfg, txn.ReadTimestamp(), sc.ieFactory)
			recv := MakeDistSQLReceiver(
				ctx,
				&cbw,
				tree.Rows, /* stmtType - doesn't matter here since no result are produced */
				sc.rangeDescriptorCache,
				nil, /* txn - the flow does not run wholly in a txn */
				sc.clock,
				evalCtx.Tracing,
			)
			defer recv.Release()

			planCtx := sc.distSQLPlanner.NewPlanningCtx(ctx, &evalCtx, nil /* planner */, txn, true /* distribute */)
			spec, err := initColumnBackfillerSpec(*tableDesc.TableDesc(), duration, chunkSize, readAsOf)
			if err != nil {
				return err
			}
			plan, err := sc.distSQLPlanner.createBackfillerPhysicalPlan(planCtx, spec, todoSpans)
			if err != nil {
				return err
			}
			sc.distSQLPlanner.Run(
				planCtx,
				nil, /* txn - the processors manage their own transactions */
				plan, recv, &evalCtx,
				nil, /* finishedSetupFn */
			)()
			return cbw.Err()
		}); err != nil {
			return err
		}
		todoSpans = updatedTodoSpans

		// Record what is left to do for the job.
		// TODO(spaskob): Execute this at a regular cadence.
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return rowexec.SetResumeSpansInJob(ctx, todoSpans, mutationIdx, txn, sc.job)
		}); err != nil {
			return err
		}
	}
	return nil
}

// updateJobRunningStatus updates the status field in the job entry
// with the given value.
//
// The update is performed in a separate txn at the current logical
// timestamp. Note that while a MutableTableDescriptor is returned, this is an
// odd case in that the descriptor is not tied to the lifetime of a transaction.
// TODO(ajwerner): Fix the transaction and descriptor lifetimes here.
func (sc *SchemaChanger) updateJobRunningStatus(
	ctx context.Context, status jobs.RunningStatus,
) (*tabledesc.Mutable, error) {
	var tableDesc *tabledesc.Mutable
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		desc, err := catalogkv.GetDescriptorByID(ctx, txn, sc.execCfg.Codec, sc.descID, catalogkv.Mutable,
			catalogkv.TableDescriptorKind, true /* required */)
		if err != nil {
			return err
		}
		tableDesc = desc.(*tabledesc.Mutable)

		// Update running status of job.
		updateJobRunningProgress := false
		for _, mutation := range tableDesc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}

			switch mutation.Direction {
			case descpb.DescriptorMutation_ADD:
				switch mutation.State {
				case descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					updateJobRunningProgress = true
				}

			case descpb.DescriptorMutation_DROP:
				switch mutation.State {
				case descpb.DescriptorMutation_DELETE_ONLY:
					updateJobRunningProgress = true
				}
			}
		}
		if updateJobRunningProgress && !tableDesc.Dropped() {
			if err := sc.job.WithTxn(txn).RunningStatus(ctx, func(
				ctx context.Context, details jobspb.Details) (jobs.RunningStatus, error) {
				return status, nil
			}); err != nil {
				return errors.Wrapf(err, "failed to update running status of job %d", errors.Safe(*sc.job.ID()))
			}
		}
		return nil
	})
	return tableDesc, err
}

// validateIndexes checks that the new indexes have entries for all the rows.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely, so it makes its own.
func (sc *SchemaChanger) validateIndexes(ctx context.Context) error {
	if lease.TestingTableLeasesAreDisabled() {
		return nil
	}
	log.Info(ctx, "validating new indexes")

	_, err := sc.updateJobRunningStatus(ctx, RunningStatusValidation)
	if err != nil {
		return err
	}

	if fn := sc.testingKnobs.RunBeforeIndexValidation; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}

	readAsOf := sc.clock.Now()
	var tableDesc *tabledesc.Immutable
	if err := sc.fixedTimestampTxn(ctx, readAsOf, func(ctx context.Context, txn *kv.Txn) (err error) {
		tableDesc, err = catalogkv.MustGetTableDescByID(ctx, txn, sc.execCfg.Codec, sc.descID)
		return err
	}); err != nil {
		return err
	}

	var forwardIndexes []*descpb.IndexDescriptor
	var invertedIndexes []*descpb.IndexDescriptor

	for _, m := range tableDesc.Mutations {
		if sc.mutationID != m.MutationID {
			break
		}
		idx := m.GetIndex()
		if idx == nil || m.Direction == descpb.DescriptorMutation_DROP {
			continue
		}
		switch idx.Type {
		case descpb.IndexDescriptor_FORWARD:
			forwardIndexes = append(forwardIndexes, idx)
		case descpb.IndexDescriptor_INVERTED:
			invertedIndexes = append(invertedIndexes, idx)
		}
	}
	if len(forwardIndexes) == 0 && len(invertedIndexes) == 0 {
		return nil
	}

	grp := ctxgroup.WithContext(ctx)
	runHistoricalTxn := sc.makeFixedTimestampRunner(readAsOf)

	if len(forwardIndexes) > 0 {
		grp.GoCtx(func(ctx context.Context) error {
			return sc.validateForwardIndexes(ctx, tableDesc, forwardIndexes, runHistoricalTxn)
		})
	}
	if len(invertedIndexes) > 0 {
		grp.GoCtx(func(ctx context.Context) error {
			return sc.validateInvertedIndexes(ctx, tableDesc, invertedIndexes, runHistoricalTxn)
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	log.Info(ctx, "finished validating new indexes")
	return nil
}

// validateInvertedIndexes checks that the indexes have entries for
// all the items of data in rows.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely.
// Instead it uses the provided runHistoricalTxn which can operate
// at the historical fixed timestamp for checks.
func (sc *SchemaChanger) validateInvertedIndexes(
	ctx context.Context,
	tableDesc *tabledesc.Immutable,
	indexes []*descpb.IndexDescriptor,
	runHistoricalTxn historicalTxnRunner,
) error {
	grp := ctxgroup.WithContext(ctx)

	expectedCount := make([]int64, len(indexes))
	countReady := make([]chan struct{}, len(indexes))

	for i, idx := range indexes {
		// Shadow i and idx to prevent the values from changing within each
		// gorountine.
		i, idx := i, idx
		countReady[i] = make(chan struct{})

		grp.GoCtx(func(ctx context.Context) error {
			// Inverted indexes currently can't be interleaved, so a KV scan can be
			// used to get the index length.
			// TODO (lucy): Switch to using DistSQL to get the count, so that we get
			// distributed execution and avoid bypassing the SQL decoding
			start := timeutil.Now()
			var idxLen int64
			span := tableDesc.IndexSpan(sc.execCfg.Codec, idx.ID)
			key := span.Key
			endKey := span.EndKey
			if err := runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, _ *extendedEvalContext) error {
				for {
					kvs, err := txn.Scan(ctx, key, endKey, 1000000)
					if err != nil {
						return err
					}
					if len(kvs) == 0 {
						break
					}
					idxLen += int64(len(kvs))
					key = kvs[len(kvs)-1].Key.PrefixEnd()
				}
				return nil
			}); err != nil {
				return err
			}
			log.Infof(ctx, "inverted index %s/%s count = %d, took %s",
				tableDesc.Name, idx.Name, idxLen, timeutil.Since(start))
			select {
			case <-countReady[i]:
				if idxLen != expectedCount[i] {
					// JSON columns cannot have unique indexes, so if the expected and
					// actual counts do not match, it's always a bug rather than a
					// uniqueness violation.
					return errors.AssertionFailedf(
						"validation of index %s failed: expected %d rows, found %d",
						idx.Name, errors.Safe(expectedCount[i]), errors.Safe(idxLen))
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})

		grp.GoCtx(func(ctx context.Context) error {
			defer close(countReady[i])

			start := timeutil.Now()
			col := idx.InvertedColumnName()

			if err := runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, evalCtx *extendedEvalContext) error {
				ie := evalCtx.InternalExecutor.(*InternalExecutor)
				var stmt string
				if geoindex.IsEmptyConfig(&idx.GeoConfig) {
					stmt = fmt.Sprintf(
						`SELECT coalesce(sum_int(crdb_internal.num_inverted_index_entries(%q, %d)), 0) FROM [%d AS t]`,
						col, idx.Version, tableDesc.ID,
					)
				} else {
					stmt = fmt.Sprintf(
						`SELECT coalesce(sum_int(crdb_internal.num_geo_inverted_index_entries(%d, %d, %q)), 0) FROM [%d AS t]`,
						tableDesc.ID, idx.ID, col, tableDesc.ID,
					)
				}
				// If the index is a partial index the predicate must be added
				// as a filter to the query.
				if idx.IsPartial() {
					stmt = fmt.Sprintf(`%s WHERE %s`, stmt, idx.Predicate)
				}
				row, err := ie.QueryRowEx(ctx, "verify-inverted-idx-count", txn,
					sessiondata.InternalExecutorOverride{}, stmt)
				if err != nil {
					return err
				}
				expectedCount[i] = int64(tree.MustBeDInt(row[0]))
				return nil
			}); err != nil {
				return err
			}
			log.Infof(ctx, "column %s/%s expected inverted index count = %d, took %s",
				tableDesc.Name, col, expectedCount[i], timeutil.Since(start))
			return nil
		})
	}

	return grp.Wait()
}

// validateForwardIndexes checks that the indexes have entries for all the rows.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely.
// Instead it uses the provided runHistoricalTxn which can operate
// at the historical fixed timestamp for checks.
func (sc *SchemaChanger) validateForwardIndexes(
	ctx context.Context,
	tableDesc *tabledesc.Immutable,
	indexes []*descpb.IndexDescriptor,
	runHistoricalTxn historicalTxnRunner,
) error {
	grp := ctxgroup.WithContext(ctx)

	var tableRowCount int64
	partialIndexExpectedCounts := make(map[descpb.IndexID]int64, len(indexes))

	// Close when table count is ready.
	tableCountsReady := make(chan struct{})

	// Compute the size of each index.
	for _, idx := range indexes {
		// Shadow idx to prevent its value from changing within each gorountine.
		idx := idx

		grp.GoCtx(func(ctx context.Context) error {
			start := timeutil.Now()
			// Make the mutations public in a private copy of the descriptor
			// and add it to the Collection, so that we can use SQL below to perform
			// the validation. We wouldn't have needed to do this if we could have
			// updated the descriptor and run validation in the same transaction. However,
			// our current system is incapable of running long running schema changes
			// (the validation can take many minutes). So we pretend that the schema
			// has been updated and actually update it in a separate transaction that
			// follows this one.
			desc, err := tableDesc.MakeFirstMutationPublic(tabledesc.IgnoreConstraints)
			if err != nil {
				return err
			}
			tc := descs.NewCollection(sc.settings, sc.leaseMgr, nil /* hydratedTables */)
			// pretend that the schema has been modified.
			if err := tc.AddUncommittedDescriptor(desc); err != nil {
				return err
			}

			// Retrieve the row count in the index.
			var idxLen int64
			if err := runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, evalCtx *extendedEvalContext) error {
				// TODO(vivek): This is not a great API. Leaving #34304 open.
				ie := evalCtx.InternalExecutor.(*InternalExecutor)
				ie.tcModifier = tc
				defer func() {
					ie.tcModifier = nil
				}()

				query := fmt.Sprintf(`SELECT count(1) FROM [%d AS t]@[%d]`, desc.GetID(), idx.ID)
				// If the index is a partial index the predicate must be added
				// as a filter to the query to force scanning the index.
				if idx.IsPartial() {
					query = fmt.Sprintf(`%s WHERE %s`, query, idx.Predicate)
				}

				row, err := ie.QueryRowEx(ctx, "verify-idx-count", txn, sessiondata.InternalExecutorOverride{}, query)
				if err != nil {
					return err
				}
				idxLen = int64(tree.MustBeDInt(row[0]))
				return nil
			}); err != nil {
				return err
			}

			log.Infof(ctx, "validation: index %s/%s row count = %d, time so far %s",
				tableDesc.Name, idx.Name, idxLen, timeutil.Since(start))

			// Now compare with the row count in the table.
			select {
			case <-tableCountsReady:
				expectedCount := tableRowCount
				// If the index is a partial index, the expected number of rows
				// is different than the total number of rows in the table.
				if idx.IsPartial() {
					expectedCount = partialIndexExpectedCounts[idx.ID]
				}

				if idxLen != expectedCount {
					// TODO(vivek): find the offending row and include it in the error.
					return pgerror.Newf(
						pgcode.UniqueViolation,
						"%d entries, expected %d violates unique constraint %q",
						idxLen, expectedCount, idx.Name,
					)
				}

			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		})
	}

	grp.GoCtx(func(ctx context.Context) error {
		defer close(tableCountsReady)
		var tableRowCountTime time.Duration
		start := timeutil.Now()

		// The query to count the expected number of rows can reference columns
		// added earlier in the same mutation. Here we make those mutations
		// pubic so that the query can reference those columns.
		desc, err := tableDesc.MakeFirstMutationPublic(tabledesc.IgnoreConstraints)
		if err != nil {
			return err
		}

		tc := descs.NewCollection(sc.settings, sc.leaseMgr, nil /* hydratedTables */)
		if err := tc.AddUncommittedDescriptor(desc); err != nil {
			return err
		}

		// Count the number of rows in the table.
		if err := runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, evalCtx *extendedEvalContext) error {
			ie := evalCtx.InternalExecutor.(*InternalExecutor)
			ie.tcModifier = tc
			defer func() {
				ie.tcModifier = nil
			}()

			var s strings.Builder
			for _, idx := range indexes {
				// For partial indexes, count the number of rows in the table
				// for which the predicate expression evaluates to true.
				if idx.IsPartial() {
					s.WriteString(fmt.Sprintf(`, count(1) FILTER (WHERE %s)`, idx.Predicate))
				}
			}
			partialIndexCounts := s.String()

			// Force the primary index so that the optimizer does not create a
			// query plan that uses the indexes being backfilled.
			query := fmt.Sprintf(`SELECT count(1)%s FROM [%d AS t]@[%d]`, partialIndexCounts, desc.ID, desc.GetPrimaryIndexID())

			cnt, err := ie.QueryRowEx(ctx, "VERIFY INDEX", txn, sessiondata.InternalExecutorOverride{}, query)
			if err != nil {
				return err
			}

			tableRowCount = int64(tree.MustBeDInt(cnt[0]))
			cntIdx := 1
			for _, idx := range indexes {
				if idx.IsPartial() {
					partialIndexExpectedCounts[idx.ID] = int64(tree.MustBeDInt(cnt[cntIdx]))
					cntIdx++
				}
			}

			return nil
		}); err != nil {
			return err
		}

		tableRowCountTime = timeutil.Since(start)
		log.Infof(ctx, "validation: table %s row count = %d, took %s",
			tableDesc.Name, tableRowCount, tableRowCountTime)
		return nil
	})

	return grp.Wait()
}

// backfillIndexes fills the missing columns in the indexes of the
// leased tables.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely.
func (sc *SchemaChanger) backfillIndexes(
	ctx context.Context, version descpb.DescriptorVersion, addingSpans []roachpb.Span,
) error {
	log.Infof(ctx, "backfilling %d indexes", len(addingSpans))

	if fn := sc.testingKnobs.RunBeforeIndexBackfill; fn != nil {
		fn()
	}

	// Split off a new range for each new index span. But only do so for the
	// system tenant. Secondary tenants do not have mandatory split points
	// between tables or indexes.
	if sc.execCfg.Codec.ForSystemTenant() {
		expirationTime := sc.db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
		for _, span := range addingSpans {
			if err := sc.db.AdminSplit(ctx, span.Key, expirationTime); err != nil {
				return err
			}
		}
	}

	if err := sc.distIndexBackfill(
		ctx, version, addingSpans, backfill.IndexMutationFilter, indexBackfillBatchSize); err != nil {
		return err
	}

	log.Info(ctx, "finished backfilling indexes")
	return sc.validateIndexes(ctx)
}

// truncateAndBackfillColumns performs the backfill operation on the given leased
// table descriptors.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely.
func (sc *SchemaChanger) truncateAndBackfillColumns(
	ctx context.Context, version descpb.DescriptorVersion,
) error {
	log.Infof(ctx, "clearing and backfilling columns")

	if err := sc.distBackfill(
		ctx, version, columnBackfill, columnTruncateAndBackfillChunkSize,
		backfill.ColumnMutationFilter, nil); err != nil {
		return err
	}
	log.Info(ctx, "finished clearing and backfilling columns")
	return nil
}

// runSchemaChangesInTxn runs all the schema changes immediately in a
// transaction. This is called when a CREATE TABLE is followed by
// schema changes in the same transaction. The CREATE TABLE is
// invisible to the rest of the cluster, so the schema changes
// can be executed immediately on the same version of the table.
//
// It operates entirely on the current goroutine and is thus able to
// reuse the planner's kv.Txn safely.
func runSchemaChangesInTxn(
	ctx context.Context, planner *planner, tableDesc *tabledesc.Mutable, traceKV bool,
) error {
	if len(tableDesc.DrainingNames) > 0 {
		// Reclaim all the old names. Leave the data and descriptor
		// cleanup for later.
		for _, drain := range tableDesc.DrainingNames {
			err := catalogkv.RemoveObjectNamespaceEntry(ctx, planner.Txn(), planner.ExecCfg().Codec,
				drain.ParentID, drain.ParentSchemaID, drain.Name, false /* KVTrace */)
			if err != nil {
				return err
			}
		}
		tableDesc.DrainingNames = nil
	}

	if tableDesc.Dropped() {
		return nil
	}

	// Only needed because columnBackfillInTxn() backfills
	// all column mutations.
	doneColumnBackfill := false

	// Mutations are processed in multiple steps: First we process all mutations
	// for schema changes other than adding check or FK constraints, then we
	// validate those constraints, and only after that do we process the
	// constraint mutations. We need an in-memory copy of the table descriptor
	// that contains newly added columns (since constraints being added can
	// reference them), but that doesn't contain constraints (since otherwise we'd
	// plan the query assuming the constraint holds). This is a different
	// procedure than in the schema changer for existing tables, since all the
	// "steps" in the schema change occur within the same transaction here.
	//
	// In the future it would be good to either unify the two implementations more
	// or make this in-transaction implementation more principled. We expect
	// constraint validation to be refactored and treated as a first-class concept
	// in the world of transactional schema changes.

	// Collect constraint mutations to process later.
	var constraintAdditionMutations []descpb.DescriptorMutation

	// We use a range loop here as the processing of some mutations
	// such as the primary key swap mutations result in queueing more
	// mutations that need to be processed.
	for i := 0; i < len(tableDesc.Mutations); i++ {
		m := tableDesc.Mutations[i]
		immutDesc := tabledesc.NewImmutable(*tableDesc.TableDesc())
		switch m.Direction {
		case descpb.DescriptorMutation_ADD:
			switch m.Descriptor_.(type) {
			case *descpb.DescriptorMutation_PrimaryKeySwap:
				// Don't need to do anything here, as the call to MakeMutationComplete
				// will perform the steps for this operation.
			case *descpb.DescriptorMutation_ComputedColumnSwap:
				return AlterColTypeInTxnNotSupportedErr
			case *descpb.DescriptorMutation_Column:
				if doneColumnBackfill || !tabledesc.ColumnNeedsBackfill(m.GetColumn()) {
					break
				}
				if err := columnBackfillInTxn(ctx, planner.Txn(), planner.EvalContext(), planner.SemaCtx(), immutDesc, traceKV); err != nil {
					return err
				}
				doneColumnBackfill = true

			case *descpb.DescriptorMutation_Index:
				if err := indexBackfillInTxn(ctx, planner.Txn(), planner.EvalContext(), planner.SemaCtx(), immutDesc, traceKV); err != nil {
					return err
				}

			case *descpb.DescriptorMutation_Constraint:
				// This is processed later. Do not proceed to MakeMutationComplete.
				constraintAdditionMutations = append(constraintAdditionMutations, m)
				continue

			default:
				return errors.AssertionFailedf(
					"unsupported mutation: %+v", m)
			}

		case descpb.DescriptorMutation_DROP:
			// Drop the name and drop the associated data later.
			switch t := m.Descriptor_.(type) {
			case *descpb.DescriptorMutation_Column:
				if doneColumnBackfill {
					break
				}
				if err := columnBackfillInTxn(
					ctx, planner.Txn(), planner.EvalContext(), planner.SemaCtx(), immutDesc, traceKV,
				); err != nil {
					return err
				}
				doneColumnBackfill = true

			case *descpb.DescriptorMutation_Index:
				if err := indexTruncateInTxn(
					ctx, planner.Txn(), planner.ExecCfg(), planner.EvalContext(), immutDesc, t.Index, traceKV,
				); err != nil {
					return err
				}

			case *descpb.DescriptorMutation_Constraint:
				switch t.Constraint.ConstraintType {
				case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
					for i := range tableDesc.Checks {
						if tableDesc.Checks[i].Name == t.Constraint.Name {
							tableDesc.Checks = append(tableDesc.Checks[:i], tableDesc.Checks[i+1:]...)
							break
						}
					}
				case descpb.ConstraintToUpdate_FOREIGN_KEY:
					for i := range tableDesc.OutboundFKs {
						fk := &tableDesc.OutboundFKs[i]
						if fk.Name == t.Constraint.Name {
							if err := planner.removeFKBackReference(ctx, tableDesc, fk); err != nil {
								return err
							}
							tableDesc.OutboundFKs = append(tableDesc.OutboundFKs[:i], tableDesc.OutboundFKs[i+1:]...)
							break
						}
					}
				default:
					return errors.AssertionFailedf(
						"unsupported constraint type: %d", errors.Safe(t.Constraint.ConstraintType))
				}

			default:
				return errors.AssertionFailedf("unsupported mutation: %+v", m)
			}

		}
		if err := tableDesc.MakeMutationComplete(m); err != nil {
			return err
		}

		// If the mutation we processed was a primary key swap, there is some
		// extra work that needs to be done. Note that we don't need to create
		// a job to clean up the dropped indexes because those mutations can
		// get processed in this txn on the new table.
		if pkSwap := m.GetPrimaryKeySwap(); pkSwap != nil {
			// If any old index had an interleaved parent, remove the
			// backreference from the parent.
			// N.B. This logic needs to be kept up to date with the
			// corresponding piece in (*SchemaChanger).done. It is slightly
			// different because of how it access tables and how it needs to
			// write the modified table descriptors explicitly.
			for _, idxID := range append(
				[]descpb.IndexID{pkSwap.OldPrimaryIndexId}, pkSwap.OldIndexes...) {
				oldIndex, err := tableDesc.FindIndexWithID(idxID)
				if err != nil {
					return err
				}
				if oldIndex.NumInterleaveAncestors() != 0 {
					ancestorInfo := oldIndex.GetInterleaveAncestor(oldIndex.NumInterleaveAncestors() - 1)
					ancestor, err := planner.Descriptors().GetMutableTableVersionByID(ctx, ancestorInfo.TableID, planner.txn)
					if err != nil {
						return err
					}
					ancestorIdxI, err := ancestor.FindIndexWithID(ancestorInfo.IndexID)
					if err != nil {
						return err
					}
					ancestorIdx := ancestorIdxI.IndexDesc()
					foundAncestor := false
					for k, ref := range ancestorIdx.InterleavedBy {
						if ref.Table == tableDesc.ID && ref.Index == oldIndex.GetID() {
							if foundAncestor {
								return errors.AssertionFailedf(
									"ancestor entry in %s for %s@%s found more than once",
									ancestor.Name, tableDesc.Name, oldIndex.GetName())
							}
							ancestorIdx.InterleavedBy = append(
								ancestorIdx.InterleavedBy[:k], ancestorIdx.InterleavedBy[k+1:]...)
							foundAncestor = true
							if err := planner.writeSchemaChange(ctx, ancestor, descpb.InvalidMutationID,
								fmt.Sprintf("remove interleaved backreference from table %s(%d) "+
									"for primary key swap of table %s(%d)",
									ancestor.Name, ancestor.ID, tableDesc.Name, tableDesc.ID,
								)); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
	// Clear all the mutations except for adding constraints.
	tableDesc.Mutations = constraintAdditionMutations

	// Now that the table descriptor is in a valid state with all column and index
	// mutations applied, it can be used for validating check/FK constraints.
	for _, m := range constraintAdditionMutations {
		constraint := m.GetConstraint()
		switch constraint.ConstraintType {
		case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
			if constraint.Check.Validity == descpb.ConstraintValidity_Validating {
				if err := validateCheckInTxn(
					ctx, planner.Descriptors().LeaseManager(), &planner.semaCtx, planner.EvalContext(), tableDesc, planner.txn, constraint.Check.Expr,
				); err != nil {
					return err
				}
				constraint.Check.Validity = descpb.ConstraintValidity_Validated
			}
		case descpb.ConstraintToUpdate_FOREIGN_KEY:
			// We can't support adding a validated foreign key constraint in the same
			// transaction as the CREATE TABLE statement. This would require adding
			// the backreference to the other table and then validating the constraint
			// for whatever rows were inserted into the referencing table in this
			// transaction, which requires multiple schema changer states across
			// multiple transactions.
			//
			// We could partially fix this by queuing a validation job to run post-
			// transaction. Better yet would be to absorb this into the transactional
			// schema change framework eventually.
			//
			// For now, just always add the FK as unvalidated.
			constraint.ForeignKey.Validity = descpb.ConstraintValidity_Unvalidated
		default:
			return errors.AssertionFailedf(
				"unsupported constraint type: %d", errors.Safe(constraint.ConstraintType))
		}
	}

	// Finally, add the constraints. We bypass MakeMutationsComplete (which makes
	// certain assumptions about the state in the usual schema changer) and just
	// update the table descriptor directly.
	for _, m := range constraintAdditionMutations {
		constraint := m.GetConstraint()
		switch constraint.ConstraintType {
		case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
			tableDesc.Checks = append(tableDesc.Checks, &constraint.Check)
		case descpb.ConstraintToUpdate_FOREIGN_KEY:
			fk := constraint.ForeignKey
			var referencedTableDesc *tabledesc.Mutable
			// We don't want to lookup/edit a second copy of the same table.
			selfReference := tableDesc.ID == fk.ReferencedTableID
			if selfReference {
				referencedTableDesc = tableDesc
			} else {
				lookup, err := planner.Descriptors().GetMutableTableVersionByID(ctx, fk.ReferencedTableID, planner.Txn())
				if err != nil {
					return errors.Errorf("error resolving referenced table ID %d: %v", fk.ReferencedTableID, err)
				}
				referencedTableDesc = lookup
			}
			referencedTableDesc.InboundFKs = append(referencedTableDesc.InboundFKs, fk)
			tableDesc.OutboundFKs = append(tableDesc.OutboundFKs, fk)

			// Write the other table descriptor here if it's not the current table
			// we're already modifying.
			if !selfReference {
				if err := planner.writeSchemaChange(
					ctx, referencedTableDesc, descpb.InvalidMutationID,
					fmt.Sprintf("updating referenced FK table %s(%d) table %s(%d)",
						referencedTableDesc.Name, referencedTableDesc.ID, tableDesc.Name, tableDesc.ID),
				); err != nil {
					return err
				}
			}
		default:
			return errors.AssertionFailedf(
				"unsupported constraint type: %d", errors.Safe(constraint.ConstraintType))
		}
	}
	tableDesc.Mutations = nil
	return nil
}

// validateCheckInTxn validates check constraints within the provided
// transaction. If the provided table descriptor version is newer than the
// cluster version, it will be used in the InternalExecutor that performs the
// validation query.
//
// TODO (lucy): The special case where the table descriptor version is the same
// as the cluster version only happens because the query in VALIDATE CONSTRAINT
// still runs in the user transaction instead of a step in the schema changer.
// When that's no longer true, this function should be updated.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func validateCheckInTxn(
	ctx context.Context,
	leaseMgr *lease.Manager,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	tableDesc *tabledesc.Mutable,
	txn *kv.Txn,
	checkExpr string,
) error {
	ie := evalCtx.InternalExecutor.(*InternalExecutor)
	if tableDesc.Version > tableDesc.ClusterVersion.Version {
		newTc := descs.NewCollection(evalCtx.Settings, leaseMgr, nil /* hydratedTables */)
		// pretend that the schema has been modified.
		if err := newTc.AddUncommittedDescriptor(tableDesc); err != nil {
			return err
		}

		ie.tcModifier = newTc
		defer func() {
			ie.tcModifier = nil
		}()
	}
	return validateCheckExpr(ctx, semaCtx, checkExpr, tableDesc, ie, txn)
}

// validateFkInTxn validates foreign key constraints within the provided
// transaction. If the provided table descriptor version is newer than the
// cluster version, it will be used in the InternalExecutor that performs the
// validation query.
//
// TODO (lucy): The special case where the table descriptor version is the same
// as the cluster version only happens because the query in VALIDATE CONSTRAINT
// still runs in the user transaction instead of a step in the schema changer.
// When that's no longer true, this function should be updated.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func validateFkInTxn(
	ctx context.Context,
	leaseMgr *lease.Manager,
	evalCtx *tree.EvalContext,
	tableDesc *tabledesc.Mutable,
	txn *kv.Txn,
	fkName string,
) error {
	ie := evalCtx.InternalExecutor.(*InternalExecutor)
	if tableDesc.Version > tableDesc.ClusterVersion.Version {
		newTc := descs.NewCollection(evalCtx.Settings, leaseMgr, nil /* hydratedTables */)
		// pretend that the schema has been modified.
		if err := newTc.AddUncommittedDescriptor(tableDesc); err != nil {
			return err
		}

		ie.tcModifier = newTc
		defer func() {
			ie.tcModifier = nil
		}()
	}

	var fk *descpb.ForeignKeyConstraint
	for i := range tableDesc.OutboundFKs {
		def := &tableDesc.OutboundFKs[i]
		if def.Name == fkName {
			fk = def
			break
		}
	}
	if fk == nil {
		return errors.AssertionFailedf("foreign key %s does not exist", fkName)
	}

	return validateForeignKey(ctx, tableDesc, fk, ie, txn, evalCtx.Codec)
}

// columnBackfillInTxn backfills columns for all mutation columns in
// the mutation list.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func columnBackfillInTxn(
	ctx context.Context,
	txn *kv.Txn,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	tableDesc *tabledesc.Immutable,
	traceKV bool,
) error {
	// A column backfill in the ADD state is a noop.
	if tableDesc.Adding() {
		return nil
	}
	var columnBackfillerMon *mon.BytesMonitor
	// This is the planner's memory monitor.
	if evalCtx.Mon != nil {
		columnBackfillerMon = execinfra.NewMonitor(ctx, evalCtx.Mon, "local-column-backfill-mon")
	}

	var backfiller backfill.ColumnBackfiller
	if err := backfiller.InitForLocalUse(ctx, evalCtx, semaCtx, tableDesc, columnBackfillerMon); err != nil {
		return err
	}
	defer backfiller.Close(ctx)
	sp := tableDesc.PrimaryIndexSpan(evalCtx.Codec)
	for sp.Key != nil {
		var err error
		sp.Key, err = backfiller.RunColumnBackfillChunk(ctx,
			txn, tableDesc, sp, columnTruncateAndBackfillChunkSize,
			false /*alsoCommit*/, traceKV)
		if err != nil {
			return err
		}
	}

	return nil
}

// indexBackfillInTxn runs one chunk of the index backfill on the
// primary index.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func indexBackfillInTxn(
	ctx context.Context,
	txn *kv.Txn,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	tableDesc *tabledesc.Immutable,
	traceKV bool,
) error {
	var indexBackfillerMon *mon.BytesMonitor
	// This is the planner's memory monitor.
	if evalCtx.Mon != nil {
		indexBackfillerMon = execinfra.NewMonitor(ctx, evalCtx.Mon, "local-index-backfill-mon")
	}

	var backfiller backfill.IndexBackfiller
	if err := backfiller.InitForLocalUse(ctx, evalCtx, semaCtx, tableDesc, indexBackfillerMon); err != nil {
		return err
	}
	defer backfiller.Close(ctx)
	sp := tableDesc.PrimaryIndexSpan(evalCtx.Codec)
	for sp.Key != nil {
		var err error
		sp.Key, err = backfiller.RunIndexBackfillChunk(ctx,
			txn, tableDesc, sp, indexTxnBackfillChunkSize, false /* alsoCommit */, traceKV)
		if err != nil {
			return err
		}
	}

	return nil
}

// indexTruncateInTxn deletes an index from a table.
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func indexTruncateInTxn(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	evalCtx *tree.EvalContext,
	tableDesc *tabledesc.Immutable,
	idx *descpb.IndexDescriptor,
	traceKV bool,
) error {
	alloc := &rowenc.DatumAlloc{}
	var sp roachpb.Span
	for done := false; !done; done = sp.Key == nil {
		rd := row.MakeDeleter(execCfg.Codec, tableDesc, nil /* requestedCols */)
		td := tableDeleter{rd: rd, alloc: alloc}
		if err := td.init(ctx, txn, evalCtx); err != nil {
			return err
		}
		var err error
		sp, err = td.deleteIndex(
			ctx, idx, sp, indexTruncateChunkSize, traceKV,
		)
		if err != nil {
			return err
		}
	}
	// Remove index zone configs.
	return RemoveIndexZoneConfigs(ctx, txn, execCfg, tableDesc.ID, []descpb.IndexDescriptor{*idx})
}
