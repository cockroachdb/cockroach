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
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	indexTruncateChunkSize = 600

	// indexTxnBackfillChunkSize is the maximum number index entries backfilled
	// per chunk during an index backfill done in a txn. The index backfill
	// involves a table scan, and a number of individual ops presented in a batch.
	// This value is smaller than ColumnTruncateAndBackfillChunkSize, because it
	// involves a number of individual index row updates that can be scattered
	// over many ranges.
	indexTxnBackfillChunkSize = 100

	// checkpointInterval is the interval after which a checkpoint of the
	// schema change is posted.
	checkpointInterval = 2 * time.Minute
)

var indexBulkBackfillChunkSize = settings.RegisterIntSetting(
	"schemachanger.bulk_index_backfill.batch_size",
	"number of rows to process at a time during bulk index backfill",
	50000,
)

var _ sort.Interface = columnsByID{}
var _ sort.Interface = indexesByID{}

type columnsByID []sqlbase.ColumnDescriptor

func (cds columnsByID) Len() int {
	return len(cds)
}
func (cds columnsByID) Less(i, j int) bool {
	return cds[i].ID < cds[j].ID
}
func (cds columnsByID) Swap(i, j int) {
	cds[i], cds[j] = cds[j], cds[i]
}

type indexesByID []sqlbase.IndexDescriptor

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
	var droppedIndexDescs []sqlbase.IndexDescriptor
	var addedIndexSpans []roachpb.Span

	var constraintsToDrop []sqlbase.ConstraintToUpdate
	var constraintsToAddBeforeValidation []sqlbase.ConstraintToUpdate
	var constraintsToValidate []sqlbase.ConstraintToUpdate

	tableDesc, err := sc.updateJobRunningStatus(ctx, RunningStatusBackfill)
	if err != nil {
		return err
	}

	// Short circuit the backfill if the table has been deleted.
	if tableDesc.Dropped() {
		return nil
	}
	version := tableDesc.Version

	log.Infof(ctx, "Running backfill for %q, v=%d", tableDesc.Name, tableDesc.Version)

	needColumnBackfill := false
	for _, m := range tableDesc.Mutations {
		if m.MutationID != sc.mutationID {
			break
		}
		switch m.Direction {
		case sqlbase.DescriptorMutation_ADD:
			switch t := m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_Column:
				if sqlbase.ColumnNeedsBackfill(m.GetColumn()) {
					needColumnBackfill = true
				}
			case *sqlbase.DescriptorMutation_Index:
				addedIndexSpans = append(addedIndexSpans, tableDesc.IndexSpan(sc.execCfg.Codec, t.Index.ID))
			case *sqlbase.DescriptorMutation_Constraint:
				switch t.Constraint.ConstraintType {
				case sqlbase.ConstraintToUpdate_CHECK:
					if t.Constraint.Check.Validity == sqlbase.ConstraintValidity_Validating {
						constraintsToAddBeforeValidation = append(constraintsToAddBeforeValidation, *t.Constraint)
						constraintsToValidate = append(constraintsToValidate, *t.Constraint)
					}
				case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
					if t.Constraint.ForeignKey.Validity == sqlbase.ConstraintValidity_Validating {
						constraintsToAddBeforeValidation = append(constraintsToAddBeforeValidation, *t.Constraint)
						constraintsToValidate = append(constraintsToValidate, *t.Constraint)
					}
				case sqlbase.ConstraintToUpdate_NOT_NULL:
					// NOT NULL constraints are always validated before they can be added
					constraintsToAddBeforeValidation = append(constraintsToAddBeforeValidation, *t.Constraint)
					constraintsToValidate = append(constraintsToValidate, *t.Constraint)
				}
			case *sqlbase.DescriptorMutation_PrimaryKeySwap, *sqlbase.DescriptorMutation_ComputedColumnSwap:
				// The backfiller doesn't need to do anything here.
			default:
				return errors.AssertionFailedf(
					"unsupported mutation: %+v", m)
			}

		case sqlbase.DescriptorMutation_DROP:
			switch t := m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_Column:
				needColumnBackfill = true
			case *sqlbase.DescriptorMutation_Index:
				if !canClearRangeForDrop(t.Index) {
					droppedIndexDescs = append(droppedIndexDescs, *t.Index)
				}
			case *sqlbase.DescriptorMutation_Constraint:
				constraintsToDrop = append(constraintsToDrop, *t.Constraint)
			case *sqlbase.DescriptorMutation_PrimaryKeySwap, *sqlbase.DescriptorMutation_ComputedColumnSwap:
				// The backfiller doesn't need to do anything here.
			default:
				return errors.AssertionFailedf(
					"unsupported mutation: %+v", m)
			}
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

	log.Infof(ctx, "Completed backfill for %q, v=%d", tableDesc.Name, tableDesc.Version)

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
	ctx context.Context, constraints []sqlbase.ConstraintToUpdate,
) (map[sqlbase.ID]*ImmutableTableDescriptor, error) {
	log.Infof(ctx, "dropping %d constraints", len(constraints))

	fksByBackrefTable := make(map[sqlbase.ID][]*sqlbase.ConstraintToUpdate)
	for i := range constraints {
		c := &constraints[i]
		if c.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
			c.ForeignKey.ReferencedTableID != sc.tableID {
			fksByBackrefTable[c.ForeignKey.ReferencedTableID] = append(fksByBackrefTable[c.ForeignKey.ReferencedTableID], c)
		}
	}
	tableIDsToUpdate := make([]sqlbase.ID, 0, len(fksByBackrefTable)+1)
	tableIDsToUpdate = append(tableIDsToUpdate, sc.tableID)
	for id := range fksByBackrefTable {
		tableIDsToUpdate = append(tableIDsToUpdate, id)
	}

	// Create update closure for the table and all other tables with backreferences.
	update := func(_ *kv.Txn, descs map[sqlbase.ID]catalog.MutableDescriptor) error {
		scDesc, ok := descs[sc.tableID]
		if !ok {
			return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
		}
		scTable := scDesc.(*MutableTableDescriptor)
		for i := range constraints {
			constraint := &constraints[i]
			switch constraint.ConstraintType {
			case sqlbase.ConstraintToUpdate_CHECK, sqlbase.ConstraintToUpdate_NOT_NULL:
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
			case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
				var foundExisting bool
				for j := range scTable.OutboundFKs {
					def := &scTable.OutboundFKs[j]
					if def.Name == constraint.Name {
						backrefDesc, ok := descs[constraint.ForeignKey.ReferencedTableID]
						if !ok {
							return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
						}
						backrefTable := backrefDesc.(*MutableTableDescriptor)
						if err := removeFKBackReferenceFromTable(backrefTable, def.Name, scTable.TableDesc()); err != nil {
							return err
						}
						scTable.OutboundFKs = append(scTable.OutboundFKs[:j], scTable.OutboundFKs[j+1:]...)
						foundExisting = true
						break
					}
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
		return nil
	}

	descs, err := sc.leaseMgr.PublishMultiple(ctx, tableIDsToUpdate, update, nil)
	if err != nil {
		return nil, err
	}
	if err := sc.waitToUpdateLeases(ctx, sc.tableID); err != nil {
		return nil, err
	}
	for id := range fksByBackrefTable {
		if err := sc.waitToUpdateLeases(ctx, id); err != nil {
			return nil, err
		}
	}

	log.Info(ctx, "finished dropping constraints")

	tableDescs := make(map[sqlbase.ID]*ImmutableTableDescriptor)
	for i := range descs {
		tableDescs[i] = descs[i].(*ImmutableTableDescriptor)
	}
	return tableDescs, nil
}

// addConstraints publishes a new version of the given table descriptor with the
// given constraint added to it, and waits until the entire cluster is on
// the new version of the table descriptor.
func (sc *SchemaChanger) addConstraints(
	ctx context.Context, constraints []sqlbase.ConstraintToUpdate,
) error {
	log.Infof(ctx, "adding %d constraints", len(constraints))

	fksByBackrefTable := make(map[sqlbase.ID][]*sqlbase.ConstraintToUpdate)
	for i := range constraints {
		c := &constraints[i]
		if c.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
			c.ForeignKey.ReferencedTableID != sc.tableID {
			fksByBackrefTable[c.ForeignKey.ReferencedTableID] = append(fksByBackrefTable[c.ForeignKey.ReferencedTableID], c)
		}
	}
	tableIDsToUpdate := make([]sqlbase.ID, 0, len(fksByBackrefTable)+1)
	tableIDsToUpdate = append(tableIDsToUpdate, sc.tableID)
	for id := range fksByBackrefTable {
		tableIDsToUpdate = append(tableIDsToUpdate, id)
	}

	// Create update closure for the table and all other tables with backreferences
	update := func(_ *kv.Txn, descs map[sqlbase.ID]catalog.MutableDescriptor) error {
		scDesc, ok := descs[sc.tableID]
		if !ok {
			return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
		}
		scTable := scDesc.(*MutableTableDescriptor)
		for i := range constraints {
			constraint := &constraints[i]
			switch constraint.ConstraintType {
			case sqlbase.ConstraintToUpdate_CHECK, sqlbase.ConstraintToUpdate_NOT_NULL:
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
						c.Validity = sqlbase.ConstraintValidity_Validating
						found = true
						break
					}
				}
				if !found {
					scTable.Checks = append(scTable.Checks, &constraints[i].Check)
				}
			case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
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
						def.Validity = sqlbase.ConstraintValidity_Validating
						foundExisting = true
						break
					}
				}
				if !foundExisting {
					scTable.OutboundFKs = append(scTable.OutboundFKs, constraint.ForeignKey)
					backrefDesc, ok := descs[constraint.ForeignKey.ReferencedTableID]
					if !ok {
						return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
					}
					backrefTable := backrefDesc.(*MutableTableDescriptor)
					backrefTable.InboundFKs = append(backrefTable.InboundFKs, constraint.ForeignKey)
				}
			}
		}
		return nil
	}

	if _, err := sc.leaseMgr.PublishMultiple(ctx, tableIDsToUpdate, update, nil); err != nil {
		return err
	}
	if err := sc.waitToUpdateLeases(ctx, sc.tableID); err != nil {
		return err
	}
	for id := range fksByBackrefTable {
		if err := sc.waitToUpdateLeases(ctx, id); err != nil {
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
	ctx context.Context, constraints []sqlbase.ConstraintToUpdate,
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
	var tableDesc *sqlbase.TableDescriptor

	if err := sc.fixedTimestampTxn(ctx, readAsOf, func(ctx context.Context, txn *kv.Txn) error {
		tableDesc, err = sqlbase.GetTableDescFromID(ctx, txn, sc.execCfg.Codec, sc.tableID)
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
			desc, err := sqlbase.NewImmutableTableDescriptor(*tableDesc).MakeFirstMutationPublic(sqlbase.IgnoreConstraints)
			if err != nil {
				return err
			}
			// Each check operates at the historical timestamp.
			return runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, evalCtx *extendedEvalContext) error {
				// If the constraint is a check constraint that fails validation, we
				// need a semaContext set up that can resolve types in order to pretty
				// print the check expression back to the user.
				evalCtx.Txn = txn
				semaCtx := tree.MakeSemaContext()
				// Use the DistSQLTypeResolver because we need to resolve types by ID.
				semaCtx.TypeResolver = &execinfrapb.DistSQLTypeResolver{EvalContext: &evalCtx.EvalContext}
				switch c.ConstraintType {
				case sqlbase.ConstraintToUpdate_CHECK:
					if err := validateCheckInTxn(ctx, sc.leaseMgr, &semaCtx, &evalCtx.EvalContext, desc, txn, c.Check.Name); err != nil {
						return err
					}
				case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
					if err := validateFkInTxn(ctx, sc.leaseMgr, &evalCtx.EvalContext, desc, txn, c.Name); err != nil {
						return err
					}
				case sqlbase.ConstraintToUpdate_NOT_NULL:
					if err := validateCheckInTxn(ctx, sc.leaseMgr, &semaCtx, &evalCtx.EvalContext, desc, txn, c.Check.Name); err != nil {
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
	ctx context.Context, txn *kv.Txn, tc *descs.Collection, version sqlbase.DescriptorVersion,
) (*sqlbase.ImmutableTableDescriptor, error) {
	tableDesc, err := tc.GetTableVersionByID(ctx, txn, sc.tableID, tree.ObjectLookupFlags{})
	if err != nil {
		return nil, err
	}
	if version != tableDesc.Version {
		return nil, makeErrTableVersionMismatch(tableDesc.Version, version)
	}
	return tableDesc, nil
}

// truncateIndexes truncate the KV ranges corresponding to dropped indexes.
//
// The indexes are dropped chunk by chunk, each chunk being deleted in
// its own txn.
func (sc *SchemaChanger) truncateIndexes(
	ctx context.Context, version sqlbase.DescriptorVersion, dropped []sqlbase.IndexDescriptor,
) error {
	log.Infof(ctx, "clearing data for %d indexes", len(dropped))

	chunkSize := sc.getChunkSize(indexTruncateChunkSize)
	if sc.testingKnobs.BackfillChunkSize > 0 {
		chunkSize = sc.testingKnobs.BackfillChunkSize
	}
	alloc := &sqlbase.DatumAlloc{}
	for _, desc := range dropped {
		var resume roachpb.Span
		for rowIdx, done := int64(0), false; !done; rowIdx += chunkSize {
			resumeAt := resume
			if log.V(2) {
				log.Infof(ctx, "drop index (%d, %d) at row: %d, span: %s",
					sc.tableID, sc.mutationID, rowIdx, resume)
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
				tc := descs.NewCollection(sc.leaseMgr, sc.settings)
				defer tc.ReleaseAll(ctx)
				tableDesc, err := sc.getTableVersion(ctx, txn, tc, version)
				if err != nil {
					return err
				}

				// Hydrate types used in the retrieved table.
				// TODO (rohany): This can be removed once table access from the
				//  desc.Collection returns tables with hydrated types.
				typLookup := func(id sqlbase.ID) (*tree.TypeName, sqlbase.TypeDescriptorInterface, error) {
					return resolver.ResolveTypeDescByID(ctx, txn, sc.execCfg.Codec, id, tree.ObjectLookupFlags{})
				}
				if err := sqlbase.HydrateTypesInTableDescriptor(tableDesc.TableDesc(), typLookup); err != nil {
					return err
				}

				rd, err := row.MakeDeleter(
					ctx,
					txn,
					sc.execCfg.Codec,
					tableDesc,
					nil,
					alloc,
				)
				if err != nil {
					return err
				}
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
			return RemoveIndexZoneConfigs(ctx, txn, sc.execCfg, sc.tableID, dropped)
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
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, mutationID sqlbase.MutationID,
) (int64, error) {
	for _, job := range tableDesc.MutationJobs {
		if job.MutationID == mutationID {
			return job.JobID, nil
		}
	}

	return 0, errors.AssertionFailedf(
		"job not found for table id %d, mutation %d", tableDesc.ID, mutationID)
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

// distBackfill runs (or continues) a backfill for the first mutation
// enqueued on the SchemaChanger's table descriptor that passes the input
// MutationFilter.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely, so it makes its own.
func (sc *SchemaChanger) distBackfill(
	ctx context.Context,
	version sqlbase.DescriptorVersion,
	backfillType backfillType,
	backfillChunkSize int64,
	filter backfill.MutationFilter,
	targetSpans []roachpb.Span,
) error {
	inMemoryStatusEnabled := sc.execCfg.Settings.Version.IsActive(
		ctx, clusterversion.VersionAtomicChangeReplicasTrigger)
	duration := checkpointInterval
	if sc.testingKnobs.WriteCheckpointInterval > 0 {
		duration = sc.testingKnobs.WriteCheckpointInterval
	}
	chunkSize := sc.getChunkSize(backfillChunkSize)

	origNRanges := -1
	origFractionCompleted := sc.job.FractionCompleted()
	fractionLeft := 1 - origFractionCompleted
	readAsOf := sc.clock.Now()
	// Index backfilling ingests SSTs that don't play nicely with running txns
	// since they just add their keys blindly. Running a Scan of the target
	// spans at the time the SSTs' keys will be written will calcify history up
	// to then since the scan will resolve intents and populate tscache to keep
	// anything else from sneaking under us. Since these are new indexes, these
	// spans should be essentially empty, so this should be a pretty quick and
	// cheap scan.
	if backfillType == indexBackfill {
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
	}

	// Gather the initial resume spans for the table.
	var todoSpans []roachpb.Span
	var mutationIdx int
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		todoSpans, _, mutationIdx, err = rowexec.GetResumeSpans(
			ctx, sc.jobRegistry, txn, sc.execCfg.Codec, sc.tableID, sc.mutationID, filter)
		return err
	}); err != nil {
		return err
	}

	for len(todoSpans) > 0 {
		log.VEventf(ctx, 2, "backfill: process %+v spans", todoSpans)
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Report schema change progress. We define progress at this point
			// as the the fraction of fully-backfilled ranges of the primary index of
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

			tc := descs.NewCollection(sc.leaseMgr, sc.settings)
			// Use a leased table descriptor for the backfill.
			defer tc.ReleaseAll(ctx)
			tableDesc, err := sc.getTableVersion(ctx, txn, tc, version)
			if err != nil {
				return err
			}
			metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
				if meta.BulkProcessorProgress != nil {
					todoSpans = roachpb.SubtractSpans(todoSpans,
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
				func(ts hlc.Timestamp) {
					sc.clock.Update(ts)
				},
				evalCtx.Tracing,
			)
			defer recv.Release()

			planCtx := sc.distSQLPlanner.NewPlanningCtx(ctx, &evalCtx, txn, true /* distribute */)
			plan, err := sc.distSQLPlanner.createBackfiller(
				planCtx, backfillType, *tableDesc.TableDesc(), duration, chunkSize, todoSpans, readAsOf,
			)
			if err != nil {
				return err
			}
			sc.distSQLPlanner.Run(
				planCtx,
				nil, /* txn - the processors manage their own transactions */
				&plan, recv, &evalCtx,
				nil, /* finishedSetupFn */
			)()
			return cbw.Err()
		}); err != nil {
			return err
		}
		if !inMemoryStatusEnabled {
			var resumeSpans []roachpb.Span
			// There is a worker node of older version that will communicate
			// its done work by writing to the jobs table.
			// In this case we intersect todoSpans with what the old node(s)
			// have set in the jobs table not to overwrite their done work.
			if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				var err error
				resumeSpans, _, _, err = rowexec.GetResumeSpans(
					ctx, sc.jobRegistry, txn, sc.execCfg.Codec, sc.tableID, sc.mutationID, filter)
				return err
			}); err != nil {
				return err
			}
			// A \intersect B = A - (A - B)
			todoSpans = roachpb.SubtractSpans(todoSpans, roachpb.SubtractSpans(todoSpans, resumeSpans))

		}
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
// timestamp.
func (sc *SchemaChanger) updateJobRunningStatus(
	ctx context.Context, status jobs.RunningStatus,
) (*sqlbase.TableDescriptor, error) {
	var tableDesc *sqlbase.TableDescriptor
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		tableDesc, err = sqlbase.GetTableDescFromID(ctx, txn, sc.execCfg.Codec, sc.tableID)
		if err != nil {
			return err
		}
		// Update running status of job.
		updateJobRunningProgress := false
		for _, mutation := range tableDesc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}

			switch mutation.Direction {
			case sqlbase.DescriptorMutation_ADD:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					updateJobRunningProgress = true
				}

			case sqlbase.DescriptorMutation_DROP:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
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
	var tableDesc *sqlbase.TableDescriptor
	if err := sc.fixedTimestampTxn(ctx, readAsOf, func(ctx context.Context, txn *kv.Txn) error {
		tableDesc, err = sqlbase.GetTableDescFromID(ctx, txn, sc.execCfg.Codec, sc.tableID)
		return err
	}); err != nil {
		return err
	}

	var forwardIndexes []*sqlbase.IndexDescriptor
	var invertedIndexes []*sqlbase.IndexDescriptor

	for _, m := range tableDesc.Mutations {
		if sc.mutationID != m.MutationID {
			break
		}
		idx := m.GetIndex()
		if idx == nil || m.Direction == sqlbase.DescriptorMutation_DROP {
			continue
		}
		switch idx.Type {
		case sqlbase.IndexDescriptor_FORWARD:
			forwardIndexes = append(forwardIndexes, idx)
		case sqlbase.IndexDescriptor_INVERTED:
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
	tableDesc *TableDescriptor,
	indexes []*sqlbase.IndexDescriptor,
	runHistoricalTxn historicalTxnRunner,
) error {
	grp := ctxgroup.WithContext(ctx)

	expectedCount := make([]int64, len(indexes))
	countReady := make([]chan struct{}, len(indexes))

	for i, idx := range indexes {
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
			if len(idx.ColumnNames) != 1 {
				panic(fmt.Sprintf("expected inverted index %s to have exactly 1 column, but found columns %+v",
					idx.Name, idx.ColumnNames))
			}
			col := idx.ColumnNames[0]

			if err := runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, evalCtx *extendedEvalContext) error {
				ie := evalCtx.InternalExecutor.(*InternalExecutor)
				var stmt string
				if geoindex.IsEmptyConfig(&idx.GeoConfig) {
					stmt = fmt.Sprintf(
						`SELECT coalesce(sum_int(crdb_internal.num_inverted_index_entries(%q)), 0) FROM [%d AS t]`,
						col, tableDesc.ID,
					)
				} else {
					stmt = fmt.Sprintf(
						`SELECT coalesce(sum_int(crdb_internal.num_geo_inverted_index_entries(%d, %d, %q)), 0) FROM [%d AS t]`,
						tableDesc.ID, idx.ID, col, tableDesc.ID,
					)
				}
				row, err := ie.QueryRowEx(ctx, "verify-inverted-idx-count", txn,
					sqlbase.InternalExecutorSessionDataOverride{}, stmt)
				if err != nil {
					return err
				}
				expectedCount[i] = int64(tree.MustBeDInt(row[0]))
				return nil
			}); err != nil {
				return err
			}
			log.Infof(ctx, "JSON column %s/%s expected inverted index count = %d, took %s",
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
	tableDesc *TableDescriptor,
	indexes []*sqlbase.IndexDescriptor,
	runHistoricalTxn historicalTxnRunner,
) error {
	grp := ctxgroup.WithContext(ctx)

	var tableRowCount int64
	// Close when table count is ready.
	tableCountReady := make(chan struct{})
	// Compute the size of each index.
	for _, idx := range indexes {
		idx := idx

		// Skip partial indexes for now.
		// TODO(mgartner): Validate partial index entry counts.
		if idx.IsPartial() {
			continue
		}

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
			desc, err := sqlbase.NewImmutableTableDescriptor(*tableDesc).MakeFirstMutationPublic(sqlbase.IgnoreConstraints)
			if err != nil {
				return err
			}
			tc := descs.NewCollection(sc.leaseMgr, sc.settings)
			// pretend that the schema has been modified.
			if err := tc.AddUncommittedTable(*desc); err != nil {
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

				row, err := ie.QueryRowEx(ctx, "verify-idx-count", txn,
					sqlbase.InternalExecutorSessionDataOverride{},
					fmt.Sprintf(`SELECT count(1) FROM [%d AS t]@[%d]`, tableDesc.ID, idx.ID))
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
			case <-tableCountReady:
				if idxLen != tableRowCount {
					// TODO(vivek): find the offending row and include it in the error.
					return pgerror.Newf(
						pgcode.UniqueViolation,
						"%d entries, expected %d violates unique constraint %q",
						idxLen, tableRowCount, idx.Name,
					)
				}

			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		})
	}

	grp.GoCtx(func(ctx context.Context) error {
		defer close(tableCountReady)
		var tableRowCountTime time.Duration
		start := timeutil.Now()

		// Count the number of rows in the table.
		if err := runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, evalCtx *extendedEvalContext) error {
			ie := evalCtx.InternalExecutor.(*InternalExecutor)
			cnt, err := ie.QueryRowEx(ctx, "VERIFY INDEX", txn,
				sqlbase.InternalExecutorSessionDataOverride{},
				fmt.Sprintf(`SELECT count(1) FROM [%d AS t]`, tableDesc.ID))
			if err != nil {
				return err
			}
			tableRowCount = int64(tree.MustBeDInt(cnt[0]))
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
	ctx context.Context, version sqlbase.DescriptorVersion, addingSpans []roachpb.Span,
) error {
	log.Infof(ctx, "backfilling %d indexes", len(addingSpans))

	if fn := sc.testingKnobs.RunBeforeIndexBackfill; fn != nil {
		fn()
	}

	expirationTime := sc.db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)

	for _, span := range addingSpans {
		if err := sc.db.AdminSplit(ctx, span.Key, expirationTime); err != nil {
			return err
		}
	}

	chunkSize := indexBulkBackfillChunkSize.Get(&sc.settings.SV)
	if err := sc.distBackfill(
		ctx, version, indexBackfill, chunkSize,
		backfill.IndexMutationFilter, addingSpans); err != nil {
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
	ctx context.Context, version sqlbase.DescriptorVersion,
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
	ctx context.Context, planner *planner, tableDesc *sqlbase.MutableTableDescriptor, traceKV bool,
) error {
	if len(tableDesc.DrainingNames) > 0 {
		// Reclaim all the old names. Leave the data and descriptor
		// cleanup for later.
		for _, drain := range tableDesc.DrainingNames {
			err := sqlbase.RemoveObjectNamespaceEntry(ctx, planner.Txn(), planner.ExecCfg().Codec,
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
	// Checks are validated after all other mutations have been applied.
	var constraintsToValidate []sqlbase.ConstraintToUpdate

	// We use a range loop here as the processing of some mutations
	// such as the primary key swap mutations result in queueing more
	// mutations that need to be processed.
	for i := 0; i < len(tableDesc.Mutations); i++ {
		m := tableDesc.Mutations[i]
		immutDesc := sqlbase.NewImmutableTableDescriptor(*tableDesc.TableDesc())
		switch m.Direction {
		case sqlbase.DescriptorMutation_ADD:
			switch t := m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_PrimaryKeySwap:
				// Don't need to do anything here, as the call to MakeMutationComplete
				// will perform the steps for this operation.
			case *sqlbase.DescriptorMutation_ComputedColumnSwap:
				return AlterColTypeInTxnNotSupportedErr
			case *sqlbase.DescriptorMutation_Column:
				if doneColumnBackfill || !sqlbase.ColumnNeedsBackfill(m.GetColumn()) {
					break
				}
				if err := columnBackfillInTxn(ctx, planner.Txn(), planner.Tables(), planner.EvalContext(), immutDesc, traceKV); err != nil {
					return err
				}
				doneColumnBackfill = true

			case *sqlbase.DescriptorMutation_Index:
				if err := indexBackfillInTxn(ctx, planner.Txn(), planner.EvalContext(), immutDesc, traceKV); err != nil {
					return err
				}

			case *sqlbase.DescriptorMutation_Constraint:
				switch t.Constraint.ConstraintType {
				case sqlbase.ConstraintToUpdate_CHECK, sqlbase.ConstraintToUpdate_NOT_NULL:
					tableDesc.Checks = append(tableDesc.Checks, &t.Constraint.Check)
				case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
					fk := t.Constraint.ForeignKey
					var referencedTableDesc *sqlbase.MutableTableDescriptor
					// We don't want to lookup/edit a second copy of the same table.
					selfReference := tableDesc.ID == fk.ReferencedTableID
					if selfReference {
						referencedTableDesc = tableDesc
					} else {
						lookup, err := planner.Tables().GetMutableTableVersionByID(ctx, fk.ReferencedTableID, planner.Txn())
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
							ctx, referencedTableDesc, sqlbase.InvalidMutationID,
							fmt.Sprintf("updating referenced FK table %s(%d) table %s(%d)",
								referencedTableDesc.Name, referencedTableDesc.ID, tableDesc.Name, tableDesc.ID),
						); err != nil {
							return err
						}
					}
				default:
					return errors.AssertionFailedf(
						"unsupported constraint type: %d", errors.Safe(t.Constraint.ConstraintType))
				}
				constraintsToValidate = append(constraintsToValidate, *t.Constraint)

			default:
				return errors.AssertionFailedf(
					"unsupported mutation: %+v", m)
			}

		case sqlbase.DescriptorMutation_DROP:
			// Drop the name and drop the associated data later.
			switch t := m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_Column:
				if doneColumnBackfill {
					break
				}
				if err := columnBackfillInTxn(
					ctx, planner.Txn(), planner.Tables(), planner.EvalContext(), immutDesc, traceKV,
				); err != nil {
					return err
				}
				doneColumnBackfill = true

			case *sqlbase.DescriptorMutation_Index:
				if err := indexTruncateInTxn(
					ctx, planner.Txn(), planner.ExecCfg(), planner.EvalContext(), immutDesc, t.Index, traceKV,
				); err != nil {
					return err
				}

			case *sqlbase.DescriptorMutation_Constraint:
				switch t.Constraint.ConstraintType {
				case sqlbase.ConstraintToUpdate_CHECK, sqlbase.ConstraintToUpdate_NOT_NULL:
					for i := range tableDesc.Checks {
						if tableDesc.Checks[i].Name == t.Constraint.Name {
							tableDesc.Checks = append(tableDesc.Checks[:i], tableDesc.Checks[i+1:]...)
							break
						}
					}
				case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
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
		// TODO (lucy): This seems suspicious, since MakeMutationsComplete should
		// add unvalidated foreign keys, but we unconditionally add them above. Do
		// unvalidated FKs get added twice?
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
				[]sqlbase.IndexID{pkSwap.OldPrimaryIndexId}, pkSwap.OldIndexes...) {
				oldIndex, err := tableDesc.FindIndexByID(idxID)
				if err != nil {
					return err
				}
				if len(oldIndex.Interleave.Ancestors) != 0 {
					ancestorInfo := oldIndex.Interleave.Ancestors[len(oldIndex.Interleave.Ancestors)-1]
					ancestor, err := planner.Tables().GetMutableTableVersionByID(ctx, ancestorInfo.TableID, planner.txn)
					if err != nil {
						return err
					}
					ancestorIdx, err := ancestor.FindIndexByID(ancestorInfo.IndexID)
					if err != nil {
						return err
					}
					foundAncestor := false
					for k, ref := range ancestorIdx.InterleavedBy {
						if ref.Table == tableDesc.ID && ref.Index == oldIndex.ID {
							if foundAncestor {
								return errors.AssertionFailedf(
									"ancestor entry in %s for %s@%s found more than once",
									ancestor.Name, tableDesc.Name, oldIndex.Name)
							}
							ancestorIdx.InterleavedBy = append(
								ancestorIdx.InterleavedBy[:k], ancestorIdx.InterleavedBy[k+1:]...)
							foundAncestor = true
							if err := planner.writeSchemaChange(ctx, ancestor, sqlbase.InvalidMutationID,
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
	tableDesc.Mutations = nil

	// Now that the table descriptor is in a valid state with all column and index
	// mutations applied, it can be used for validating check constraints
	for _, c := range constraintsToValidate {
		switch c.ConstraintType {
		case sqlbase.ConstraintToUpdate_CHECK, sqlbase.ConstraintToUpdate_NOT_NULL:
			if err := validateCheckInTxn(
				ctx, planner.Tables().LeaseManager(), &planner.semaCtx, planner.EvalContext(), tableDesc, planner.txn, c.Check.Name,
			); err != nil {
				return err
			}
		case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
			// We can't support adding a validated foreign key constraint in the same
			// transaction as the CREATE TABLE statement. This would require adding
			// the backreference to the other table and then validating the constraint
			// for whatever rows were inserted into the referencing table in this
			// transaction, which requires multiple schema changer states across
			// multiple transactions.
			// TODO (lucy): Add a validation job that runs after the user transaction.
			// This won't roll back the original transaction if validation fails, but
			// it will at least leave the constraint in the Validated state if
			// validation succeeds.

			// For now, revert the constraint to an unvalidated state.
			for i := range tableDesc.OutboundFKs {
				desc := &tableDesc.OutboundFKs[i]
				if desc.Name == c.ForeignKey.Name {
					desc.Validity = sqlbase.ConstraintValidity_Unvalidated
					break
				}
			}
		default:
			return errors.AssertionFailedf(
				"unsupported constraint type: %d", errors.Safe(c.ConstraintType))
		}
	}
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
	tableDesc *MutableTableDescriptor,
	txn *kv.Txn,
	checkName string,
) error {
	ie := evalCtx.InternalExecutor.(*InternalExecutor)
	if tableDesc.Version > tableDesc.ClusterVersion.Version {
		newTc := descs.NewCollection(leaseMgr, evalCtx.Settings)
		// pretend that the schema has been modified.
		if err := newTc.AddUncommittedTable(*tableDesc); err != nil {
			return err
		}

		ie.tcModifier = newTc
		defer func() {
			ie.tcModifier = nil
		}()
	}

	check, err := tableDesc.FindCheckByName(checkName)
	if err != nil {
		return err
	}
	return validateCheckExpr(ctx, semaCtx, check.Expr, tableDesc, ie, txn)
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
	tableDesc *MutableTableDescriptor,
	txn *kv.Txn,
	fkName string,
) error {
	ie := evalCtx.InternalExecutor.(*InternalExecutor)
	if tableDesc.Version > tableDesc.ClusterVersion.Version {
		newTc := descs.NewCollection(leaseMgr, evalCtx.Settings)
		// pretend that the schema has been modified.
		if err := newTc.AddUncommittedTable(*tableDesc); err != nil {
			return err
		}

		ie.tcModifier = newTc
		defer func() {
			ie.tcModifier = nil
		}()
	}

	var fk *sqlbase.ForeignKeyConstraint
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

	return validateForeignKey(ctx, tableDesc.TableDesc(), fk, ie, txn, evalCtx.Codec)
}

// columnBackfillInTxn backfills columns for all mutation columns in
// the mutation list.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func columnBackfillInTxn(
	ctx context.Context,
	txn *kv.Txn,
	tc *descs.Collection,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	traceKV bool,
) error {
	// A column backfill in the ADD state is a noop.
	if tableDesc.Adding() {
		return nil
	}
	var backfiller backfill.ColumnBackfiller
	if err := backfiller.Init(ctx, evalCtx, tableDesc); err != nil {
		return err
	}
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
	tableDesc *sqlbase.ImmutableTableDescriptor,
	traceKV bool,
) error {
	var backfiller backfill.IndexBackfiller
	if err := backfiller.Init(evalCtx, tableDesc); err != nil {
		return err
	}
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
	tableDesc *sqlbase.ImmutableTableDescriptor,
	idx *sqlbase.IndexDescriptor,
	traceKV bool,
) error {
	alloc := &sqlbase.DatumAlloc{}
	var sp roachpb.Span
	for done := false; !done; done = sp.Key == nil {
		rd, err := row.MakeDeleter(
			ctx, txn, execCfg.Codec, tableDesc, nil, alloc,
		)
		if err != nil {
			return err
		}
		td := tableDeleter{rd: rd, alloc: alloc}
		if err := td.init(ctx, txn, evalCtx); err != nil {
			return err
		}
		sp, err = td.deleteIndex(
			ctx, idx, sp, indexTruncateChunkSize, traceKV,
		)
		if err != nil {
			return err
		}
	}
	// Remove index zone configs.
	return RemoveIndexZoneConfigs(ctx, txn, execCfg, tableDesc.ID, []sqlbase.IndexDescriptor{*idx})
}
