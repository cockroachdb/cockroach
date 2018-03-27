// Copyright 2015 The Cockroach Authors.
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

package sql

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var (
	// SchemaChangeLeaseDuration is the duration a lease will be acquired for.
	// Exported for testing purposes only.
	SchemaChangeLeaseDuration = 5 * time.Minute
	// MinSchemaChangeLeaseDuration is the minimum duration a lease will have
	// remaining upon acquisition. Exported for testing purposes only.
	MinSchemaChangeLeaseDuration = time.Minute
)

// This is a delay [0.9 * asyncSchemaChangeDelay, 1.1 * asyncSchemaChangeDelay)
// added to an attempt to run a schema change via the asynchronous path.
// This delay allows the synchronous path to execute the schema change
// in all likelihood. We'd like the synchronous path to execute
// the schema change so that it doesn't have to poll and wait for
// another node to execute the schema change. Polling can add a polling
// delay to the normal execution of a schema change. This interval is also
// used to reattempt execution of a schema change. We don't want this to
// be too low because once a node has started executing a schema change
// the other nodes should not cause a storm by rapidly try to grab the
// schema change lease.
//
// TODO(mjibson): Refine the job coordinator to elect a new job coordinator
// on coordinator failure without causing a storm of polling requests
// attempting to become the job coordinator.
const asyncSchemaChangeDelay = 30 * time.Second

// This is the delay [0.9 * asyncSchemaChangeGCDelay, 1.1 * asyncSchemaChangeGCDelay)
// added to an attempt to run a schema change via the asynchronous path
// for GC-ing of dropped schema change data.
const asyncSchemaChangeGCDelay = 10 * time.Minute

// SchemaChanger is used to change the schema on a table.
type SchemaChanger struct {
	tableID    sqlbase.ID
	mutationID sqlbase.MutationID
	nodeID     roachpb.NodeID
	db         *client.DB
	leaseMgr   *LeaseManager
	// The SchemaChangeManager can attempt to execute this schema
	// changer after this time.
	execAfter time.Time
	// This schema change is responsible for GC-ing drop schema change data.
	forGC          bool
	readAsOf       hlc.Timestamp
	testingKnobs   *SchemaChangerTestingKnobs
	distSQLPlanner *DistSQLPlanner
	jobRegistry    *jobs.Registry
	job            *jobs.Job
	// Caches updated by DistSQL.
	rangeDescriptorCache *kv.RangeDescriptorCache
	leaseHolderCache     *kv.LeaseHolderCache
	clock                *hlc.Clock
	settings             *cluster.Settings
	execCfg              *ExecutorConfig
}

// NewSchemaChangerForTesting only for tests.
func NewSchemaChangerForTesting(
	tableID sqlbase.ID,
	mutationID sqlbase.MutationID,
	nodeID roachpb.NodeID,
	db client.DB,
	leaseMgr *LeaseManager,
	jobRegistry *jobs.Registry,
	execCfg *ExecutorConfig,
) SchemaChanger {
	return SchemaChanger{
		tableID:     tableID,
		mutationID:  mutationID,
		nodeID:      nodeID,
		db:          &db,
		leaseMgr:    leaseMgr,
		jobRegistry: jobRegistry,
		settings:    cluster.MakeTestingClusterSettings(),
		execCfg:     execCfg,
	}
}

func (sc *SchemaChanger) createSchemaChangeLease() sqlbase.TableDescriptor_SchemaChangeLease {
	return sqlbase.TableDescriptor_SchemaChangeLease{
		NodeID: sc.nodeID, ExpirationTime: timeutil.Now().Add(SchemaChangeLeaseDuration).UnixNano()}
}

var errExistingSchemaChangeLease = errors.New(
	"an outstanding schema change lease exists")
var errSchemaChangeNotFirstInLine = errors.New(
	"schema change not first in line")
var errNotHitGCTTLDeadline = errors.New(
	"not hit gc ttl deadline")

func shouldLogSchemaChangeError(err error) bool {
	return err != errExistingSchemaChangeLease &&
		err != errSchemaChangeNotFirstInLine &&
		err != errNotHitGCTTLDeadline
}

// AcquireLease acquires a schema change lease on the table if
// an unexpired lease doesn't exist. It returns the lease.
func (sc *SchemaChanger) AcquireLease(
	ctx context.Context, tables *TableCollection,
) (sqlbase.TableDescriptor_SchemaChangeLease, error) {
	var lease sqlbase.TableDescriptor_SchemaChangeLease
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}

		// A second to deal with the time uncertainty across nodes.
		// It is perfectly valid for two or more goroutines to hold a valid
		// lease and execute a schema change in parallel, because schema
		// changes are executed using transactions that run sequentially.
		// This just reduces the probability of a write collision.
		expirationTimeUncertainty := time.Second

		if tableDesc.Lease != nil {
			if timeutil.Unix(0, tableDesc.Lease.ExpirationTime).Add(expirationTimeUncertainty).After(timeutil.Now()) {
				return errExistingSchemaChangeLease
			}
			log.Infof(ctx, "Overriding existing expired lease %v", tableDesc.Lease)
		}
		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		if tables != nil {
			// tables is nil when running schema changes from the background schema change task.
			// When running schema changes at the end of a SQL txn, tables is the
			// table collection used as descriptor cache by queries. That must
			// remain consistent with the KV state, so we must update it here.
			tables.addUncommittedTable(*tableDesc)
		}
		return txn.Put(ctx, sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc))
	})
	return lease, err
}

func (sc *SchemaChanger) findTableWithLease(
	ctx context.Context, txn *client.Txn, lease sqlbase.TableDescriptor_SchemaChangeLease,
) (*sqlbase.TableDescriptor, error) {
	tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
	if err != nil {
		return nil, err
	}
	if tableDesc.Lease == nil {
		return nil, errors.Errorf("no lease present for tableID: %d", sc.tableID)
	}
	if *tableDesc.Lease != lease {
		return nil, errors.Errorf("table: %d has lease: %v, expected: %v", sc.tableID, tableDesc.Lease, lease)
	}
	return tableDesc, nil
}

// ReleaseLease releases the table lease if it is the one registered with
// the table descriptor.
func (sc *SchemaChanger) ReleaseLease(
	ctx context.Context, lease sqlbase.TableDescriptor_SchemaChangeLease, tables *TableCollection,
) error {
	return sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		tableDesc, err := sc.findTableWithLease(ctx, txn, lease)
		if err != nil {
			return err
		}
		tableDesc.Lease = nil
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		if tables != nil {
			// tables is nil when running schema changes from the background schema change task.
			// When running schema changes at the end of a SQL txn, tables is the
			// table collection used as descriptor cache by queries. That must
			// remain consistent with the KV state, so we must update it here.
			tables.addUncommittedTable(*tableDesc)
		}
		return txn.Put(ctx, sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc))
	})
}

// ExtendLease for the current leaser. This needs to be called often while
// doing a schema change to prevent more than one node attempting to apply a
// schema change (which is still safe, but unwise). It updates existingLease
// with the new lease.
func (sc *SchemaChanger) ExtendLease(
	ctx context.Context, existingLease *sqlbase.TableDescriptor_SchemaChangeLease,
) error {
	// Check if there is still time on this lease.
	minDesiredExpiration := timeutil.Now().Add(MinSchemaChangeLeaseDuration)
	if timeutil.Unix(0, existingLease.ExpirationTime).After(minDesiredExpiration) {
		return nil
	}
	// Update lease.
	var lease sqlbase.TableDescriptor_SchemaChangeLease
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		tableDesc, err := sc.findTableWithLease(ctx, txn, *existingLease)
		if err != nil {
			return err
		}

		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		return txn.Put(ctx, sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc))
	}); err != nil {
		return err
	}
	*existingLease = lease
	return nil
}

// DropTableDesc removes a descriptor from the KV database.
func DropTableDesc(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, db *client.DB, traceKV bool,
) error {
	descKey := sqlbase.MakeDescMetadataKey(tableDesc.ID)
	zoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(tableDesc.ID))

	// Finished deleting all the table data, now delete the table meta data.
	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Delete table descriptor
		b := &client.Batch{}
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", descKey)
			log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
		}
		// Delete the descriptor.
		b.Del(descKey)
		// Delete the zone config entry for this table.
		b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
}

// maybe Add/Drop a table depending on the state of a table descriptor.
// This method returns true if the table is deleted.
func (sc *SchemaChanger) maybeAddDrop(
	ctx context.Context,
	inSession bool,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	table *sqlbase.TableDescriptor,
) (bool, error) {
	if table.Dropped() {
		if err := sc.ExtendLease(ctx, lease); err != nil {
			return false, err
		}

		if inSession {
			return false, nil
		}

		// This can happen if a change other than the drop originally
		// scheduled the changer for this table. If that's the case,
		// we still need to wait for the deadline to expire.
		if table.DropTime != 0 {
			var timeRemaining time.Duration
			if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				timeRemaining = 0
				_, zoneCfg, _, err := GetZoneConfigInTxn(
					ctx, txn, uint32(table.ID), &sqlbase.IndexDescriptor{}, "",
				)
				if err != nil {
					return err
				}
				deadline := table.DropTime + int64(zoneCfg.GC.TTLSeconds)*time.Second.Nanoseconds()
				timeRemaining = timeutil.Since(timeutil.Unix(0, deadline))
				return nil
			}); err != nil {
				return false, err
			}
			if timeRemaining < 0 {
				return false, errNotHitGCTTLDeadline
			}
		}
		// Do all the hard work of deleting the table data and the table ID.
		if err := truncateTableInChunks(ctx, table, sc.db, false /* traceKV */); err != nil {
			return false, err
		}

		return true, DropTableDesc(ctx, table, sc.db, false /* traceKV */)
	}

	if table.Adding() {
		for _, idx := range table.AllNonDropIndexes() {
			if idx.ForeignKey.IsSet() {
				if err := sc.waitToUpdateLeases(ctx, idx.ForeignKey.Table); err != nil {
					return false, err
				}
			}
		}

		if _, err := sc.leaseMgr.Publish(
			ctx,
			table.ID,
			func(tbl *sqlbase.TableDescriptor) error {
				tbl.State = sqlbase.TableDescriptor_PUBLIC
				return nil
			},
			func(txn *client.Txn) error { return nil },
		); err != nil {
			return false, err
		}
	}

	return false, nil
}

// Drain old names from the cluster.
func (sc *SchemaChanger) drainNames(
	ctx context.Context, lease *sqlbase.TableDescriptor_SchemaChangeLease,
) error {
	if err := sc.ExtendLease(ctx, lease); err != nil {
		return err
	}

	// Publish a new version with all the names drained after everyone
	// has seen the version with the new name. All the draining names
	// can be reused henceforth.
	var namesToReclaim []sqlbase.TableDescriptor_NameInfo
	_, err := sc.leaseMgr.Publish(
		ctx,
		sc.tableID,
		func(desc *sqlbase.TableDescriptor) error {
			if sc.testingKnobs.OldNamesDrainedNotification != nil {
				sc.testingKnobs.OldNamesDrainedNotification()
			}
			// Check that another schema change didn't run during the
			// drain phase. This ensures that we don't reclaim old names
			// here without explicitly going through a drain phase for the
			// names. On seeing a need to increment the version return an
			// error, so that sc.exec() is reexecuted and increments the
			// version before coming back here to correctly drain the names.
			if desc.UpVersion {
				return errors.Errorf("a schema change ran during the drain phase, re-increment")
			}
			// Free up the old name(s) for reuse.
			namesToReclaim = desc.DrainingNames
			desc.DrainingNames = nil
			return nil
		},
		// Reclaim all the old names.
		func(txn *client.Txn) error {
			b := txn.NewBatch()
			for _, drain := range namesToReclaim {
				tbKey := tableKey{drain.ParentID, drain.Name}.Key()
				b.Del(tbKey)
			}
			return txn.Run(ctx, b)
		},
	)
	return err
}

// Execute the entire schema change in steps.
// inSession is set to false when this is called from the asynchronous
// schema change execution path.
//
// If the txn that queued the schema changer did not commit, this will be a
// no-op, as we'll fail to find the job for our mutation in the jobs registry.
func (sc *SchemaChanger) exec(
	ctx context.Context, inSession bool, evalCtx *extendedEvalContext,
) error {
	// Acquire lease.
	lease, err := sc.AcquireLease(ctx, evalCtx.Tables)
	if err != nil {
		return err
	}
	needRelease := true
	// Always try to release lease.
	defer func() {
		// If the schema changer deleted the descriptor, there's no longer a lease to be
		// released.
		if !needRelease {
			return
		}
		if err := sc.ReleaseLease(ctx, lease, evalCtx.Tables); err != nil {
			log.Warning(ctx, err)
		}
	}()

	notFirst, err := sc.notFirstInLine(ctx)
	if err != nil {
		return err
	}
	if notFirst {
		return errSchemaChangeNotFirstInLine
	}

	// Increment the version and unset tableDescriptor.UpVersion.
	desc, err := sc.MaybeIncrementVersion(ctx)
	if err != nil {
		return err
	}

	tableDesc := desc.GetTable()
	if tableDesc.HasDrainingNames() {
		if err := sc.drainNames(ctx, &lease); err != nil {
			return err
		}
	}

	if drop, err := sc.maybeAddDrop(ctx, inSession, &lease, tableDesc); err != nil {
		return err
	} else if drop {
		needRelease = false
		return nil
	}

	// Wait for the schema change to propagate to all nodes after this function
	// returns, so that the new schema is live everywhere. This is not needed for
	// correctness but is done to make the UI experience/tests predictable.
	defer func() {
		if err := sc.waitToUpdateLeases(ctx, sc.tableID); err != nil {
			log.Warning(ctx, err)
		}
	}()

	if sc.mutationID == sqlbase.InvalidMutationID {
		// Nothing more to do.
		return nil
	}

	// Find our job.
	foundJobID := false
	for _, g := range tableDesc.MutationJobs {
		if g.MutationID == sc.mutationID {
			job, err := sc.jobRegistry.LoadJob(ctx, g.JobID)
			if err != nil {
				return err
			}
			sc.job = job
			foundJobID = true
			break
		}
	}
	if !foundJobID {
		// No job means we've already run and completed this schema change
		// successfully, so we can just exit.
		return nil
	}

	if err := sc.job.Started(ctx); err != nil {
		if log.V(2) {
			log.Infof(ctx, "Failed to mark job %d as started: %v", *sc.job.ID(), err)
		}
	}

	// Another transaction might set the up_version bit again,
	// but we're no longer responsible for taking care of that.

	// Run through mutation state machine and backfill.
	err = sc.runStateMachineAndBackfill(ctx, &lease, evalCtx)

	// Purge the mutations if the application of the mutations failed due to
	// a permanent error. All other errors are transient errors that are
	// resolved by retrying the backfill.
	if sqlbase.IsPermanentSchemaChangeError(err) {
		if err := sc.rollbackSchemaChange(ctx, err, &lease, evalCtx); err != nil {
			return err
		}
	}

	return err
}

func (sc *SchemaChanger) rollbackSchemaChange(
	ctx context.Context,
	err error,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	evalCtx *extendedEvalContext,
) error {
	log.Warningf(ctx, "reversing schema change %d due to irrecoverable error: %s", *sc.job.ID(), err)
	if err := sc.job.Failed(ctx, err, jobs.NoopFn); err != nil {
		log.Warningf(ctx, "schema change ignoring error while marking job %d as failed: %+v",
			*sc.job.ID(), err)
	}

	if errReverse := sc.reverseMutations(ctx, err); errReverse != nil {
		// Although the backfill did hit an integrity constraint violation
		// and made a decision to reverse the mutations,
		// reverseMutations() failed. If exec() is called again the entire
		// schema change will be retried.
		return errReverse
	}

	// After this point the schema change has been reversed and any retry
	// of the schema change will act upon the reversed schema change.
	if errPurge := sc.runStateMachineAndBackfill(ctx, lease, evalCtx); errPurge != nil {
		// Don't return this error because we do want the caller to know
		// that an integrity constraint was violated with the original
		// schema change. The reversed schema change will be
		// retried via the async schema change manager.
		log.Warningf(ctx, "error purging mutation: %s, after error: %s", errPurge, err)
	}
	return nil
}

// MaybeIncrementVersion increments the version if needed.
// If the version is to be incremented, it also assures that all nodes are on
// the current (pre-increment) version of the descriptor.
// Returns the (potentially updated) descriptor.
func (sc *SchemaChanger) MaybeIncrementVersion(ctx context.Context) (*sqlbase.Descriptor, error) {
	return sc.leaseMgr.Publish(ctx, sc.tableID, func(desc *sqlbase.TableDescriptor) error {
		if !desc.UpVersion {
			// Return error so that Publish() doesn't increment the version.
			return errDidntUpdateDescriptor
		}
		desc.UpVersion = false
		// Publish() will increment the version.
		return nil
	}, nil)
}

// RunStateMachineBeforeBackfill moves the state machine forward
// and wait to ensure that all nodes are seeing the latest version
// of the table.
func (sc *SchemaChanger) RunStateMachineBeforeBackfill(ctx context.Context) error {
	if _, err := sc.leaseMgr.Publish(ctx, sc.tableID, func(desc *sqlbase.TableDescriptor) error {
		var modified bool
		// Apply mutations belonging to the same version.
		for i, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			switch mutation.Direction {
			case sqlbase.DescriptorMutation_ADD:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					// TODO(vivek): while moving up the state is appropriate,
					// it will be better to run the backfill of a unique index
					// twice: once in the DELETE_ONLY state to confirm that
					// the index can indeed be created, and subsequently in the
					// DELETE_AND_WRITE_ONLY state to fill in the missing elements of the
					// index (INSERT and UPDATE that happened in the interim).
					desc.Mutations[i].State = sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY
					modified = true

				case sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					// The state change has already moved forward.
				}

			case sqlbase.DescriptorMutation_DROP:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					// The state change has already moved forward.

				case sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					desc.Mutations[i].State = sqlbase.DescriptorMutation_DELETE_ONLY
					modified = true
				}
			}
		}
		if !modified {
			// Return error so that Publish() doesn't increment the version.
			return errDidntUpdateDescriptor
		}
		return nil
	}, nil); err != nil {
		return err
	}
	// wait for the state change to propagate to all leases.
	return sc.waitToUpdateLeases(ctx, sc.tableID)
}

// Wait until the entire cluster has been updated to the latest version
// of the table descriptor.
func (sc *SchemaChanger) waitToUpdateLeases(ctx context.Context, tableID sqlbase.ID) error {
	// Aggressively retry because there might be a user waiting for the
	// schema change to complete.
	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	if log.V(2) {
		log.Infof(ctx, "waiting for a single version of table %d...", tableID)
	}
	_, err := sc.leaseMgr.WaitForOneVersion(ctx, tableID, retryOpts)
	if log.V(2) {
		log.Infof(ctx, "waiting for a single version of table %d... done", tableID)
	}
	return err
}

// done finalizes the mutations (adds new cols/indexes to the table).
// It ensures that all nodes are on the current (pre-update) version of the
// schema.
// Returns the updated of the descriptor.
func (sc *SchemaChanger) done(ctx context.Context) (*sqlbase.Descriptor, error) {
	isRollback := false
	return sc.leaseMgr.Publish(ctx, sc.tableID, func(desc *sqlbase.TableDescriptor) error {
		i := 0
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			isRollback = mutation.Rollback
			desc.MakeMutationComplete(mutation)
			i++
		}
		if i == 0 {
			// The table descriptor is unchanged. Don't let Publish() increment
			// the version.
			return errDidntUpdateDescriptor
		}
		// Trim the executed mutations from the descriptor.
		desc.Mutations = desc.Mutations[i:]

		for i, g := range desc.MutationJobs {
			if g.MutationID == sc.mutationID {
				// Trim the executed mutation group from the descriptor.
				desc.MutationJobs = append(desc.MutationJobs[:i], desc.MutationJobs[i+1:]...)
				break
			}
		}
		return nil
	}, func(txn *client.Txn) error {
		if err := sc.job.WithTxn(txn).Succeeded(ctx, jobs.NoopFn); err != nil {
			return errors.Wrapf(err, "failed to mark job %d as as successful", *sc.job.ID())
		}

		schemaChangeEventType := EventLogFinishSchemaChange
		if isRollback {
			schemaChangeEventType = EventLogFinishSchemaRollback
		}

		// Log "Finish Schema Change" or "Finish Schema Change Rollback"
		// event. Only the table ID and mutation ID are logged; this can
		// be correlated with the DDL statement that initiated the change
		// using the mutation id.
		return MakeEventLogger(sc.execCfg).InsertEventRecord(
			ctx,
			txn,
			schemaChangeEventType,
			int32(sc.tableID),
			int32(sc.nodeID),
			struct {
				MutationID uint32
			}{uint32(sc.mutationID)},
		)
	})
}

// notFirstInLine returns true whenever the schema change has been queued
// up for execution after another schema change.
func (sc *SchemaChanger) notFirstInLine(ctx context.Context) (bool, error) {
	var notFirst bool
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		notFirst = false
		desc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}
		for i, mutation := range desc.Mutations {
			if mutation.MutationID == sc.mutationID {
				notFirst = i != 0
				break
			}
		}
		return nil
	})
	return notFirst, err
}

// runStateMachineAndBackfill runs the schema change state machine followed by
// the backfill.
func (sc *SchemaChanger) runStateMachineAndBackfill(
	ctx context.Context,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	evalCtx *extendedEvalContext,
) error {
	if fn := sc.testingKnobs.RunBeforePublishWriteAndDelete; fn != nil {
		fn()
	}
	// Run through mutation state machine before backfill.
	if err := sc.RunStateMachineBeforeBackfill(ctx); err != nil {
		return err
	}

	if err := sc.job.Progressed(ctx, jobs.FractionUpdater(.1)); err != nil {
		log.Warningf(ctx, "failed to log progress on job %v after completing state machine: %v",
			*sc.job.ID(), err)
	}

	// Run backfill(s).
	if err := sc.runBackfill(ctx, lease, evalCtx); err != nil {
		return err
	}

	// Mark the mutations as completed.
	_, err := sc.done(ctx)
	return err
}

// reverseMutations reverses the direction of all the mutations with the
// mutationID. This is called after hitting an irrecoverable error while
// applying a schema change. If a column being added is reversed and dropped,
// all new indexes referencing the column will also be dropped.
func (sc *SchemaChanger) reverseMutations(ctx context.Context, causingError error) error {
	// Reverse the flow of the state machine.
	_, err := sc.leaseMgr.Publish(ctx, sc.tableID, func(desc *sqlbase.TableDescriptor) error {
		// Keep track of the column mutations being reversed so that indexes
		// referencing them can be dropped.
		columns := make(map[string]struct{})

		for i, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Only reverse the first set of mutations if they have the
				// mutation ID we're looking for.
				break
			}

			if mutation.Rollback {
				// Can actually never happen. This prevents a rollback of
				// an already rolled back mutation.
				return errors.Errorf("mutation already rolled back: %v", mutation)
			}

			jobID, err := sc.getJobIDForMutationWithDescriptor(ctx, desc, mutation.MutationID)
			if err != nil {
				return err
			}
			job, err := sc.jobRegistry.LoadJob(ctx, jobID)
			if err != nil {
				return err
			}

			details, ok := job.Record.Details.(jobs.SchemaChangeDetails)
			if !ok {
				return errors.Errorf("expected SchemaChangeDetails job type, got %T", sc.job.Record.Details)
			}
			details.ResumeSpanList[i].ResumeSpans = nil
			err = job.SetDetails(ctx, details)
			if err != nil {
				return err
			}

			log.Warningf(ctx, "reverse schema change mutation: %+v", mutation)
			switch mutation.Direction {
			case sqlbase.DescriptorMutation_ADD:
				desc.Mutations[i].Direction = sqlbase.DescriptorMutation_DROP
				// A column ADD being reversed gets placed in the map.
				if col := mutation.GetColumn(); col != nil {
					columns[col.Name] = struct{}{}
				}

			case sqlbase.DescriptorMutation_DROP:
				desc.Mutations[i].Direction = sqlbase.DescriptorMutation_ADD
			}
			desc.Mutations[i].Rollback = true
		}

		for i := range desc.MutationJobs {
			if desc.MutationJobs[i].MutationID == sc.mutationID {
				// Create a roll back job.
				record := sc.job.Record
				record.Description = "ROLL BACK " + record.Description
				job := sc.jobRegistry.NewJob(record)
				if err := job.Created(ctx); err != nil {
					return err
				}
				if err := job.Started(ctx); err != nil {
					return err
				}
				desc.MutationJobs[i].JobID = *job.ID()
				sc.job = job
				break
			}
		}

		// Delete index mutations that reference any of the reversed columns.
		if len(columns) > 0 {
			sc.deleteIndexMutationsWithReversedColumns(ctx, desc, columns)
		}

		// Publish() will increment the version.
		return nil
	}, func(txn *client.Txn) error {
		// Log "Reverse Schema Change" event. Only the causing error and the
		// mutation ID are logged; this can be correlated with the DDL statement
		// that initiated the change using the mutation id.
		return MakeEventLogger(sc.execCfg).InsertEventRecord(
			ctx,
			txn,
			EventLogReverseSchemaChange,
			int32(sc.tableID),
			int32(sc.nodeID),
			struct {
				Error      string
				MutationID uint32
			}{fmt.Sprintf("%+v", causingError), uint32(sc.mutationID)},
		)
	})
	return err
}

// deleteIndexMutationsWithReversedColumns deletes index mutations with a
// different mutationID than the schema changer and a reference to one of the
// reversed columns.
func (sc *SchemaChanger) deleteIndexMutationsWithReversedColumns(
	ctx context.Context, desc *sqlbase.TableDescriptor, columns map[string]struct{},
) {
	newMutations := make([]sqlbase.DescriptorMutation, 0, len(desc.Mutations))
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != sc.mutationID {
			if idx := mutation.GetIndex(); idx != nil {
				deleteMutation := false
				for _, name := range idx.ColumnNames {
					if _, ok := columns[name]; ok {
						// Such an index mutation has to be with direction ADD and
						// in the DELETE_ONLY state. Live indexes referencing live
						// columns cannot be deleted and thus never have direction
						// DROP. All mutations with the ADD direction start off in
						// the DELETE_ONLY state.
						if mutation.Direction != sqlbase.DescriptorMutation_ADD ||
							mutation.State != sqlbase.DescriptorMutation_DELETE_ONLY {
							panic(fmt.Sprintf("mutation in bad state: %+v", mutation))
						}
						log.Warningf(ctx, "delete schema change mutation: %+v", mutation)
						deleteMutation = true
						break
					}
				}
				if deleteMutation {
					continue
				}
			}
		}
		newMutations = append(newMutations, mutation)
	}
	// Reset mutations.
	desc.Mutations = newMutations
}

// TestingSchemaChangerCollection is an exported (for testing) version of
// schemaChangerCollection.
// TODO(andrei): get rid of this type once we can have tests internal to the sql
// package (as of April 2016 we can't because sql can't import server).
type TestingSchemaChangerCollection struct {
	scc *schemaChangerCollection
}

// ClearSchemaChangers clears the schema changers from the collection.
// If this is called from a SyncSchemaChangersFilter, no schema changer will be
// run.
func (tscc TestingSchemaChangerCollection) ClearSchemaChangers() {
	tscc.scc.schemaChangers = tscc.scc.schemaChangers[:0]
}

// SyncSchemaChangersFilter is the type of a hook to be installed through the
// ExecutorContext for blocking or otherwise manipulating schema changers run
// through the sync schema changers path.
type SyncSchemaChangersFilter func(TestingSchemaChangerCollection)

// SchemaChangerTestingKnobs for testing the schema change execution path
// through both the synchronous and asynchronous paths.
type SchemaChangerTestingKnobs struct {
	// SyncFilter is called before running schema changers synchronously (at
	// the end of a txn). The function can be used to clear the schema
	// changers (if the test doesn't want them run using the synchronous path)
	// or to temporarily block execution. Note that this has nothing to do
	// with the async path for running schema changers. To block that, set
	// AsyncExecNotification.
	SyncFilter SyncSchemaChangersFilter

	// RunBeforePublishWriteAndDelete is called just before publishing the
	// write+delete state for the schema change.
	RunBeforePublishWriteAndDelete func()

	// RunBeforeBackfill is called just before starting the backfill.
	RunBeforeBackfill func() error

	// RunBeforeBackfill is called just before starting the index backfill, after
	// fixing the index backfill scan timestamp.
	RunBeforeIndexBackfill func()

	// RunBeforeBackfillChunk is called before executing each chunk of a
	// backfill during a schema change operation. It is called with the
	// current span and returns an error which eventually is returned to the
	// caller of SchemaChanger.exec(). It is called at the start of the
	// backfill function passed into the transaction executing the chunk.
	RunBeforeBackfillChunk func(sp roachpb.Span) error

	// RunAfterBackfillChunk is called after executing each chunk of a
	// backfill during a schema change operation. It is called just before
	// returning from the backfill function passed into the transaction
	// executing the chunk. It is always called even when the backfill
	// function returns an error, or if the table has already been dropped.
	RunAfterBackfillChunk func()

	// OldNamesDrainedNotification is called during a schema change,
	// after all leases on the version of the descriptor with the old
	// names are gone, and just before the mapping of the old names to the
	// descriptor id are about to be deleted.
	OldNamesDrainedNotification func()

	// AsyncExecNotification is a function called before running a schema
	// change asynchronously. Returning an error will prevent the asynchronous
	// execution path from running.
	AsyncExecNotification func() error

	// AsyncExecQuickly executes queued schema changes as soon as possible.
	AsyncExecQuickly bool

	// WriteCheckpointInterval is the interval after which a checkpoint is
	// written.
	WriteCheckpointInterval time.Duration

	// BackfillChunkSize is to be used for all backfill chunked operations.
	BackfillChunkSize int64
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*SchemaChangerTestingKnobs) ModuleTestingKnobs() {}

// SchemaChangeManager processes pending schema changes seen in gossip
// updates. Most schema changes are executed synchronously by the node
// that created the schema change. If the node dies while
// processing the schema change this manager acts as a backup
// execution mechanism.
type SchemaChangeManager struct {
	ambientCtx   log.AmbientContext
	execCfg      *ExecutorConfig
	testingKnobs *SchemaChangerTestingKnobs
	// Create a schema changer for every outstanding schema change seen.
	schemaChangers map[sqlbase.ID]SchemaChanger
	distSQLPlanner *DistSQLPlanner
}

// NewSchemaChangeManager returns a new SchemaChangeManager.
func NewSchemaChangeManager(
	ambientCtx log.AmbientContext,
	execCfg *ExecutorConfig,
	testingKnobs *SchemaChangerTestingKnobs,
	db client.DB,
	nodeDesc roachpb.NodeDescriptor,
	dsp *DistSQLPlanner,
) *SchemaChangeManager {
	return &SchemaChangeManager{
		ambientCtx:     ambientCtx,
		execCfg:        execCfg,
		testingKnobs:   testingKnobs,
		schemaChangers: make(map[sqlbase.ID]SchemaChanger),
		distSQLPlanner: dsp,
	}
}

// Creates a timer that is used by the manager to decide on
// when to run the next schema changer.
func (s *SchemaChangeManager) newTimer() *time.Timer {
	waitDuration := time.Duration(math.MaxInt64)
	now := timeutil.Now()
	for _, sc := range s.schemaChangers {
		d := sc.execAfter.Sub(now)
		if d < waitDuration {
			waitDuration = d
		}
	}
	// Create a timer if there is an existing schema changer.
	if len(s.schemaChangers) > 0 {
		return time.NewTimer(waitDuration)
	}
	return &time.Timer{}
}

// Start starts a goroutine that runs outstanding schema changes
// for tables received in the latest system configuration via gossip.
func (s *SchemaChangeManager) Start(stopper *stop.Stopper) {
	stopper.RunWorker(s.ambientCtx.AnnotateCtx(context.Background()), func(ctx context.Context) {
		descKeyPrefix := keys.MakeTablePrefix(uint32(sqlbase.DescriptorTable.ID))
		cfgFilter := gossip.MakeSystemConfigDeltaFilter(descKeyPrefix)
		gossipUpdateC := s.execCfg.Gossip.RegisterSystemConfigChannel()
		timer := &time.Timer{}
		for {
			// A jitter is added to reduce contention between nodes
			// attempting to run the schema change.
			delay := time.Duration(float64(asyncSchemaChangeDelay) * (0.9 + 0.2*rand.Float64()))
			gcDelay := time.Duration(float64(asyncSchemaChangeGCDelay) * (0.9 + 0.2*rand.Float64()))
			if s.testingKnobs.AsyncExecQuickly {
				delay = 20 * time.Millisecond
				gcDelay = 20 * time.Millisecond
			}
			select {
			case <-gossipUpdateC:
				cfg, _ := s.execCfg.Gossip.GetSystemConfig()
				// Read all tables and their versions
				if log.V(2) {
					log.Info(ctx, "received a new config")
				}
				schemaChanger := SchemaChanger{
					execCfg:              s.execCfg,
					nodeID:               s.execCfg.NodeID.Get(),
					db:                   s.execCfg.DB,
					leaseMgr:             s.execCfg.LeaseManager,
					testingKnobs:         s.testingKnobs,
					distSQLPlanner:       s.distSQLPlanner,
					jobRegistry:          s.execCfg.JobRegistry,
					leaseHolderCache:     s.execCfg.LeaseHolderCache,
					rangeDescriptorCache: s.execCfg.RangeDescriptorCache,
					clock:                s.execCfg.Clock,
					settings:             s.execCfg.Settings,
				}

				execAfter := timeutil.Now().Add(delay)
				execGCAfter := timeutil.Now().Add(gcDelay)
				resetTimer := false
				cfgFilter.ForModified(cfg, func(kv roachpb.KeyValue) {
					resetTimer = true
					// Attempt to unmarshal config into a table/database descriptor.
					var descriptor sqlbase.Descriptor
					if err := kv.Value.GetProto(&descriptor); err != nil {
						log.Warningf(ctx, "%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
						return
					}
					switch union := descriptor.Union.(type) {
					case *sqlbase.Descriptor_Table:
						table := union.Table
						table.MaybeFillInDescriptor()
						if err := table.ValidateTable(s.execCfg.Settings); err != nil {
							log.Errorf(ctx, "%s: received invalid table descriptor: %s. Desc: %v",
								kv.Key, err, table,
							)
							return
						}

						// Keep track of outstanding schema changes.
						// If all schema change commands always set UpVersion, why
						// check for the presence of mutations?
						// A schema change execution might fail soon after
						// unsetting UpVersion, and we still want to process
						// outstanding mutations. Similar with a table marked for deletion.
						if table.UpVersion || table.Dropped() || table.Adding() ||
							table.HasDrainingNames() || len(table.Mutations) > 0 {
							if log.V(2) {
								log.Infof(ctx, "%s: queue up pending schema change; table: %d, version: %d",
									kv.Key, table.ID, table.Version)
							}

							// Only track the first schema change. We depend on
							// gossip to renotify us when a schema change has been
							// completed.
							schemaChanger.tableID = table.ID
							schemaChanger.forGC = false
							if len(table.Mutations) == 0 || table.Dropped() {
								schemaChanger.mutationID = sqlbase.InvalidMutationID
							} else {
								schemaChanger.mutationID = table.Mutations[0].MutationID
							}
							schemaChanger.execAfter = execAfter
							// If the table is dropped and there's a DropTime set, use that
							// to generate an extended delay instead of the standard delay.
							if !table.UpVersion && table.Dropped() && table.DropTime != 0 {
								schemaChanger.forGC = true
								schemaChanger.execAfter = execGCAfter
							}
							// Keep track of this schema change.
							if sc, ok := s.schemaChangers[table.ID]; ok {
								if sc.mutationID == schemaChanger.mutationID {
									// Ignore duplicate.
									return
								}
							}
							s.schemaChangers[table.ID] = schemaChanger
						}

					case *sqlbase.Descriptor_Database:
						// Ignore.
					}
				})

				if resetTimer {
					timer = s.newTimer()
				}

			case <-timer.C:
				if s.testingKnobs.AsyncExecNotification != nil &&
					s.testingKnobs.AsyncExecNotification() != nil {
					timer = s.newTimer()
					continue
				}
				for tableID, sc := range s.schemaChangers {
					if timeutil.Since(sc.execAfter) > 0 {
						evalCtx := createSchemaChangeEvalCtx(s.execCfg.Clock.Now())

						execCtx, cleanup := tracing.EnsureContext(ctx, s.ambientCtx.Tracer, "schema change [async]")
						err := sc.exec(execCtx, false /* inSession */, &evalCtx)
						cleanup()

						// Advance the execAfter time so that this schema
						// changer doesn't get called again for a while.
						if sc.forGC {
							sc.execAfter = timeutil.Now().Add(gcDelay)
						} else {
							sc.execAfter = timeutil.Now().Add(delay)
						}
						s.schemaChangers[tableID] = sc

						if err != nil {
							if shouldLogSchemaChangeError(err) {
								log.Warningf(ctx, "Error executing schema change: %s", err)
							}
							if err == sqlbase.ErrDescriptorNotFound {
								// Someone deleted this table. Don't try to run the schema
								// changer again. Note that there's no gossip update for the
								// deletion which would remove this schemaChanger.
								delete(s.schemaChangers, tableID)
							}
						} else {
							// We successfully executed the schema change. Delete it.
							delete(s.schemaChangers, tableID)
						}

						// Only attempt to run one schema changer.
						break
					}
				}
				timer = s.newTimer()

			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// createSchemaChangeEvalCtx creates an extendedEvalContext() to be used for backfills.
//
// TODO(andrei): This EvalContext() will be broken for backfills trying to use
// functions marked with distsqlBlacklist.
func createSchemaChangeEvalCtx(ts hlc.Timestamp) extendedEvalContext {
	dummyLocation := time.UTC
	evalCtx := extendedEvalContext{
		EvalContext: tree.EvalContext{
			SessionData: &sessiondata.SessionData{
				SearchPath: sqlbase.DefaultSearchPath,
				Location:   dummyLocation,
				// The database is not supposed to be needed in schema changes, as there
				// shouldn't be unqualified identifiers in backfills, and the pure functions
				// that need it should have already been evaluated.
				//
				// TODO(andrei): find a way to assert that this field is indeed not used.
				// And in fact it is used by `current_schemas()`, which, although is a pure
				// function, takes arguments which might be impure (so it can't always be
				// pre-evaluated).
				Database:      "",
				SequenceState: sessiondata.NewSequenceState(),
			},
		},
	}
	// The backfill is going to use the current timestamp for the various
	// functions, like now(), that need it.  It's possible that the backfill has
	// been partially performed already by another SchemaChangeManager with
	// another timestamp.
	//
	// TODO(andrei): Figure out if this is what we want, and whether the
	// timestamp from the session that enqueued the schema change
	// is/should be used for impure functions like now().
	evalCtx.SetTxnTimestamp(timeutil.Unix(0 /* sec */, ts.WallTime))
	evalCtx.SetStmtTimestamp(timeutil.Unix(0 /* sec */, ts.WallTime))

	return evalCtx
}
