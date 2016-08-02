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
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/pkg/errors"
)

var (
	// SchemaChangeLeaseDuration is the duration a lease will be acquired for.
	// Exported for testing purposes only.
	SchemaChangeLeaseDuration = 5 * time.Minute
	// MinSchemaChangeLeaseDuration is the minimum duration a lease will have
	// remaining upon acquisition. Exported for testing purposes only.
	MinSchemaChangeLeaseDuration = time.Minute
)

// SchemaChanger is used to change the schema on a table.
type SchemaChanger struct {
	tableID    sqlbase.ID
	mutationID sqlbase.MutationID
	nodeID     roachpb.NodeID
	db         client.DB
	leaseMgr   *LeaseManager
	evalCtx    parser.EvalContext
	// The SchemaChangeManager can attempt to execute this schema
	// changer after this time.
	execAfter time.Time
}

func (sc *SchemaChanger) truncateAndDropTable(
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	tableDesc *sqlbase.TableDescriptor) error {

	l, err := sc.ExtendLease(*lease)
	if err != nil {
		return err
	}
	*lease = l
	return truncateAndDropTable(tableDesc, &sc.db)
}

// NewSchemaChangerForTesting only for tests.
func NewSchemaChangerForTesting(
	tableID sqlbase.ID,
	mutationID sqlbase.MutationID,
	nodeID roachpb.NodeID,
	db client.DB,
	leaseMgr *LeaseManager,
) SchemaChanger {
	return SchemaChanger{
		tableID:    tableID,
		mutationID: mutationID,
		nodeID:     nodeID,
		db:         db,
		leaseMgr:   leaseMgr}
}

func (sc *SchemaChanger) createSchemaChangeLease() sqlbase.TableDescriptor_SchemaChangeLease {
	return sqlbase.TableDescriptor_SchemaChangeLease{
		NodeID: sc.nodeID, ExpirationTime: timeutil.Now().Add(SchemaChangeLeaseDuration).UnixNano()}
}

var errExistingSchemaChangeLease = errors.New(
	"an outstanding schema change lease exists")

// AcquireLease acquires a schema change lease on the table if
// an unexpired lease doesn't exist. It returns the lease.
func (sc *SchemaChanger) AcquireLease() (sqlbase.TableDescriptor_SchemaChangeLease, error) {
	var lease sqlbase.TableDescriptor_SchemaChangeLease
	err := sc.db.Txn(func(txn *client.Txn) error {
		txn.SetSystemConfigTrigger()
		tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
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
			if time.Unix(0, tableDesc.Lease.ExpirationTime).Add(expirationTimeUncertainty).After(timeutil.Now()) {
				return errExistingSchemaChangeLease
			}
			log.Infof(txn.Context, "Overriding existing expired lease %v", tableDesc.Lease)
		}
		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		return txn.Put(sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc))
	})
	return lease, err
}

func (sc *SchemaChanger) findTableWithLease(
	txn *client.Txn, lease sqlbase.TableDescriptor_SchemaChangeLease,
) (*sqlbase.TableDescriptor, error) {
	tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
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

// ReleaseLease the table lease if it is the one registered with
// the table descriptor.
func (sc *SchemaChanger) ReleaseLease(lease sqlbase.TableDescriptor_SchemaChangeLease) error {
	err := sc.db.Txn(func(txn *client.Txn) error {
		tableDesc, err := sc.findTableWithLease(txn, lease)
		if err != nil {
			return err
		}
		tableDesc.Lease = nil
		txn.SetSystemConfigTrigger()
		return txn.Put(sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc))
	})
	return err
}

// ExtendLease for the current leaser. This needs to be called often while
// doing a schema change to prevent more than one node attempting to apply a
// schema change (which is still safe, but unwise).
func (sc *SchemaChanger) ExtendLease(
	existingLease sqlbase.TableDescriptor_SchemaChangeLease,
) (sqlbase.TableDescriptor_SchemaChangeLease, error) {
	// Check if there is still time on this lease.
	minDesiredExpiration := timeutil.Now().Add(MinSchemaChangeLeaseDuration)
	if time.Unix(0, existingLease.ExpirationTime).After(minDesiredExpiration) {
		return existingLease, nil
	}
	// Update lease.
	var lease sqlbase.TableDescriptor_SchemaChangeLease
	err := sc.db.Txn(func(txn *client.Txn) error {
		tableDesc, err := sc.findTableWithLease(txn, existingLease)
		if err != nil {
			return err
		}

		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		txn.SetSystemConfigTrigger()
		return txn.Put(sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc))
	})
	return lease, err
}

func isSchemaChangeRetryError(err error) bool {
	switch err {
	case sqlbase.ErrDescriptorNotFound:
		return false
	default:
		return !sqlbase.IsIntegrityConstraintError(err)
	}
}

// Execute the entire schema change in steps. startBackfillNotification is
// called before the backfill starts; it can be nil.
func (sc SchemaChanger) exec(
	startBackfillNotification func() error,
	oldNameNotInUseNotification func(),
) error {
	// Acquire lease.
	lease, err := sc.AcquireLease()
	if err != nil {
		return err
	}
	needRelease := true
	// Always try to release lease.
	defer func(l *sqlbase.TableDescriptor_SchemaChangeLease) {
		// If the schema changer deleted the descriptor, there's no longer a lease to be
		// released.
		if !needRelease {
			return
		}
		if err := sc.ReleaseLease(*l); err != nil {
			log.Warning(context.TODO(), err)
		}
	}(&lease)

	// Increment the version and unset tableDescriptor.UpVersion.
	desc, err := sc.MaybeIncrementVersion()
	if err != nil {
		return err
	}

	if desc.GetTable().Deleted() {
		lease, err = sc.ExtendLease(lease)
		if err != nil {
			return err
		}
		// Wait for everybody to see the version with the deleted bit set. When
		// this returns, nobody has any leases on the table, nor can get new leases,
		// so the table will no longer be modified.
		if err := sc.waitToUpdateLeases(); err != nil {
			return err
		}

		// Truncate the table and delete the descriptor.
		if err := sc.truncateAndDropTable(&lease, desc.GetTable()); err != nil {
			return err
		}
		needRelease = false
		return nil
	}

	if desc.GetTable().Renamed() {
		lease, err = sc.ExtendLease(lease)
		if err != nil {
			return err
		}
		// Wait for everyone to see the version with the new name. When this
		// returns, no new transactions will be using the old name for the table, so
		// the old name can now be re-used (by CREATE).
		if err := sc.waitToUpdateLeases(); err != nil {
			return err
		}

		if oldNameNotInUseNotification != nil {
			oldNameNotInUseNotification()
		}
		// Free up the old name(s).
		err := sc.db.Txn(func(txn *client.Txn) error {
			b := client.Batch{}
			for _, renameDetails := range desc.GetTable().Renames {
				tbKey := tableKey{renameDetails.OldParentID, renameDetails.OldName}.Key()
				b.Del(tbKey)
			}
			return txn.Run(&b)
		})
		if err != nil {
			return err
		}

		// Clean up - clear the descriptor's state.
		_, err = sc.leaseMgr.Publish(sc.tableID, func(desc *sqlbase.TableDescriptor) error {
			desc.Renames = nil
			return nil
		}, nil)
		if err != nil {
			return err
		}
	}

	// Wait for the schema change to propagate to all nodes after this function
	// returns, so that the new schema is live everywhere. This is not needed for
	// correctness but is done to make the UI experience/tests predictable.
	defer func() {
		if err := sc.waitToUpdateLeases(); err != nil {
			log.Warning(context.TODO(), err)
		}
	}()

	if sc.mutationID == sqlbase.InvalidMutationID {
		// Nothing more to do.
		return nil
	}

	// Another transaction might set the up_version bit again,
	// but we're no longer responsible for taking care of that.

	// Run through mutation state machine and backfill.
	err = sc.runStateMachineAndBackfill(&lease, startBackfillNotification)

	// Purge the mutations if the application of the mutations failed due to
	// an integrity constraint violation. All other errors are transient
	// errors that are resolved by retrying the backfill.
	if sqlbase.IsIntegrityConstraintError(err) {
		log.Warningf(context.TODO(), "reversing schema change due to irrecoverable error: %s", err)
		if errReverse := sc.reverseMutations(err); errReverse != nil {
			// Although the backfill did hit an integrity constraint violation
			// and made a decision to reverse the mutations,
			// reverseMutations() failed. If exec() is called again the entire
			// schema change will be retried.
			return errReverse
		}

		// After this point the schema change has been reversed and any retry
		// of the schema change will act upon the reversed schema change.
		if errPurge := sc.runStateMachineAndBackfill(
			&lease, startBackfillNotification,
		); errPurge != nil {
			// Don't return this error because we do want the caller to know
			// that an integrity constraint was violated with the original
			// schema change. The reversed schema change will be
			// retried via the async schema change manager.
			log.Warningf(context.TODO(), "error purging mutation: %s, after error: %s", errPurge, err)
		}
	}

	return err
}

// MaybeIncrementVersion increments the version if needed.
// If the version is to be incremented, it also assures that all nodes are on
// the current (pre-increment) version of the descriptor.
// Returns the (potentially updated) descriptor.
func (sc *SchemaChanger) MaybeIncrementVersion() (*sqlbase.Descriptor, error) {
	return sc.leaseMgr.Publish(sc.tableID, func(desc *sqlbase.TableDescriptor) error {
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
func (sc *SchemaChanger) RunStateMachineBeforeBackfill() error {
	if _, err := sc.leaseMgr.Publish(sc.tableID, func(desc *sqlbase.TableDescriptor) error {
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
					// WRITE_ONLY state to fill in the missing elements of the
					// index (INSERT and UPDATE that happened in the interim).
					desc.Mutations[i].State = sqlbase.DescriptorMutation_WRITE_ONLY
					modified = true

				case sqlbase.DescriptorMutation_WRITE_ONLY:
					// The state change has already moved forward.
				}

			case sqlbase.DescriptorMutation_DROP:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					// The state change has already moved forward.

				case sqlbase.DescriptorMutation_WRITE_ONLY:
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
	return sc.waitToUpdateLeases()
}

// Wait until the entire cluster has been updated to the latest version
// of the table descriptor.
func (sc *SchemaChanger) waitToUpdateLeases() error {
	// Aggressively retry because there might be a user waiting for the
	// schema change to complete.
	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	if log.V(2) {
		log.Infof(context.TODO(), "waiting for a single version of table %d...", sc.tableID)
	}
	_, err := sc.leaseMgr.waitForOneVersion(sc.tableID, retryOpts)
	if log.V(2) {
		log.Infof(context.TODO(), "waiting for a single version of table %d... done", sc.tableID)
	}
	return err
}

// done finalizes the mutations (adds new cols/indexes to the table).
// It ensures that all nodes are on the current (pre-update) version of the
// schema.
// Returns the updated of the descriptor.
func (sc *SchemaChanger) done() (*sqlbase.Descriptor, error) {
	return sc.leaseMgr.Publish(sc.tableID, func(desc *sqlbase.TableDescriptor) error {
		i := 0
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
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
		return nil
	}, func(txn *client.Txn) error {
		// Log "Finish Schema Change" event. Only the table ID and mutation ID
		// are logged; this can be correlated with the DDL statement that
		// initiated the change using the mutation id.
		return MakeEventLogger(sc.leaseMgr).InsertEventRecord(txn,
			EventLogFinishSchemaChange,
			int32(sc.tableID),
			int32(sc.evalCtx.NodeID),
			struct {
				MutationID uint32
			}{uint32(sc.mutationID)},
		)
	})
}

// runStateMachineAndBackfill runs the schema change state machine followed by
// the backfill.
func (sc *SchemaChanger) runStateMachineAndBackfill(
	lease *sqlbase.TableDescriptor_SchemaChangeLease, startBackfillNotification func() error,
) error {
	// Run through mutation state machine before backfill.
	if err := sc.RunStateMachineBeforeBackfill(); err != nil {
		return err
	}

	if startBackfillNotification != nil {
		if err := startBackfillNotification(); err != nil {
			return err
		}
	}

	// Run backfill.
	if err := sc.runBackfill(lease); err != nil {
		return err
	}

	// Mark the mutations as completed.
	_, err := sc.done()
	return err
}

// reverseMutations reverses the direction of all the mutations with the
// mutationID. This is called after hitting an irrecoverable error while
// applying a schema change. If a column being added is reversed and dropped,
// all new indexes referencing the column will also be dropped.
func (sc *SchemaChanger) reverseMutations(causingError error) error {
	// Reverse the flow of the state machine.
	_, err := sc.leaseMgr.Publish(sc.tableID, func(desc *sqlbase.TableDescriptor) error {
		// Keep track of the column mutations being reversed so that indexes
		// referencing them can be dropped.
		columns := make(map[string]struct{})

		for i, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Only reverse the first set of mutations if they have the
				// mutation ID we're looking for.
				break
			}
			log.Warningf(context.TODO(), "reverse schema change mutation: %+v", mutation)
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
		}

		// Delete index mutations that reference any of the reversed columns.
		if len(columns) > 0 {
			sc.deleteIndexMutationsWithReversedColumns(desc, columns)
		}

		// Publish() will increment the version.
		return nil
	}, func(txn *client.Txn) error {
		// Log "Reverse Schema Change" event. Only the causing error and the
		// mutation ID are logged; this can be correlated with the DDL statement
		// that initiated the change using the mutation id.
		return MakeEventLogger(sc.leaseMgr).InsertEventRecord(txn,
			EventLogReverseSchemaChange,
			int32(sc.tableID),
			int32(sc.evalCtx.NodeID),
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
	desc *sqlbase.TableDescriptor, columns map[string]struct{},
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
						log.Warningf(context.TODO(), "delete schema change mutation: %+v", mutation)
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

// IsDone returns true if the work scheduled for the schema changer
// is complete.
func (sc *SchemaChanger) IsDone() (bool, error) {
	var done bool
	err := sc.db.Txn(func(txn *client.Txn) error {
		done = true
		tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}
		if sc.mutationID == sqlbase.InvalidMutationID {
			if tableDesc.UpVersion {
				done = false
			}
		} else {
			for _, mutation := range tableDesc.Mutations {
				if mutation.MutationID == sc.mutationID {
					done = false
					break
				}
			}
		}
		return nil
	})
	return done, err
}

// SchemaChangeManagerTestingKnobs for the SchemaChangeManager.
type SchemaChangeManagerTestingKnobs struct {
	// AsyncSchemaChangersExecNotification is a function called before running
	// a schema change asynchronously. Returning an error will prevent the
	// asynchronous execution path from running.
	AsyncSchemaChangerExecNotification func() error

	// AsyncSchemaChangerExecQuickly executes queued schema changes as soon as
	// possible.
	AsyncSchemaChangerExecQuickly bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*SchemaChangeManagerTestingKnobs) ModuleTestingKnobs() {}

// SchemaChangeManager processes pending schema changes seen in gossip
// updates. Most schema changes are executed synchronously by the node
// that created the schema change. If the node dies while
// processing the schema change this manager acts as a backup
// execution mechanism.
type SchemaChangeManager struct {
	db           client.DB
	gossip       *gossip.Gossip
	leaseMgr     *LeaseManager
	testingKnobs *SchemaChangeManagerTestingKnobs
	// Create a schema changer for every outstanding schema change seen.
	schemaChangers map[sqlbase.ID]SchemaChanger
}

// NewSchemaChangeManager returns a new SchemaChangeManager.
func NewSchemaChangeManager(
	testingKnobs *SchemaChangeManagerTestingKnobs,
	db client.DB,
	gossip *gossip.Gossip,
	leaseMgr *LeaseManager,
) *SchemaChangeManager {
	return &SchemaChangeManager{
		db:             db,
		gossip:         gossip,
		leaseMgr:       leaseMgr,
		testingKnobs:   testingKnobs,
		schemaChangers: make(map[sqlbase.ID]SchemaChanger),
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
	stopper.RunWorker(func() {
		descKeyPrefix := keys.MakeTablePrefix(uint32(sqlbase.DescriptorTable.ID))
		gossipUpdateC := s.gossip.RegisterSystemConfigChannel()
		timer := &time.Timer{}
		delay := 360 * time.Second
		if s.testingKnobs.AsyncSchemaChangerExecQuickly {
			delay = 20 * time.Millisecond
		}
		for {
			select {
			case <-gossipUpdateC:
				cfg, _ := s.gossip.GetSystemConfig()
				// Read all tables and their versions
				if log.V(2) {
					log.Info(context.TODO(), "received a new config")
				}
				schemaChanger := SchemaChanger{
					nodeID:   roachpb.NodeID(s.leaseMgr.nodeID),
					db:       s.db,
					leaseMgr: s.leaseMgr,
				}
				// Keep track of existing schema changers.
				oldSchemaChangers := make(map[sqlbase.ID]struct{}, len(s.schemaChangers))
				for k := range s.schemaChangers {
					oldSchemaChangers[k] = struct{}{}
				}
				execAfter := timeutil.Now().Add(delay)
				// Loop through the configuration to find all the tables.
				for _, kv := range cfg.Values {
					if !bytes.HasPrefix(kv.Key, descKeyPrefix) {
						continue
					}
					// Attempt to unmarshal config into a table/database descriptor.
					var descriptor sqlbase.Descriptor
					if err := kv.Value.GetProto(&descriptor); err != nil {
						log.Warningf(context.TODO(), "%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
						continue
					}
					switch union := descriptor.Union.(type) {
					case *sqlbase.Descriptor_Table:
						table := union.Table
						table.MaybeUpgradeFormatVersion()
						if err := table.ValidateTable(); err != nil {
							log.Errorf(context.TODO(), "%s: received invalid table descriptor: %v", kv.Key, table)
							continue
						}

						// Keep track of outstanding schema changes.
						// If all schema change commands always set UpVersion, why
						// check for the presence of mutations?
						// A schema change execution might fail soon after
						// unsetting UpVersion, and we still want to process
						// outstanding mutations. Similar with a table marked for deletion.
						if table.UpVersion || table.Deleted() ||
							table.Renamed() || len(table.Mutations) > 0 {
							if log.V(2) {
								log.Infof(context.TODO(), "%s: queue up pending schema change; table: %d, version: %d",
									kv.Key, table.ID, table.Version)
							}

							// Only track the first schema change. We depend on
							// gossip to renotify us when a schema change has been
							// completed.
							schemaChanger.tableID = table.ID
							if len(table.Mutations) == 0 {
								schemaChanger.mutationID = sqlbase.InvalidMutationID
							} else {
								schemaChanger.mutationID = table.Mutations[0].MutationID
							}
							schemaChanger.execAfter = execAfter
							// Keep track of this schema change.
							// Remove from oldSchemaChangers map.
							delete(oldSchemaChangers, table.ID)
							if sc, ok := s.schemaChangers[table.ID]; ok {
								if sc.mutationID == schemaChanger.mutationID {
									// Ignore duplicate.
									continue
								}
							}
							s.schemaChangers[table.ID] = schemaChanger
						}

					case *sqlbase.Descriptor_Database:
						// Ignore.
					}
				}
				// Delete old schema changers.
				for k := range oldSchemaChangers {
					delete(s.schemaChangers, k)
				}
				timer = s.newTimer()

			case <-timer.C:
				if s.testingKnobs.AsyncSchemaChangerExecNotification != nil &&
					s.testingKnobs.AsyncSchemaChangerExecNotification() != nil {
					timer = s.newTimer()
					continue
				}
				for tableID, sc := range s.schemaChangers {
					if timeutil.Since(sc.execAfter) > 0 {
						err := sc.exec(nil, nil)
						if err != nil {
							if err == errExistingSchemaChangeLease {
							} else if err == sqlbase.ErrDescriptorNotFound {
								// Someone deleted this table. Don't try to run the schema
								// changer again. Note that there's no gossip update for the
								// deletion which would remove this schemaChanger.
								delete(s.schemaChangers, tableID)
							} else {
								// We don't need to act on integrity
								// constraints violations because exec()
								// purges mutations that violate integrity
								// constraints.
								log.Warningf(context.TODO(), "Error executing schema change: %s", err)
							}
						}
						// Advance the execAfter time so that this schema
						// changer doesn't get called again for a while.
						sc.execAfter = timeutil.Now().Add(delay)
					}
					// Only attempt to run one schema changer.
					break
				}
				timer = s.newTimer()

			case <-stopper.ShouldStop():
				return
			}
		}
	})
}
