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
	"math"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// SchemaChanger is used to change the schema on a table.
type SchemaChanger struct {
	tableID    sqlbase.ID
	mutationID sqlbase.MutationID
	nodeID     roachpb.NodeID
	db         client.DB
	cfg        config.SystemConfig
	leaseMgr   *LeaseManager
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
		NodeID: sc.nodeID, ExpirationTime: timeutil.Now().Add(jitteredLeaseDuration()).UnixNano()}
}

// AcquireLease acquires a schema change lease on the table if
// an unexpired lease doesn't exist. It returns the lease.
// TODO(andrei): change to error
func (sc *SchemaChanger) AcquireLease() (sqlbase.TableDescriptor_SchemaChangeLease, *roachpb.Error) {
	var lease sqlbase.TableDescriptor_SchemaChangeLease
	err := sc.db.Txn(func(txn *client.Txn) error {
		txn.SetSystemConfigTrigger()
		tableDesc, err := getTableDescFromID(txn, sc.tableID)
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
				return &roachpb.ExistingSchemaChangeLeaseError{}
			}
			log.Infof("Overriding existing expired lease %v", tableDesc.Lease)
		}
		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		return txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	})
	return lease, roachpb.NewError(err)
}

func (sc *SchemaChanger) findTableWithLease(
	txn *client.Txn, lease sqlbase.TableDescriptor_SchemaChangeLease,
) (*sqlbase.TableDescriptor, error) {
	tableDesc, err := getTableDescFromID(txn, sc.tableID)
	if err != nil {
		return nil, err
	}
	if tableDesc.Lease == nil {
		return nil, util.Errorf("no lease present for tableID: %d", sc.tableID)
	}
	if *tableDesc.Lease != lease {
		return nil, util.Errorf("table: %d has lease: %v, expected: %v", sc.tableID, tableDesc.Lease, lease)
	}
	return tableDesc, nil
}

// ReleaseLease the table lease if it is the one registered with
// the table descriptor.
func (sc *SchemaChanger) ReleaseLease(lease sqlbase.TableDescriptor_SchemaChangeLease) error {
	err := sc.db.Txn(func(txn *client.Txn) error {
		tableDesc, pErr := sc.findTableWithLease(txn, lease)
		if pErr != nil {
			return pErr
		}
		tableDesc.Lease = nil
		txn.SetSystemConfigTrigger()
		return txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	})
	return err
}

// ExtendLease for the current leaser.
func (sc *SchemaChanger) ExtendLease(
	existingLease sqlbase.TableDescriptor_SchemaChangeLease,
) (sqlbase.TableDescriptor_SchemaChangeLease, error) {
	var lease sqlbase.TableDescriptor_SchemaChangeLease
	err := sc.db.Txn(func(txn *client.Txn) error {
		tableDesc, err := sc.findTableWithLease(txn, existingLease)
		if err != nil {
			return err
		}
		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		txn.SetSystemConfigTrigger()
		return txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	})
	return lease, err
}

// Execute the entire schema change in steps. startBackfillNotification is
// called before the backfill starts; it can be nil.
func (sc SchemaChanger) exec(startBackfillNotification func()) error {
	// Acquire lease.
	lease, pErr := sc.AcquireLease()
	if pErr != nil {
		return pErr.GoError()
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
			log.Warning(err)
		}
	}(&lease)

	// Increment the version and unset tableDescriptor.UpVersion.
	desc, err := sc.MaybeIncrementVersion()
	if err != nil {
		return err
	}

	if desc.GetTable().Deleted {
		// Wait for everybody to see the version with the deleted bit set. When
		// this returns, nobody has any leases on the table, nor can get new leases,
		// so the table will no longer be modified.

		lease, err = sc.ExtendLease(lease)
		if err != nil {
			return err
		}
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

	// Wait for the schema change to propagate to all nodes after this function
	// returns, so that the new schema is live everywhere. This is not needed for
	// correctness but is done to make the UI experience/tests predictable.
	defer func() {
		if err := sc.waitToUpdateLeases(); err != nil {
			log.Warning(err)
		}
	}()

	if sc.mutationID == invalidMutationID {
		// Nothing more to do.
		return nil
	}

	// Another transaction might set the up_version bit again,
	// but we're no longer responsible for taking care of that.

	// Run through mutation state machine before backfill.
	if err := sc.RunStateMachineBeforeBackfill(); err != nil {
		return err
	}

	if startBackfillNotification != nil {
		startBackfillNotification()
	}

	// Run backfill.
	if err := sc.runBackfill(&lease); err != nil {
		// Purge the mutations if the application of the mutations fail.
		if errPurge := sc.purgeMutations(&lease); errPurge != nil {
			return util.Errorf("error purging mutation: %s, after error: %s", errPurge, pErr)
		}
		return err
	}

	// Mark the mutations as completed.
	_, err = sc.done()
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
			return &roachpb.DidntUpdateDescriptorError{}
		}
		desc.UpVersion = false
		// Publish() will increment the version.
		return nil
	})
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
			case DescriptorMutation_ADD:
				switch mutation.State {
				case DescriptorMutation_DELETE_ONLY:
					// TODO(vivek): while moving up the state is appropriate,
					// it will be better to run the backfill of a unique index
					// twice: once in the DELETE_ONLY state to confirm that
					// the index can indeed be created, and subsequently in the
					// WRITE_ONLY state to fill in the missing elements of the
					// index (INSERT and UPDATE that happened in the interim).
					desc.Mutations[i].State = DescriptorMutation_WRITE_ONLY
					modified = true

				case DescriptorMutation_WRITE_ONLY:
					// The state change has already moved forward.
				}

			case DescriptorMutation_DROP:
				switch mutation.State {
				case DescriptorMutation_DELETE_ONLY:
					// The state change has already moved forward.

				case DescriptorMutation_WRITE_ONLY:
					desc.Mutations[i].State = DescriptorMutation_DELETE_ONLY
					modified = true
				}
			}
		}
		if !modified {
			// Return error so that Publish() doesn't increment the version.
			return &roachpb.DidntUpdateDescriptorError{}
		}
		return nil
	}); err != nil {
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
	_, err := sc.leaseMgr.waitForOneVersion(sc.tableID, retryOpts)
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
			desc.makeMutationComplete(mutation)
			i++
		}
		if i == 0 {
			// The table descriptor is unchanged. Don't let Publish() increment
			// the version.
			return &roachpb.DidntUpdateDescriptorError{}
		}
		// Trim the executed mutations from the descriptor.
		desc.Mutations = desc.Mutations[i:]
		return nil
	})
}

// Purge all mutations with the mutationID. This is called after
// hitting an irrecoverable error. Reverse the direction of the mutations
// and run through the state machine until the mutations are deleted.
func (sc *SchemaChanger) purgeMutations(lease *sqlbase.TableDescriptor_SchemaChangeLease) error {
	// Reverse the flow of the state machine.
	if _, err := sc.leaseMgr.Publish(sc.tableID, func(desc *sqlbase.TableDescriptor) error {
		for i, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			log.Warningf("Purging schema change mutation: %v", desc.Mutations[i])
			switch mutation.Direction {
			case DescriptorMutation_ADD:
				desc.Mutations[i].Direction = DescriptorMutation_DROP

			case DescriptorMutation_DROP:
				desc.Mutations[i].Direction = DescriptorMutation_ADD
			}
		}
		// Publish() will increment the version.
		return nil
	}); err != nil {
		return err
	}

	// Run through mutation state machine before backfill.
	if err := sc.RunStateMachineBeforeBackfill(); err != nil {
		return err
	}

	// Run backfill and don't run purge on hitting an error.
	// TODO(vivek): If this fails we can get into a permanent
	// failure with some mutations, where subsequent schema
	// changers keep attempting to apply and purge mutations.
	// This is a theoretical problem at this stage (2015/12).
	if err := sc.runBackfill(lease); err != nil {
		return err
	}

	// Mark the mutations as completed.
	_, err := sc.done()
	return err
}

// IsDone returns true if the work scheduled for the schema changer
// is complete.
func (sc *SchemaChanger) IsDone() (bool, error) {
	var done bool
	err := sc.db.Txn(func(txn *client.Txn) error {
		done = true
		tableDesc, err := getTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}
		if sc.mutationID == invalidMutationID {
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

// SchemaChangeManager processes pending schema changes seen in gossip
// updates. Most schema changes are executed synchronously by the node
// that created the schema change. If the node dies while
// processing the schema change this manager acts as a backup
// execution mechanism.
type SchemaChangeManager struct {
	db       client.DB
	gossip   *gossip.Gossip
	leaseMgr *LeaseManager
	// Create a schema changer for every outstanding schema change seen.
	schemaChangers map[sqlbase.ID]SchemaChanger
}

// NewSchemaChangeManager returns a new SchemaChangeManager.
func NewSchemaChangeManager(
	db client.DB, gossip *gossip.Gossip, leaseMgr *LeaseManager,
) *SchemaChangeManager {
	return &SchemaChangeManager{
		db:             db,
		gossip:         gossip,
		leaseMgr:       leaseMgr,
		schemaChangers: make(map[ID]SchemaChanger),
	}
}

var (
	disableAsyncSchemaChangeExec = false
	// How often does the SchemaChangeManager attempt to execute
	// pending schema changes.
	asyncSchemaChangeExecInterval = 60 * time.Second
	// How old must the schema change be before the SchemaChangeManager
	// attempts to execute it.
	asyncSchemaChangeExecDelay = 360 * time.Second
)

// TestSpeedupAsyncSchemaChanges can be used in tests to manipulate the async
// executor timing and make it run quickly and often.  Returns a cleanup
// function restoring original values.
func TestSpeedupAsyncSchemaChanges() func() {
	origInterval, origDelay := asyncSchemaChangeExecInterval, asyncSchemaChangeExecDelay
	asyncSchemaChangeExecDelay = 20 * time.Millisecond
	asyncSchemaChangeExecInterval = 20 * time.Millisecond
	return func() {
		asyncSchemaChangeExecDelay = origDelay
		asyncSchemaChangeExecInterval = origInterval
	}
}

// TestDisableAsyncSchemaChangeExec is used in tests to
// disable the asynchronous execution of schema changes.
func TestDisableAsyncSchemaChangeExec() func() {
	disableAsyncSchemaChangeExec = true
	return func() {
		disableAsyncSchemaChangeExec = false
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
	if disableAsyncSchemaChangeExec {
		return
	}
	stopper.RunWorker(func() {
		descKeyPrefix := keys.MakeTablePrefix(uint32(descriptorTable.ID))
		gossipUpdateC := s.gossip.RegisterSystemConfigChannel()
		timer := &time.Timer{}
		for {
			select {
			case <-gossipUpdateC:
				cfg, _ := s.gossip.GetSystemConfig()
				// Read all tables and their versions
				if log.V(2) {
					log.Info("received a new config %v", cfg)
				}
				schemaChanger := SchemaChanger{
					nodeID:   roachpb.NodeID(s.leaseMgr.nodeID),
					db:       s.db,
					leaseMgr: s.leaseMgr,
				}
				// Keep track of existing schema changers.
				oldSchemaChangers := make(map[ID]struct{}, len(s.schemaChangers))
				for k := range s.schemaChangers {
					oldSchemaChangers[k] = struct{}{}
				}
				execAfter := timeutil.Now().Add(asyncSchemaChangeExecDelay)
				// Loop through the configuration to find all the tables.
				for _, kv := range cfg.Values {
					if !bytes.HasPrefix(kv.Key, descKeyPrefix) {
						continue
					}
					// Attempt to unmarshal config into a table/database descriptor.
					var descriptor Descriptor
					if err := kv.Value.GetProto(&descriptor); err != nil {
						log.Warningf("%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
						continue
					}
					switch union := descriptor.Union.(type) {
					case *Descriptor_Table:
						table := union.Table
						if err := table.Validate(); err != nil {
							log.Errorf("%s: received invalid table descriptor: %v", kv.Key, table)
							continue
						}

						// Keep track of outstanding schema changes.
						// If all schema change commands always set UpVersion, why
						// check for the presence of mutations?
						// A schema change execution might fail soon after
						// unsetting UpVersion, and we still want to process
						// outstanding mutations. Similar with a table marked for deletion.
						if table.UpVersion || table.Deleted || len(table.Mutations) > 0 {
							if log.V(2) {
								log.Infof("%s: queue up pending schema change; table: %d, version: %d",
									kv.Key, table.ID, table.Version)
							}

							// Only track the first schema change. We depend on
							// gossip to renotify us when a schema change has been
							// completed.
							schemaChanger.tableID = table.ID
							if len(table.Mutations) == 0 {
								schemaChanger.mutationID = invalidMutationID
							} else {
								schemaChanger.mutationID = table.Mutations[0].MutationID
							}
							schemaChanger.cfg = cfg
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

					case *Descriptor_Database:
						// Ignore.
					}
				}
				// Delete old schema changers.
				for k := range oldSchemaChangers {
					delete(s.schemaChangers, k)
				}
				timer = s.newTimer()

			case <-timer.C:
				for tableID, sc := range s.schemaChangers {
					if timeutil.Since(sc.execAfter) > 0 {
						err := sc.exec(nil)
						if err != nil {
							switch err.(type) {
							case *roachpb.ExistingSchemaChangeLeaseError:
							case *roachpb.DescriptorNotFoundError:
								// Someone deleted this table. Don't try to run the schema
								// changer again. Note that there's no gossip update for the
								// deletion which would remove this schemaChanger.
								delete(s.schemaChangers, tableID)
							default:
								log.Warningf("Error executing schema change: %s", err)
							}
						}
						// Advance the execAfter time so that this schema changer
						// doesn't get called again for a while.
						sc.execAfter = timeutil.Now().Add(asyncSchemaChangeExecDelay)
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
