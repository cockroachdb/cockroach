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
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// SchemaChanger is used to change the schema on a table.
type SchemaChanger struct {
	tableID    ID
	mutationID MutationID
	nodeID     roachpb.NodeID
	db         client.DB
	cfg        config.SystemConfig
	leaseMgr   *LeaseManager
	// The SchemaChangeManager can attempt to execute this schema
	// changer after this time.
	execAfter time.Time
}

// NewSchemaChangerForTesting only for tests.
func NewSchemaChangerForTesting(
	tableID ID, mutationID MutationID, nodeID roachpb.NodeID, db client.DB, leaseMgr *LeaseManager,
) SchemaChanger {
	return SchemaChanger{tableID: tableID, mutationID: mutationID, nodeID: nodeID, db: db, leaseMgr: leaseMgr}
}

func (sc *SchemaChanger) createSchemaChangeLease() TableDescriptor_SchemaChangeLease {
	return TableDescriptor_SchemaChangeLease{NodeID: sc.nodeID, ExpirationTime: timeutil.Now().Add(jitteredLeaseDuration()).UnixNano()}
}

// AcquireLease acquires a schema change lease on the table if
// an unexpired lease doesn't exist. It returns the lease.
func (sc *SchemaChanger) AcquireLease() (TableDescriptor_SchemaChangeLease, *roachpb.Error) {
	var lease TableDescriptor_SchemaChangeLease
	err := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
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
				return roachpb.NewError(&roachpb.ExistingSchemaChangeLeaseError{})
			}
			log.Infof("Overriding existing expired lease %v", tableDesc.Lease)
		}
		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		return txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	})
	return lease, err
}

func (sc *SchemaChanger) findTableWithLease(
	txn *client.Txn, lease TableDescriptor_SchemaChangeLease,
) (*TableDescriptor, *roachpb.Error) {
	tableDesc, err := getTableDescFromID(txn, sc.tableID)
	if err != nil {
		return nil, err
	}
	if tableDesc.Lease == nil {
		return nil, roachpb.NewErrorf("no lease present for tableID: %d", sc.tableID)
	}
	if *tableDesc.Lease != lease {
		return nil, roachpb.NewErrorf("table: %d has lease: %v, expected: %v", sc.tableID, tableDesc.Lease, lease)
	}
	return tableDesc, nil
}

// ReleaseLease the table lease if it is the one registered with
// the table descriptor.
func (sc *SchemaChanger) ReleaseLease(lease TableDescriptor_SchemaChangeLease) error {
	pErr := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
		tableDesc, pErr := sc.findTableWithLease(txn, lease)
		if pErr != nil {
			return pErr
		}
		tableDesc.Lease = nil
		txn.SetSystemConfigTrigger()
		return txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	})
	return pErr.GoError()
}

// ExtendLease for the current leaser.
func (sc *SchemaChanger) ExtendLease(
	existingLease TableDescriptor_SchemaChangeLease,
) (TableDescriptor_SchemaChangeLease, *roachpb.Error) {
	var lease TableDescriptor_SchemaChangeLease
	err := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
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

// Execute the entire schema change in steps.
func (sc SchemaChanger) exec() *roachpb.Error {
	// Acquire lease.
	lease, pErr := sc.AcquireLease()
	if pErr != nil {
		return pErr
	}
	// Always try to release lease.
	defer func(l *TableDescriptor_SchemaChangeLease) {
		if err := sc.ReleaseLease(*l); err != nil {
			log.Warning(err)
		}
	}(&lease)

	// Increment the version and unset tableDescriptor.UpVersion.
	if pErr := sc.MaybeIncrementVersion(); pErr != nil {
		return pErr
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
		return roachpb.NewError(err)
	}

	// Run backfill.
	if pErr := sc.runBackfill(&lease); pErr != nil {
		// Purge the mutations if the application of the mutations fail.
		if errPurge := sc.purgeMutations(&lease); errPurge != nil {
			return roachpb.NewErrorf("error purging mutation: %s, after error: %s", errPurge, pErr)
		}
		return pErr
	}

	// Mark the mutations as completed.
	return sc.done()
}

// MaybeIncrementVersion increments the version if needed.
func (sc *SchemaChanger) MaybeIncrementVersion() *roachpb.Error {
	return sc.leaseMgr.Publish(sc.tableID, func(desc *TableDescriptor) error {
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
	if pErr := sc.leaseMgr.Publish(sc.tableID, func(desc *TableDescriptor) error {
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
	}); pErr != nil {
		return pErr.GoError()
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

func (sc *SchemaChanger) done() *roachpb.Error {
	return sc.leaseMgr.Publish(sc.tableID, func(desc *TableDescriptor) error {
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
		desc.Mutations = desc.Mutations[i:]
		return nil
	})
}

// Purge all mutations with the mutationID. This is called after
// hitting an irrecoverable error. Reverse the direction of the mutations
// and run through the state machine until the mutations are deleted.
func (sc *SchemaChanger) purgeMutations(lease *TableDescriptor_SchemaChangeLease) error {
	// Reverse the flow of the state machine.
	if pErr := sc.leaseMgr.Publish(sc.tableID, func(desc *TableDescriptor) error {
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
	}); pErr != nil {
		return pErr.GoError()
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
	if pErr := sc.runBackfill(lease); pErr != nil {
		return pErr.GoError()
	}

	// Mark the mutations as completed.
	return sc.done().GoError()
}

// IsDone returns true if the work scheduled for the schema changer
// is complete.
func (sc *SchemaChanger) IsDone() (bool, error) {
	var done bool
	pErr := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
		done = true
		tableDesc, pErr := getTableDescFromID(txn, sc.tableID)
		if pErr != nil {
			return pErr
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
	return done && pErr == nil, pErr.GoError()
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
	schemaChangers map[ID]SchemaChanger
}

// NewSchemaChangeManager returns a new SchemaChangeManager.
func NewSchemaChangeManager(db client.DB, gossip *gossip.Gossip, leaseMgr *LeaseManager) *SchemaChangeManager {
	return &SchemaChangeManager{db: db, gossip: gossip, leaseMgr: leaseMgr, schemaChangers: make(map[ID]SchemaChanger)}
}

var (
	disableSyncSchemaChangeExec  = false
	disableAsyncSchemaChangeExec = false
	// How often does the SchemaChangeManager attempt to execute
	// pending schema changes.
	asyncSchemaChangeExecInterval = 60 * time.Second
	// How old must the schema change be before the SchemaChangeManager
	// attempts to execute it.
	asyncSchemaChangeExecDelay = 360 * time.Second
)

// TestDisableSyncSchemaChangeExec is used in tests to
// disable the synchronous execution of schema changes,
// so that the asynchronous schema changer can run the
// schema changes.
func TestDisableSyncSchemaChangeExec() func() {
	disableSyncSchemaChangeExec = true
	// Attempt to execute almost immediately.
	asyncSchemaChangeExecInterval = 20 * time.Millisecond
	asyncSchemaChangeExecDelay = 20 * time.Millisecond
	return func() {
		disableSyncSchemaChangeExec = false
		asyncSchemaChangeExecInterval = 60 * time.Second
		asyncSchemaChangeExecDelay = 360 * time.Second
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
						// outstanding mutations.
						if table.UpVersion || len(table.Mutations) > 0 {
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
				for _, sc := range s.schemaChangers {
					if time.Since(sc.execAfter) > 0 {
						pErr := sc.exec()
						if _, ok := pErr.GetDetail().(*roachpb.ExistingSchemaChangeLeaseError); !ok && pErr != nil {
							log.Info(pErr)
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
