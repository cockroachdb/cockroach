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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"errors"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/gogo/protobuf/proto"
)

var errNoMutations = errors.New("no mutations")
var errNoUpVersion = errors.New("no up version")
var errReadyToApply = errors.New("ready to apply")

// SchemaChanger is used to change the schema on a table.
type SchemaChanger struct {
	tableID    ID
	mutationID MutationID
	nodeID     roachpb.NodeID
	db         client.DB
	// Do we really need this?
	cfg      config.SystemConfig
	leaseMgr *LeaseManager
	// Time created within the asynchronous
	// schema changer. Only used by the
	// asynchronous schema changer.
	creationTime time.Time
}

// applyMutations runs the backfill for the mutations.
// TODO(vivek): Merge this with backfill.
//
func (sc *SchemaChanger) applyMutations() error {
	err := sc.db.Txn(func(txn *client.Txn) error {
		// Should we be using the original users privileges?
		p := planner{user: security.RootUser, systemConfig: sc.cfg, leaseMgr: sc.leaseMgr}
		timestamp := time.Now()
		p.setTxn(txn, timestamp)

		tableDesc, err := getTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}

		newTableDesc := proto.Clone(tableDesc).(*TableDescriptor)
		// Locally apply mutations belonging to the same mutationID
		// for use by backfillBatch().
		i := 0
		for _, mutation := range newTableDesc.Mutations {
			if mutation.MutationID != sc.mutationID {
				break
			}
			i++
			newTableDesc.makeMutationComplete(mutation)
		}
		if i == 0 {
			// Nothing to do.
			return nil
		}
		newTableDesc.Mutations = newTableDesc.Mutations[i:]
		if err := newTableDesc.Validate(); err != nil {
			return err
		}

		b := client.Batch{}
		if err := p.backfillBatch(&b, tableDesc, newTableDesc); err != nil {
			return err
		}
		if err := p.txn.Run(&b); err != nil {
			return convertBatchError(newTableDesc, b, err)
		}

		return nil
	})
	return err
}

// NewSchemaChangerForTesting only for tests.
func NewSchemaChangerForTesting(tableID ID, nodeID roachpb.NodeID, db client.DB) SchemaChanger {
	return SchemaChanger{tableID: tableID, nodeID: nodeID, db: db}
}

var errExistingSchemaChangeLease = errors.New("an outstanding schema change lease exists")

func (sc *SchemaChanger) createSchemaChangeLease() TableDescriptor_SchemaChangeLease {
	return TableDescriptor_SchemaChangeLease{NodeID: sc.nodeID, ExpirationTime: time.Now().Add(jitteredLeaseDuration()).UnixNano()}
}

// AcquireLease acquires a schema change lease on the table if
// an unexpired lease doesn't exist. It returns the lease.
func (sc *SchemaChanger) AcquireLease() (TableDescriptor_SchemaChangeLease, error) {
	var lease TableDescriptor_SchemaChangeLease
	err := sc.db.Txn(func(txn *client.Txn) error {
		txn.SetSystemDBTrigger()
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

		if tableDesc.Lease != nil && time.Unix(0, tableDesc.Lease.ExpirationTime).Add(expirationTimeUncertainty).After(time.Now()) {
			return errExistingSchemaChangeLease
		}
		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		return txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	})
	return lease, err
}

func (sc *SchemaChanger) findTableWithLease(txn *client.Txn, lease TableDescriptor_SchemaChangeLease) (*TableDescriptor, error) {
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
func (sc *SchemaChanger) ReleaseLease(lease TableDescriptor_SchemaChangeLease) error {
	return sc.db.Txn(func(txn *client.Txn) error {
		tableDesc, err := sc.findTableWithLease(txn, lease)
		if err != nil {
			return err
		}
		tableDesc.Lease = nil
		txn.SetSystemDBTrigger()
		return txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	})
}

// ExtendLease for the current leaser.
func (sc *SchemaChanger) ExtendLease(lease TableDescriptor_SchemaChangeLease) (TableDescriptor_SchemaChangeLease, error) {
	err := sc.db.Txn(func(txn *client.Txn) error {
		tableDesc, err := sc.findTableWithLease(txn, lease)
		if err != nil {
			return err
		}
		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		txn.SetSystemDBTrigger()
		return txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	})
	return lease, err
}

// execute the entire schema change in steps.
func (sc SchemaChanger) exec() error {
	// Acquire lease.
	lease, err := sc.AcquireLease()
	if err != nil {
		return err
	}
	// Always release lease.
	defer func() {
		if err := sc.ReleaseLease(lease); err != nil {
			log.Warning(err)
		}
	}()

	// Increment the version and unset tableDescriptor.UpVersion.
	if err := sc.maybeIncrementVersion(); err != nil {
		return err
	}

	if sc.mutationID == invalidMutationID {
		// Nothing more to do.
		return sc.waitToUpdateLeases()
	}

	// Another transaction might set the up_version bit again,
	// but we're not anymore responsible for taking care of that.

	// Run through mutation state machine before backfill.
	if err := sc.runStateMachineBeforeBackfill(); err != nil {
		if err == errNoMutations {
			return nil
		}
		return err
	}

	// apply backfill.
	if err := sc.applyMutations(); err != nil {
		// Purge the mutations if the application of the mutations fail.
		if err := sc.purgeMutations(); err != nil {
			return err
		}
		return err
	}

	// Mark the mutations as completed, and wait until the latest version
	// is visible on all leases.
	return sc.done()
}

// Returns nil if the table descriptor had UpVersion set and
// the version is incremented.
func (sc *SchemaChanger) maybeIncrementVersion() error {
	if err := sc.leaseMgr.Publish(sc.tableID, func(desc *TableDescriptor) error {
		if !desc.UpVersion {
			// Return error so that Publish() doesn't increment the version.
			return errNoUpVersion
		}
		desc.UpVersion = false
		// Publish() will increment the version.
		return nil
	}); err != nil && err != errNoUpVersion {
		return err
	}
	return nil
}

// Pick the mutation version that is going to be applied, move the
// state forward and wait to ensure that all nodes are seeing the
// latest version of the table. return the version of the mutation
// picked (which is not necessarily the latest version).
func (sc *SchemaChanger) runStateMachineBeforeBackfill() error {
	if err := sc.leaseMgr.Publish(sc.tableID, func(desc *TableDescriptor) error {
		var workDone bool
		// Apply mutations belonging to the same version.
		for i, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
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

				case DescriptorMutation_WRITE_ONLY:
					// The state change has already moved forward.
				}

			case DescriptorMutation_DROP:
				switch mutation.State {
				case DescriptorMutation_DELETE_ONLY:
					// The state change has already moved forward.

				case DescriptorMutation_WRITE_ONLY:
					desc.Mutations[i].State = DescriptorMutation_DELETE_ONLY
				}
			}
			workDone = true
		}
		if !workDone {
			return errNoMutations
		}
		return nil
	}); err != nil {
		return err
	}
	// wait for the state change to propagate to all leases.
	return sc.waitToUpdateLeases()
}

// wait for the last version to propagate to all leases.
func (sc *SchemaChanger) waitToUpdateLeases() error {
	// Wait for last state to propagate everywhere.
	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	_, err := sc.leaseMgr.waitForOneVersion(sc.tableID, retryOpts)
	return err
}

func (sc *SchemaChanger) done() error {
	if err := sc.leaseMgr.Publish(sc.tableID, func(desc *TableDescriptor) error {
		i := 0
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				break
			}
			desc.makeMutationComplete(mutation)
			i++
		}
		desc.Mutations = desc.Mutations[i:]
		return nil
	}); err != nil {
		return err
	}
	// Finished executing some schema changes. Wait for the schema change
	// to propagate to all nodes, so that the new schema is live everywhere.
	// This is not needed for correctness but is done to make the UI
	// experience/tests predictable.
	_ = sc.waitToUpdateLeases()
	return nil
}

// Purge all mutations with the mutationID. This is called after
// hitting an irrecoverable error. Reverse the direction of the mutations
// and run through the mutations until their deleted.
func (sc *SchemaChanger) purgeMutations() error {
	// Reverse the flow of the state machine.
	if err := sc.leaseMgr.Publish(sc.tableID, func(desc *TableDescriptor) error {
		for i, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				break
			}
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
	if err := sc.runStateMachineBeforeBackfill(); err != nil {
		if err == errNoMutations {
			return nil
		}
		return err
	}

	// apply backfill and don't run purge on hitting an error.
	// TODO(vivek): If this fails we can get into a permanent
	// failure with some mutations, where subsequent schema
	// changers keep attempting to apply and purge mutations.
	// we try to apply and purge the mutations
	if err := sc.applyMutations(); err != nil {
		return err
	}

	// Mark the mutations as completed, and wait until the latest version
	// is visible on all leases.
	return sc.done()
}

// Is the work scheduled for the schema changer already done.
func (sc *SchemaChanger) isDone() (bool, error) {
	var done bool
	err := sc.db.Txn(func(txn *client.Txn) error {
		tableDesc, err := getTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}
		if sc.mutationID == invalidMutationID {
			if !tableDesc.UpVersion {
				done = true
			}
		} else {
			done = true
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
