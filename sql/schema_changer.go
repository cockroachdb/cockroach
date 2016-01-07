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
	"errors"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/gogo/protobuf/proto"
)

// Future home of the asynchronous schema changer that picks up
// queued schema changes and processes them.
//
// applyMutations applies the queued mutations for a table.
func (p *planner) applyMutations(tableDesc *TableDescriptor) *roachpb.Error {
	if len(tableDesc.Mutations) == 0 {
		return nil
	}
	newTableDesc := proto.Clone(tableDesc).(*TableDescriptor)
	p.applyUpVersion(newTableDesc)
	// Make all mutations active.
	for _, mutation := range newTableDesc.Mutations {
		newTableDesc.makeMutationComplete(mutation)
	}
	newTableDesc.Mutations = nil
	if err := newTableDesc.Validate(); err != nil {
		return roachpb.NewError(err)
	}

	b := client.Batch{}
	if err := p.backfillBatch(&b, tableDesc, newTableDesc); err != nil {
		return err
	}

	b.Put(MakeDescMetadataKey(newTableDesc.GetID()), wrapDescriptor(newTableDesc))

	if err := p.txn.Run(&b); err != nil {
		return convertBatchError(newTableDesc, b, err)
	}
	p.notifyCompletedSchemaChange(newTableDesc.ID)
	return nil
}

// applyUpVersion only increments the version of the table descriptor.
func (p *planner) applyUpVersion(tableDesc *TableDescriptor) {
	if tableDesc.UpVersion {
		tableDesc.UpVersion = false
		tableDesc.Version++
	}
}

// SchemaChanger is used to change the schema on a table.
type SchemaChanger struct {
	tableID ID
	nodeID  roachpb.NodeID
	db      client.DB
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
func (sc *SchemaChanger) AcquireLease() (TableDescriptor_SchemaChangeLease, *roachpb.Error) {
	var lease TableDescriptor_SchemaChangeLease
	err := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
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
			return roachpb.NewError(errExistingSchemaChangeLease)
		}
		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		return txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	})
	return lease, err
}

func (sc *SchemaChanger) findTableWithLease(txn *client.Txn, lease TableDescriptor_SchemaChangeLease) (*TableDescriptor, *roachpb.Error) {
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
func (sc *SchemaChanger) ReleaseLease(lease TableDescriptor_SchemaChangeLease) *roachpb.Error {
	return sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
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
func (sc *SchemaChanger) ExtendLease(lease TableDescriptor_SchemaChangeLease) (TableDescriptor_SchemaChangeLease, *roachpb.Error) {
	err := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
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
