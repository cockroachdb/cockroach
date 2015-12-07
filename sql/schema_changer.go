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
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

// Future home of the aysynchronous schema changer that picks up
// queued schema changes and processes them.
//
// applyMutations applies the queued mutations for a table.
func (p *planner) applyMutations(tableDesc *TableDescriptor) error {
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
		return err
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
	node    roachpb.NodeID
	txn     *client.Txn
}

// NewSchemaChangerForTesting only for tests.
func NewSchemaChangerForTesting(tableID ID, node roachpb.NodeID, txn *client.Txn) SchemaChanger {
	return SchemaChanger{tableID: tableID, node: node, txn: txn}
}

var errExistingSchemaChangeLease = errors.New("an outstanding schema change lease exists")

func (t *SchemaChanger) createSchemaChangeLease() TableDescriptor_SchemaChangeLease {
	id := parser.GenerateUniqueBytes(t.node)
	return TableDescriptor_SchemaChangeLease{ID: string(id), Node: uint32(t.node), ExpirationTime: time.Now().Add(jitteredLeaseDuration()).Unix()}
}

// AcquireLease acquires a schema change lease on the table if
// an unexpired lease doesn't exist. It returns the lease.
func (t *SchemaChanger) AcquireLease() (TableDescriptor_SchemaChangeLease, error) {
	t.txn.SetSystemDBTrigger()
	tableDesc, err := getTableDescFromID(t.txn, t.tableID)
	if err != nil {
		return TableDescriptor_SchemaChangeLease{}, err
	}

	// Add 5 seconds to the lease expiration to deal with time uncertainty across nodes.
	if tableDesc.Lease != nil && time.Unix(tableDesc.Lease.ExpirationTime+5, 0).After(time.Now()) {
		// Return the existing lease with an error.
		return *tableDesc.Lease, errExistingSchemaChangeLease
	}
	lease := t.createSchemaChangeLease()
	tableDesc.Lease = &lease
	err = t.txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	return lease, err
}

func (t *SchemaChanger) findTableWithLease(lease TableDescriptor_SchemaChangeLease) (*TableDescriptor, error) {
	tableDesc, err := getTableDescFromID(t.txn, t.tableID)
	if err != nil {
		return nil, err
	}
	if tableDesc.Lease == nil {
		return nil, util.Errorf("no lease present for tableID: %d", t.tableID)
	}
	if tableDesc.Lease.ID != lease.ID {
		return nil, util.Errorf("table: %d has lease: %v, expected: %v", t.tableID, tableDesc.Lease, lease)
	}
	return tableDesc, nil
}

// ReleaseLease the table lease if it is the one registered with
// the table descriptor.
func (t *SchemaChanger) ReleaseLease(lease TableDescriptor_SchemaChangeLease) error {
	tableDesc, err := t.findTableWithLease(lease)
	if err != nil {
		return err
	}
	tableDesc.Lease = nil
	t.txn.SetSystemDBTrigger()
	return t.txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
}

// ExtendLease for the current leaser.
func (t *SchemaChanger) ExtendLease(lease TableDescriptor_SchemaChangeLease) (TableDescriptor_SchemaChangeLease, error) {
	tableDesc, err := t.findTableWithLease(lease)
	if err != nil {
		return TableDescriptor_SchemaChangeLease{}, err
	}
	lease = t.createSchemaChangeLease()
	tableDesc.Lease = &lease
	t.txn.SetSystemDBTrigger()
	err = t.txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	return lease, err
}
