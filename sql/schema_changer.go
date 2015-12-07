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

// ScehmaChangeLeaser is used to acquire a schema change lease on a table.
type SchemaChangeLeaser struct {
	p *planner
}

// NewSchemaChangeLeaserForTesting for tests.
func NewSchemaChangeLeaserForTesting(txn *client.Txn) SchemaChangeLeaser {
	return SchemaChangeLeaser{p: &planner{txn: txn}}
}

var errExistingSchemaChanger = errors.New("an outstanding lease exists")

func (t SchemaChangeLeaser) createSchemaChangeLease() TableDescriptor_SchemaChangeLease {
	id := parser.GenerateUniqueBytes(t.p.evalCtx.NodeID)
	return TableDescriptor_SchemaChangeLease{ID: string(id), Node: uint32(t.p.evalCtx.NodeID), ExpirationTime: time.Now().Add(jitteredLeaseDuration()).Unix()}
}

// Acquire acquires a schema change lease on the table if
// an unexpired lease doesn't exist. It returns the lease.
func (t SchemaChangeLeaser) Acquire(tableID ID) (TableDescriptor_SchemaChangeLease, error) {
	t.p.txn.SetSystemDBTrigger()
	tableDesc, err := t.p.getTableDescFromID(tableID)
	if err != nil {
		return TableDescriptor_SchemaChangeLease{}, err
	}

	// Add 5 seconds to the lease expiration to deal with time uncertainty across nodes.
	if tableDesc.Lease != nil && time.Unix(tableDesc.Lease.ExpirationTime+5, 0).After(time.Now()) {
		// Return the existing lease with an error.
		return *tableDesc.Lease, errExistingSchemaChanger
	}
	lease := t.createSchemaChangeLease()
	tableDesc.Lease = &lease
	err = t.p.txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	return lease, err
}

func (p *planner) findTableWithLease(tableID ID, lease TableDescriptor_SchemaChangeLease) (*TableDescriptor, error) {
	tableDesc, err := p.getTableDescFromID(tableID)
	if err != nil {
		return nil, err
	}
	if tableDesc.Lease == nil {
		return nil, util.Errorf("no lease present for tableID: %d", tableID)
	}
	if tableDesc.Lease.ID != lease.ID {
		return nil, util.Errorf("table: %d has lease: %v, expected: %v", tableID, tableDesc.Lease, lease)
	}
	return tableDesc, nil
}

// Release the table lease if it is the one registered with
// the table descriptor.
func (t SchemaChangeLeaser) Release(tableID ID, lease TableDescriptor_SchemaChangeLease) error {
	tableDesc, err := t.p.findTableWithLease(tableID, lease)
	if err != nil {
		return err
	}
	tableDesc.Lease = nil
	t.p.txn.SetSystemDBTrigger()
	return t.p.txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
}

// Extend the lease for the current leaser.
func (t SchemaChangeLeaser) Extend(tableID ID, lease TableDescriptor_SchemaChangeLease) (TableDescriptor_SchemaChangeLease, error) {
	tableDesc, err := t.p.findTableWithLease(tableID, lease)
	if err != nil {
		return TableDescriptor_SchemaChangeLease{}, err
	}
	lease = t.createSchemaChangeLease()
	tableDesc.Lease = &lease
	t.p.txn.SetSystemDBTrigger()
	err = t.p.txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	return lease, err
}
