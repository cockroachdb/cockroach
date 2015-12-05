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
	newTableDesc.Version++

	b.Put(MakeDescMetadataKey(newTableDesc.GetID()), wrapDescriptor(newTableDesc))

	if err := p.txn.Run(&b); err != nil {
		return convertBatchError(newTableDesc, b, err)
	}
	p.notifyCompletedSchemaChange(newTableDesc.ID)
	return nil
}

type TableLeaderLeaser struct {
	p *planner
}

// NewTableLeaderLeaserForTesting.
func NewTableLeaderLeaserForTesting(txn *client.Txn) TableLeaderLeaser {
	return TableLeaderLeaser{p: &planner{txn: txn}}
}

var errExistingTableLeader = errors.New("an outstanding lease exists")

// Acquire acquires a leader lease on the table if
// an unexpired lease doesn't exist. It returns the lease.
func (t TableLeaderLeaser) Acquire(tableID ID) (TableDescriptor_LeaderLease, error) {
	t.p.txn.SetSystemDBTrigger()
	tableDesc, err := t.p.getTableDescFromID(tableID)
	if err != nil {
		return TableDescriptor_LeaderLease{}, err
	}

	now := time.Now()
	// Add 5 seconds to the lease expiration to deal with time uncertainty across nodes.
	if tableDesc.Lease != nil && time.Unix(tableDesc.Lease.ExpirationTime+5, 0).After(now) {
		// Return the existing lease with an error.
		return *tableDesc.Lease, errExistingTableLeader
	}

	leader := parser.GenerateUniqueBytes(t.p.evalCtx.NodeID)
	lease := TableDescriptor_LeaderLease{Leader: string(leader), ExpirationTime: now.Add(jitteredLeaseDuration()).Unix()}
	tableDesc.Lease = &lease
	err = t.p.txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	return lease, err
}

func (p *planner) findTableWithLease(tableID ID, lease TableDescriptor_LeaderLease) (*TableDescriptor, error) {
	tableDesc, err := p.getTableDescFromID(tableID)
	if err != nil {
		return nil, err
	}
	if tableDesc.Lease == nil {
		return nil, util.Errorf("no lease present for tableID: %d", tableID)
	}
	if tableDesc.Lease.Leader != lease.Leader {
		return nil, util.Errorf("lease for table ID: %d, owned by different leader: %q, expected: %q", tableID, tableDesc.Lease.Leader, lease.Leader)
	}
	return tableDesc, nil
}

// Release the table lease if leader is the lease leader.
func (t TableLeaderLeaser) Release(tableID ID, lease TableDescriptor_LeaderLease) error {
	tableDesc, err := t.p.findTableWithLease(tableID, lease)
	if err != nil {
		return err
	}
	tableDesc.Lease = nil
	t.p.txn.SetSystemDBTrigger()
	return t.p.txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
}

// Extend the lease for the current leader.
func (t TableLeaderLeaser) Extend(tableID ID, lease TableDescriptor_LeaderLease) (TableDescriptor_LeaderLease, error) {
	tableDesc, err := t.p.findTableWithLease(tableID, lease)
	if err != nil {
		return TableDescriptor_LeaderLease{}, err
	}
	leader := parser.GenerateUniqueBytes(t.p.evalCtx.NodeID)
	lease = TableDescriptor_LeaderLease{Leader: string(leader), ExpirationTime: time.Now().Add(jitteredLeaseDuration()).Unix()}
	tableDesc.Lease = &lease
	t.p.txn.SetSystemDBTrigger()
	err = t.p.txn.Put(MakeDescMetadataKey(tableDesc.ID), wrapDescriptor(tableDesc))
	return lease, err
}
