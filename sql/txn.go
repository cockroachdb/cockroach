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
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// BeginTransaction starts a new transaction.
func (p *planner) BeginTransaction(n *parser.BeginTransaction) (planNode, *roachpb.Error) {
	if p.txn == nil {
		return nil, roachpb.NewErrorf("the server should have already created a transaction")
	}
	if err := p.setIsolationLevel(n.Isolation); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}

// CommitTransaction commits a transaction.
func (p *planner) CommitTransaction(n *parser.CommitTransaction) (planNode, *roachpb.Error) {
	err := p.txn.Commit()
	// Reset transaction.
	p.resetTxn()
	return &valuesNode{}, err
}

// RollbackTransaction rolls back a transaction.
func (p *planner) RollbackTransaction(n *parser.RollbackTransaction) (planNode, *roachpb.Error) {
	err := p.txn.Rollback()
	// Reset transaction.
	p.resetTxn()
	return &valuesNode{}, err
}

// SetTransaction sets a transaction's isolation level
func (p *planner) SetTransaction(n *parser.SetTransaction) (planNode, *roachpb.Error) {
	if err := p.setIsolationLevel(n.Isolation); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}

func (p *planner) setIsolationLevel(level parser.IsolationLevel) *roachpb.Error {
	switch level {
	case parser.UnspecifiedIsolation:
		return nil
	case parser.SnapshotIsolation:
		return p.txn.SetIsolation(roachpb.SNAPSHOT)
	case parser.SerializableIsolation:
		return p.txn.SetIsolation(roachpb.SERIALIZABLE)
	default:
		return roachpb.NewErrorf("unknown isolation level: %s", level)
	}
}
