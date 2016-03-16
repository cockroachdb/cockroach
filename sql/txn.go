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
	"fmt"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
)

var deterministicPriorities = make(map[parser.UserPriority]int32)

// SetDeterministicPriority allows hardcoding priority values for SQL
// transaction priorities to avoid the randomness in priority generation.
// Subsequent transactions with the given priority level will alwys have the
// given priority value.
// Returns a function that removes the deterministic priority for this level.
func SetDeterministicPriority(level parser.UserPriority, priorityValue int32) func() {
	switch level {
	case parser.Low:
	case parser.Normal:
	case parser.High:
	default:
		panic(fmt.Sprintf("bad priority level %s", level))
	}
	deterministicPriorities[level] = priorityValue
	return func() { delete(deterministicPriorities, level) }
}

// BeginTransaction starts a new transaction.
func (p *planner) BeginTransaction(n *parser.BeginTransaction) (planNode, error) {
	if p.txn == nil {
		return nil, util.Errorf("the server should have already created a transaction")
	}
	if err := p.setIsolationLevel(n.Isolation); err != nil {
		return nil, err
	}
	if err := p.setUserPriority(n.UserPriority); err != nil {
		return nil, err
	}
	return &emptyNode{}, nil
}

// CommitTransaction commits a transaction.
func (p *planner) CommitTransaction(n *parser.CommitTransaction) (planNode, *roachpb.Error) {
	pErr := p.txn.Commit()
	// Reset transaction.
	p.resetTxn()
	return &emptyNode{}, pErr
}

// RollbackTransaction rolls back a transaction.
func (p *planner) RollbackTransaction(n *parser.RollbackTransaction) (planNode, *roachpb.Error) {
	pErr := p.txn.Rollback()
	// Reset transaction.
	p.resetTxn()
	return &emptyNode{}, pErr
}

// SetTransaction sets a transaction's isolation level
func (p *planner) SetTransaction(n *parser.SetTransaction) (planNode, error) {
	if err := p.setIsolationLevel(n.Isolation); err != nil {
		return nil, err
	}
	if err := p.setUserPriority(n.UserPriority); err != nil {
		return nil, err
	}
	return &emptyNode{}, nil
}

func (p *planner) setIsolationLevel(level parser.IsolationLevel) error {
	switch level {
	case parser.UnspecifiedIsolation:
		return p.txn.SetIsolation(p.session.DefaultIsolationLevel)
	case parser.SnapshotIsolation:
		return p.txn.SetIsolation(roachpb.SNAPSHOT)
	case parser.SerializableIsolation:
		return p.txn.SetIsolation(roachpb.SERIALIZABLE)
	default:
		return util.Errorf("unknown isolation level: %s", level)
	}
}

func (p *planner) setUserPriority(userPriority parser.UserPriority) error {
	if priVal, ok := deterministicPriorities[userPriority]; ok {
		p.txn.InternalSetPriority(priVal)
		return nil
	}

	switch userPriority {
	case parser.UnspecifiedUserPriority:
		return nil
	case parser.Low:
		return p.txn.SetUserPriority(roachpb.LowUserPriority)
	case parser.Normal:
		return p.txn.SetUserPriority(roachpb.NormalUserPriority)
	case parser.High:
		return p.txn.SetUserPriority(roachpb.HighUserPriority)
	default:
		return util.Errorf("unknown user priority: %s", userPriority)
	}
}
