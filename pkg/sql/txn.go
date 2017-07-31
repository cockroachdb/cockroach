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

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/pkg/errors"
)

// BeginTransaction starts a new transaction.
func (p *planner) BeginTransaction(n *parser.BeginTransaction) (planNode, error) {
	if p.txn == nil {
		return nil, errors.Errorf("the server should have already created a transaction")
	}
	if err := p.setTransactionModes(n.Modes); err != nil {
		return nil, err
	}

	// Enter the FirstBatch state.
	p.session.TxnState.SetState(FirstBatch)

	return &emptyNode{}, nil
}

// SetTransaction sets a transaction's isolation level
func (p *planner) SetTransaction(n *parser.SetTransaction) (planNode, error) {
	return &emptyNode{}, p.setTransactionModes(n.Modes)
}

func (p *planner) setTransactionModes(modes parser.TransactionModes) error {
	if err := p.setIsolationLevel(modes.Isolation); err != nil {
		return err
	}
	if err := p.setUserPriority(modes.UserPriority); err != nil {
		return err
	}
	if err := p.setReadWriteMode(modes.ReadWriteMode); err != nil {
		return err
	}
	return nil
}

func (p *planner) setIsolationLevel(level parser.IsolationLevel) error {
	var iso enginepb.IsolationType
	switch level {
	case parser.UnspecifiedIsolation:
		return nil
	case parser.SnapshotIsolation:
		iso = enginepb.SNAPSHOT
	case parser.SerializableIsolation:
		iso = enginepb.SERIALIZABLE
	default:
		return errors.Errorf("unknown isolation level: %s", level)
	}

	return p.session.TxnState.setIsolationLevel(iso)
}

func (p *planner) setUserPriority(userPriority parser.UserPriority) error {
	var up roachpb.UserPriority
	switch userPriority {
	case parser.UnspecifiedUserPriority:
		return nil
	case parser.Low:
		up = roachpb.MinUserPriority
	case parser.Normal:
		up = roachpb.NormalUserPriority
	case parser.High:
		up = roachpb.MaxUserPriority
	default:
		return errors.Errorf("unknown user priority: %s", userPriority)
	}
	return p.session.TxnState.setPriority(up)
}

// Note: This setting currently doesn't have any effect and therefor is not
// persisted anywhere. If this changes, care needs to be taken to restore it
// when ROLLBACK TO SAVEPOINT starts a new sql transaction.
func (p *planner) setReadWriteMode(readWriteMode parser.ReadWriteMode) error {
	switch readWriteMode {
	case parser.UnspecifiedReadWriteMode:
		return nil
	case parser.ReadOnly:
		return errors.New("read only not supported")
	case parser.ReadWrite:
		return nil
	default:
		return errors.Errorf("unknown read mode: %s", readWriteMode)
	}
}
