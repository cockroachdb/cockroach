// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/pkg/errors"
)

// SetTransaction sets a transaction's isolation level
func (p *Planner) SetTransaction(n *tree.SetTransaction) (PlanNode, error) {
	return &zeroNode{}, p.setTransactionModes(n.Modes)
}

func (p *Planner) setTransactionModes(modes tree.TransactionModes) error {
	if err := p.setIsolationLevel(modes.Isolation); err != nil {
		return err
	}
	if err := p.setUserPriority(modes.UserPriority); err != nil {
		return err
	}
	return p.setReadWriteMode(modes.ReadWriteMode)
}

func (p *Planner) setIsolationLevel(level tree.IsolationLevel) error {
	var iso enginepb.IsolationType
	switch level {
	case tree.UnspecifiedIsolation:
		return nil
	case tree.SnapshotIsolation:
		iso = enginepb.SNAPSHOT
	case tree.SerializableIsolation:
		iso = enginepb.SERIALIZABLE
	default:
		return errors.Errorf("unknown isolation level: %s", level)
	}

	return p.session.TxnState.setIsolationLevel(iso)
}

func (p *Planner) setUserPriority(userPriority tree.UserPriority) error {
	var up roachpb.UserPriority
	switch userPriority {
	case tree.UnspecifiedUserPriority:
		return nil
	case tree.Low:
		up = roachpb.MinUserPriority
	case tree.Normal:
		up = roachpb.NormalUserPriority
	case tree.High:
		up = roachpb.MaxUserPriority
	default:
		return errors.Errorf("unknown user priority: %s", userPriority)
	}
	return p.session.TxnState.setPriority(up)
}

func (p *Planner) setReadWriteMode(readWriteMode tree.ReadWriteMode) error {
	switch readWriteMode {
	case tree.UnspecifiedReadWriteMode:
		return nil
	case tree.ReadOnly:
		p.session.TxnState.setReadOnly(true)
	case tree.ReadWrite:
		p.session.TxnState.setReadOnly(false)
	default:
		return errors.Errorf("unknown read mode: %s", readWriteMode)
	}
	return nil
}
