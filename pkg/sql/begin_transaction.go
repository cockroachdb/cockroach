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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// BeginTransaction starts a new transaction.
func (p *Planner) BeginTransaction(n *tree.BeginTransaction) (planNode, error) {
	if p.session.TxnState.State() != AutoRetry || p.txn == nil {
		return nil, errors.Errorf("the server should have already created a transaction. "+
			"state: %s", p.session.TxnState.State())
	}
	if err := p.setTransactionModes(n.Modes); err != nil {
		return nil, err
	}

	return &zeroNode{}, nil
}
