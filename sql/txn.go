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

import "github.com/cockroachdb/cockroach/sql/parser"

// BeginTransaction starts a new transaction.
func (p *planner) BeginTransaction(n *parser.BeginTransaction) (planNode, error) {
	if p.txn == nil {
		panic("the server should have already created a transaction")
	}
	return &valuesNode{}, nil
}

// CommitTransaction commits a transaction.
func (p *planner) CommitTransaction(n *parser.CommitTransaction) (planNode, error) {
	err := p.txn.Commit()
	// Reset transaction.
	p.txn = nil
	return &valuesNode{}, err
}

// RollbackTransaction rolls back a transaction.
func (p *planner) RollbackTransaction(n *parser.RollbackTransaction) (planNode, error) {
	err := p.txn.Rollback()
	// Reset transaction.
	p.txn = nil
	return &valuesNode{}, err
}
