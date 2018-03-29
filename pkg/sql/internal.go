// Copyright 2016 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// InternalExecutor can be used internally by cockroach to execute SQL
// statements without needing to open a SQL connection. InternalExecutor assumes
// that the caller has access to a cockroach KV client to handle connection and
// transaction management.
type InternalExecutor struct {
	ExecCfg *ExecutorConfig
}

var _ sqlutil.InternalExecutor = &InternalExecutor{}

// ExecuteStatementInTransaction executes the supplied SQL statement as part of
// the supplied transaction. Statements are currently executed as the root user
// with the system database as current database.
func (ie *InternalExecutor) ExecuteStatementInTransaction(
	ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
) (int, error) {
	// TODO(andrei): The use of the LeaseManager's memMetrics is very dubious. We
	// should probably pass in the metrics to use.
	p, cleanup := newInternalPlanner(
		opName, txn, security.RootUser, ie.ExecCfg.LeaseManager.memMetrics, ie.ExecCfg)
	defer cleanup()
	ie.initSession(p)
	return p.exec(ctx, statement, qargs...)
}

// QueryRowInTransaction executes the supplied SQL statement as part of the
// supplied transaction and returns the result. Statements are currently
// executed as the root user.
func (ie *InternalExecutor) QueryRowInTransaction(
	ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
) (tree.Datums, error) {
	p, cleanup := newInternalPlanner(
		opName, txn, security.RootUser, ie.ExecCfg.LeaseManager.memMetrics, ie.ExecCfg)
	defer cleanup()
	ie.initSession(p)
	return p.queryRow(ctx, statement, qargs...)
}

// QueryRowsInTransaction executes the supplied SQL statement as part of the
// supplied transaction and returns the resulting rows. Statements are currently
// executed as the root user.
func (ie *InternalExecutor) QueryRowsInTransaction(
	ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	p, cleanup := newInternalPlanner(
		opName, txn, security.RootUser, ie.ExecCfg.LeaseManager.memMetrics, ie.ExecCfg)
	defer cleanup()
	ie.initSession(p)
	rows, cols, err := p.queryRows(ctx, statement, qargs...)
	if err != nil {
		return nil, nil, err
	}
	return rows, cols, nil
}

// QueryRows is like QueryRowsInTransaction, except it runs a transaction
// internally. Committing the transaction and any required retries are handled
// transparently.
func (ie InternalExecutor) QueryRows(
	ctx context.Context, opName string, statement string, qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	var rows []tree.Datums
	var cols sqlbase.ResultColumns
	err := ie.ExecCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		rows, cols, err = ie.QueryRowsInTransaction(ctx, opName, txn, statement, qargs...)
		return err
	})
	return rows, cols, err
}

// ExecuteStatement is like ExecuteStatementInTransaction, except that it runs
// a transaction internally.
// Statements are currently executed as the root user with the system database as current database.
func (ie InternalExecutor) ExecuteStatement(
	ctx context.Context, opName string, statement string, qargs ...interface{},
) (int, error) {
	var numAffected int
	err := ie.ExecCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		numAffected, err = ie.ExecuteStatementInTransaction(ctx, opName, txn, statement, qargs...)
		return err
	})
	return numAffected, err
}

func (ie *InternalExecutor) initSession(p *planner) {
	p.extendedEvalCtx.NodeID = ie.ExecCfg.LeaseManager.LeaseStore.execCfg.NodeID.Get()
	p.extendedEvalCtx.Tables.leaseMgr = ie.ExecCfg.LeaseManager
}
