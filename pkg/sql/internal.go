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
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// InternalExecutor can be used internally by cockroach to execute SQL
// statements without needing to open a SQL connection. InternalExecutor assumes
// that the caller has access to a cockroach KV client to handle connection and
// transaction management.
type InternalExecutor struct {
	LeaseManager *LeaseManager
}

var _ sqlutil.InternalExecutor = InternalExecutor{}

// ExecuteStatementInTransaction executes the supplied SQL statement as part of
// the supplied transaction. Statements are currently executed as the root user.
func (ie InternalExecutor) ExecuteStatementInTransaction(
	ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
) (int, error) {
	p := makeInternalPlanner(opName, txn, security.RootUser, ie.LeaseManager.memMetrics)
	defer finishInternalPlanner(p)
	p.session.leases.leaseMgr = ie.LeaseManager
	return p.exec(ctx, statement, qargs...)
}

// QueryRowInTransaction executes the supplied SQL statement as part of the
// supplied transaction and returns the result. Statements are currently
// executed as the root user.
func (ie InternalExecutor) QueryRowInTransaction(
	ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
) (parser.Datums, error) {
	p := makeInternalPlanner(opName, txn, security.RootUser, ie.LeaseManager.memMetrics)
	defer finishInternalPlanner(p)
	p.session.leases.leaseMgr = ie.LeaseManager
	return p.QueryRow(ctx, statement, qargs...)
}

// GetTableSpan gets the key span for a SQL table, including any indices.
func (ie InternalExecutor) GetTableSpan(
	ctx context.Context, user string, txn *client.Txn, dbName, tableName string,
) (roachpb.Span, error) {
	// Lookup the table ID.
	p := makeInternalPlanner("get-table-span", txn, user, ie.LeaseManager.memMetrics)
	defer finishInternalPlanner(p)
	p.session.leases.leaseMgr = ie.LeaseManager

	tn := parser.TableName{DatabaseName: parser.Name(dbName), TableName: parser.Name(tableName)}
	tableID, err := getTableID(ctx, p, &tn)
	if err != nil {
		return roachpb.Span{}, err
	}

	// Determine table data span.
	tablePrefix := keys.MakeTablePrefix(uint32(tableID))
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	return roachpb.Span{Key: tableStartKey, EndKey: tableEndKey}, nil
}

// getTableID retrieves the table ID for the specified table.
func getTableID(ctx context.Context, p *planner, tn *parser.TableName) (sqlbase.ID, error) {
	if err := tn.QualifyWithDatabase(p.session.Database); err != nil {
		return 0, err
	}

	virtual, err := p.session.virtualSchemas.getVirtualTableDesc(tn)
	if err != nil {
		return 0, err
	}
	if virtual != nil {
		return virtual.GetID(), nil
	}

	dbID, err := p.session.leases.databaseCache.getDatabaseID(ctx, p.txn, p.getVirtualTabler(), tn.Database())
	if err != nil {
		return 0, err
	}

	nameKey := tableKey{dbID, tn.Table()}
	key := nameKey.Key()
	gr, err := p.txn.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if !gr.Exists() {
		return 0, sqlbase.NewUndefinedTableError(parser.AsString(tn))
	}
	return sqlbase.ID(gr.ValueInt()), nil
}
