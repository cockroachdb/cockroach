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
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/sql/sqlutil"
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
	txn *client.Txn, statement string, qargs ...interface{},
) (int, error) {
	p := makeInternalPlanner(txn, security.RootUser)
	p.leaseMgr = ie.LeaseManager
	return p.exec(statement, qargs...)
}

// GetTableSpan gets the key span for a SQL table, including any indices.
func (ie InternalExecutor) GetTableSpan(user string, txn *client.Txn, dbName, tableName string) (roachpb.Span, error) {
	// Lookup the table ID.
	p := makeInternalPlanner(txn, user)
	p.leaseMgr = ie.LeaseManager
	qname := &parser.QualifiedName{Base: parser.Name(tableName)}
	if err := qname.NormalizeTableName(dbName); err != nil {
		return roachpb.Span{}, err
	}
	tableID, err := getTableID(p, qname)
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
func getTableID(p *planner, qname *parser.QualifiedName) (sqlbase.ID, error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return 0, err
	}

	virtual, err := getVirtualTableDesc(qname)
	if err != nil {
		return 0, err
	}
	if virtual != nil {
		return virtual.GetID(), nil
	}

	dbID, err := p.getDatabaseID(qname.Database())
	if err != nil {
		return 0, err
	}

	nameKey := tableKey{dbID, qname.Table()}
	key := nameKey.Key()
	gr, err := p.txn.Get(key)
	if err != nil {
		return 0, err
	}
	if !gr.Exists() {
		return 0, sqlbase.NewUndefinedTableError(qname.String())
	}
	return sqlbase.ID(gr.ValueInt()), nil
}
