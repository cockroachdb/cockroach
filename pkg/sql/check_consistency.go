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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

type checkConsistencyNode struct {
	optColumnsSlot

	n        *parser.CheckConsistency
	desc     *sqlbase.TableDescriptor
	database string
}

// CheckConsistency checks the database.
// Privileges: security.RootUser user.
func (p *planner) CheckConsistency(
	ctx context.Context, n *parser.CheckConsistency,
) (planNode, error) {
	if err := p.RequireSuperUser("CHECK"); err != nil {
		return nil, err
	}
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	tableDesc, err := getTableDesc(ctx, p.txn, p.getVirtualTabler(), tn)
	if err != nil {
		return nil, err
	} else if tableDesc == nil {
		return nil, sqlbase.NewUndefinedRelationError(tn)
	}

	return &checkConsistencyNode{n: n, desc: tableDesc, database: tn.DatabaseName.Normalize()}, nil
}

var checkConsistencyColumns = sqlbase.ResultColumns{
	{Name: "JobUUID", Typ: parser.TypeUUID},
	{Name: "CheckFailure", Typ: parser.TypeString},
	{Name: "Database", Typ: parser.TypeString},
	{Name: "Table", Typ: parser.TypeString},
	{Name: "ConstraintName", Typ: parser.TypeString},
	{Name: "Columns", Typ: parser.TypeNameArray},
	{Name: "KeyPrefix", Typ: parser.TypeString},
	{Name: "PKeyID", Typ: parser.TypeString},
}

func (*checkConsistencyNode) Start(params runParams) error        { return nil }
func (*checkConsistencyNode) Next(params runParams) (bool, error) { return false, nil }
func (*checkConsistencyNode) Close(context.Context)               {}
func (*checkConsistencyNode) Values() parser.Datums               { return nil }
