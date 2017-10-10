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

type scrubNode struct {
	optColumnsSlot

	n *parser.Scrub
}

// Scrub checks the database.
// Privileges: security.RootUser user.
func (p *planner) Scrub(ctx context.Context, n *parser.Scrub) (planNode, error) {
	if err := p.RequireSuperUser("SCRUB"); err != nil {
		return nil, err
	}
	return &scrubNode{n: n}, nil
}

var scrubColumns = sqlbase.ResultColumns{
	{Name: "job_uuid", Typ: parser.TypeUUID},
	{Name: "error_type", Typ: parser.TypeString},
	{Name: "database", Typ: parser.TypeString},
	{Name: "table", Typ: parser.TypeString},
	{Name: "primary_key", Typ: parser.TypeString},
	{Name: "timestamp", Typ: parser.TypeTimestamp},
	{Name: "repaired", Typ: parser.TypeBool},
	{Name: "details", Typ: parser.TypeString},
}

func (*scrubNode) Start(params runParams) error        { return nil }
func (*scrubNode) Next(params runParams) (bool, error) { return false, nil }
func (*scrubNode) Close(context.Context)               {}
func (*scrubNode) Values() parser.Datums               { return nil }
