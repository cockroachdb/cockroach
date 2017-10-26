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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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

func (n *scrubNode) Start(params runParams) error {
	tn, err := n.n.Table.NormalizeWithDatabaseName(params.p.session.Database)
	if err != nil {
		return err
	}

	tableDesc, err := params.p.getTableDesc(params.ctx, tn)
	if err != nil {
		return err
	}

	if tableDesc.IsView() {
		return pgerror.NewErrorf(pgerror.CodeSyntaxError, "cannot run SCRUB on views")
	}

	// Process SCRUB options
	var indexes []*sqlbase.IndexDescriptor
	for _, option := range n.n.Options {
		switch v := option.(type) {
		case *parser.ScrubOptionIndex:
			if indexes != nil {
				return pgerror.NewErrorf(pgerror.CodeSyntaxError,
					"cannot specify INDEX option more than once")
			}
			indexes, err = indexesToCheck(v.IndexNames, tableDesc)
			if err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("Unhandled SCRUB option received: %s", v))
		}
	}

	// No options were provided. By default exhaustive checks are run.
	if len(n.n.Options) == 0 {
		indexes, err = indexesToCheck(nil /* indexNames */, tableDesc)
		if err != nil {
			return err
		}
	}

	// TODO(joey): Squelch the "indexes not used" lint error for now.
	_ = indexes

	return nil
}

// indexesToCheck will return all of the indexes that are being checked.
// If indexNames is nil, then all indexes are returned.
func indexesToCheck(
	indexNames parser.NameList, tableDesc *sqlbase.TableDescriptor,
) (results []*sqlbase.IndexDescriptor, err error) {
	if indexNames == nil {
		// Populate results with all secondary indexes of the
		// table.
		for _, idx := range tableDesc.Indexes {
			results = append(results, &idx)
		}
		return results, nil
	}

	// Find the indexes corresponding to the user input index names.
	names := make(map[string]struct{})
	for _, idxName := range indexNames {
		names[idxName.String()] = struct{}{}
	}
	for _, idx := range tableDesc.Indexes {
		if _, ok := names[idx.Name]; ok {
			results = append(results, &idx)
			delete(names, idx.Name)
		}
	}
	if len(names) > 0 {
		// Get a list of all the indexes that could not be found.
		missingIndexNames := []string(nil)
		for _, idxName := range indexNames {
			if _, ok := names[idxName.String()]; ok {
				missingIndexNames = append(missingIndexNames, idxName.String())
			}
		}
		return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
			"specified indexes to check that do not exist on table %q: %v",
			tableDesc.Name, strings.Join(missingIndexNames, ", "))
	}
	return results, nil
}

func (n *scrubNode) Next(params runParams) (bool, error) { return false, nil }
func (n *scrubNode) Close(context.Context)               {}
func (n *scrubNode) Values() parser.Datums               { return nil }
