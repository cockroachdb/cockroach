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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
)

type createStatsNode struct {
	tree.CreateStats
	tableDesc *sqlbase.ImmutableTableDescriptor
	columns   [][]sqlbase.ColumnID
}

func (p *planner) CreateStatistics(ctx context.Context, n *tree.CreateStats) (planNode, error) {
	var tableDesc *ImmutableTableDescriptor
	var err error
	switch t := n.Table.(type) {
	case *tree.TableName:
		// TODO(anyone): if CREATE STATISTICS is meant to be able to operate
		// within a transaction, then the following should probably run with
		// caching disabled, like other DDL statements.
		tableDesc, err = ResolveExistingObject(ctx, p, t, true /*required*/, requireTableDesc)
		if err != nil {
			return nil, err
		}

	case *tree.TableRef:
		flags := ObjectLookupFlags{CommonLookupFlags: CommonLookupFlags{
			avoidCached: p.avoidCachedDescriptors,
		}}
		tableDesc, err = p.Tables().getTableVersionByID(ctx, p.txn, sqlbase.ID(t.TableID), flags)
		if err != nil {
			return nil, err
		}
	}

	if tableDesc.IsVirtualTable() {
		return nil, errors.Errorf("cannot create statistics on virtual tables")
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
		return nil, err
	}

	if len(n.ColumnNames) == 0 {
		return createStatsDefaultColumns(n, tableDesc)
	}

	columns, err := tableDesc.FindActiveColumnsByNames(n.ColumnNames)
	if err != nil {
		return nil, err
	}
	columnIDs := make([]sqlbase.ColumnID, len(columns))
	for i := range columns {
		columnIDs[i] = columns[i].ID
	}

	return &createStatsNode{
		CreateStats: *n,
		tableDesc:   tableDesc,
		columns:     [][]sqlbase.ColumnID{columnIDs},
	}, nil
}

// createStatsDefaultColumns creates column statistics on a default set of
// columns when no columns were specified by the caller.
//
// To determine a useful set of default column statistics, we rely on
// information provided by the schema. In particular, the presence of an index
// on a particular set of columns indicates that the workload likely contains
// queries that involve those columns (e.g., for filters), and it would be
// useful to have statistics on prefixes of those columns. For example, if a
// table abc contains indexes on (a ASC, b ASC) and (b ASC, c ASC), we will
// collect statistics on a, {a, b}, b, and {b, c}.
// TODO(rytaft): This currently only generates one single-column stat per
// index. Add code to collect multi-column stats once they are supported.
func createStatsDefaultColumns(
	n *tree.CreateStats, desc *ImmutableTableDescriptor,
) (planNode, error) {
	pn := &createStatsNode{
		CreateStats: *n,
		tableDesc:   desc,
		columns:     make([][]sqlbase.ColumnID, 0, len(desc.Indexes)+1),
	}

	var requestedCols util.FastIntSet

	// If the primary key is not the hidden rowid column, collect stats on it.
	pkCol := desc.PrimaryIndex.ColumnIDs[0]
	if !isHidden(desc, pkCol) {
		pn.columns = append(pn.columns, []sqlbase.ColumnID{pkCol})
		requestedCols.Add(int(pkCol))
	}

	// Add columns for each secondary index.
	for i := range desc.Indexes {
		idxCol := desc.Indexes[i].ColumnIDs[0]
		if !requestedCols.Contains(int(idxCol)) {
			pn.columns = append(pn.columns, []sqlbase.ColumnID{idxCol})
			requestedCols.Add(int(idxCol))
		}
	}

	// If there are no non-hidden index columns, collect stats on the first
	// non-hidden column in the table.
	if len(pn.columns) == 0 {
		for i := range desc.Columns {
			if !desc.Columns[i].IsHidden() {
				pn.columns = append(pn.columns, []sqlbase.ColumnID{desc.Columns[i].ID})
				break
			}
		}
	}

	// If there are still no columns, return an error.
	if len(pn.columns) == 0 {
		return nil, errors.New("CREATE STATISTICS called on a table with no visible columns")
	}

	return pn, nil
}

func isHidden(desc *ImmutableTableDescriptor, columnID sqlbase.ColumnID) bool {
	for i := range desc.Columns {
		if desc.Columns[i].ID == columnID {
			return desc.Columns[i].IsHidden()
		}
	}
	panic("column not found in table")
}

func (*createStatsNode) Next(runParams) (bool, error) {
	return false, pgerror.NewAssertionErrorf("createStatsNode cannot be executed locally")
}
func (*createStatsNode) Close(context.Context) {}
func (*createStatsNode) Values() tree.Datums   { return nil }
