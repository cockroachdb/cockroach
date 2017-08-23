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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// ShowFingerprints statement fingerprints the data in each index of a table.
// For each index, a full index scan is run to hash every row with the fnv64
// hash. For the primary index, all table columns are included in the hash,
// whereas for secondary indexes, the index cols + the primary index cols + the
// STORING cols are included. The hashed rows are all combined with XOR using
// distsql.
//
// Our hash functions expect input of type BYTES (or string but we use bytes
// here), so we have to convert any datums that are not BYTES. This is currently
// done by round tripping through the string representation of the column
// (`::string::bytes`) and is an obvious area for improvement in the next
// version.
//
// To extract the fingerprints at some point in the past, the following
// query can be used:
//    SELECT * FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE foo] AS OF SYSTEM TIME xxx
func (p *planner) ShowFingerprints(
	ctx context.Context, n *parser.ShowFingerprints,
) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	tableDesc, err := MustGetTableDesc(
		ctx, p.txn, p.getVirtualTabler(), tn, true /*allowAdding*/)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(tableDesc, privilege.SELECT); err != nil {
		return nil, err
	}

	return &showFingerprintsNode{
		tableDesc: tableDesc,
		indexes:   tableDesc.AllNonDropIndexes(),
	}, nil
}

type showFingerprintsNode struct {
	optColumnsSlot

	tableDesc *sqlbase.TableDescriptor
	indexes   []sqlbase.IndexDescriptor

	rowIdx int
	// values stores the current row, updated by Next().
	values []parser.Datum
}

var showFingerprintsColumns = sqlbase.ResultColumns{
	{Name: "index", Typ: parser.TypeString},
	{Name: "fingerprint", Typ: parser.TypeString},
}

func (n *showFingerprintsNode) Start(params runParams) error {
	n.values = []parser.Datum{parser.DNull, parser.DNull}
	return nil
}
func (n *showFingerprintsNode) Values() parser.Datums   { return n.values }
func (n *showFingerprintsNode) Close(_ context.Context) {}
func (n *showFingerprintsNode) Next(params runParams) (bool, error) {
	if n.rowIdx >= len(n.indexes) {
		return false, nil
	}
	index := n.indexes[n.rowIdx]

	cols := make([]string, 0, len(n.tableDesc.Columns))
	addColumn := func(col sqlbase.ColumnDescriptor) {

		// TODO(dan): This is known to be a flawed way to fingerprint. Any datum
		// with the same string representation is fingerprinted the same, even
		// if they're different types.
		switch col.Type.SemanticType {
		case sqlbase.ColumnType_BYTES:
			cols = append(cols, fmt.Sprintf("%s:::bytes", parser.Name(col.Name)))
		default:
			cols = append(cols, fmt.Sprintf("%s::string::bytes", parser.Name(col.Name)))
		}
	}

	if index.ID == n.tableDesc.PrimaryIndex.ID {
		for _, col := range n.tableDesc.Columns {
			addColumn(col)
		}
	} else {
		colsByID := make(map[sqlbase.ColumnID]sqlbase.ColumnDescriptor)
		for _, col := range n.tableDesc.Columns {
			colsByID[col.ID] = col
		}
		colIDs := append(append(index.ColumnIDs, index.ExtraColumnIDs...), index.StoreColumnIDs...)
		for _, colID := range colIDs {
			col := colsByID[colID]
			addColumn(col)
		}
	}

	// The fnv64 hash was chosen mostly due to speed. I did an AS OF SYSTEM TIME
	// fingerprint over 31GiB on a 4 node production cluster (with no other
	// traffic to try and keep things comparable). The cluster was restarted in
	// between each run. Resulting times:
	//
	//  fnv => 17m
	//  sha512 => 1h6m
	//  sha265 => 1h6m
	//  fnv64 (again) => 17m
	//
	// TODO(dan): If/when this ever loses its EXPERIMENTAL prefix and gets
	// exposed to users, consider adding a version to the fingerprint output.
	sql := fmt.Sprintf(`SELECT
	  XOR_AGG(FNV64(%s))::string AS fingerprint
	  FROM [%d AS t]@{FORCE_INDEX=[%d],NO_INDEX_JOIN}
	`, strings.Join(cols, `,`), n.tableDesc.ID, index.ID)

	fingerprintCols, err := params.p.QueryRow(params.ctx, sql)
	if err != nil {
		return false, err
	}

	if len(fingerprintCols) != 1 {
		return false, errors.Errorf("unexpected number of columns returned: 1 vs %d",
			len(fingerprintCols))
	}
	fingerprint := fingerprintCols[0]

	n.values[0] = parser.NewDString(index.Name)
	n.values[1] = fingerprint
	n.rowIdx++
	return true, nil
}
