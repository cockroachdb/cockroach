// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

func newTestScanNode(kvDB *kv.DB, tableName string) (*scanNode, error) {
	desc := sqlbase.TestingGetImmutableTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

	p := planner{alloc: &sqlbase.DatumAlloc{}}
	scan := p.Scan()
	scan.desc = desc
	var colCfg scanColumnsConfig
	for _, col := range desc.Columns {
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(col.ID))
	}
	err := scan.initDescDefaults(colCfg)
	if err != nil {
		return nil, err
	}
	// Initialize the required ordering.
	columnIDs, dirs := scan.index.FullColumnIDs()
	ordering := make(sqlbase.ColumnOrdering, len(columnIDs))
	for i, colID := range columnIDs {
		idx, ok := scan.colIdxMap[colID]
		if !ok {
			panic(fmt.Sprintf("index refers to unknown column id %d", colID))
		}
		ordering[i].ColIdx = idx
		ordering[i].Direction, err = dirs[i].ToEncodingDirection()
		if err != nil {
			return nil, err
		}
	}
	scan.reqOrdering = ordering
	sb := span.MakeBuilder(keys.SystemSQLCodec, desc.TableDesc(), &desc.PrimaryIndex)
	scan.spans, err = sb.SpansFromConstraint(nil /* constraint */, exec.TableColumnOrdinalSet{}, false /* forDelete */)
	if err != nil {
		return nil, err
	}
	return scan, nil
}

func newTestJoinNode(kvDB *kv.DB, leftName, rightName string) (*joinNode, error) {
	left, err := newTestScanNode(kvDB, leftName)
	if err != nil {
		return nil, err
	}
	right, err := newTestScanNode(kvDB, rightName)
	if err != nil {
		return nil, err
	}
	return &joinNode{
		left:  planDataSource{plan: left},
		right: planDataSource{plan: right},
	}, nil
}
