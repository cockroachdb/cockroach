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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

func newTestScanNode(kvDB *client.DB, tableName string) (*scanNode, error) {
	desc := sqlbase.GetImmutableTableDescriptor(kvDB, sqlutils.TestDB, tableName)

	p := planner{}
	scan := p.Scan()
	scan.desc = desc
	err := scan.initDescDefaults(p.curPlan.deps, publicColumnsCfg)
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

	scan.spans, err = spansFromConstraint(
		desc, &desc.PrimaryIndex, nil /* constraint */, exec.ColumnOrdinalSet{}, false /* forDelete */)
	if err != nil {
		return nil, err
	}
	return scan, nil
}

func newTestJoinNode(kvDB *client.DB, leftName, rightName string) (*joinNode, error) {
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
