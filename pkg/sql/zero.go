// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// zeroNode is a planNode with no columns and no rows and is used for nodes that
// have no results. (e.g. a table for which the filtering condition has a
// contradiction).
type zeroNode struct {
	columns sqlbase.ResultColumns
}

func newZeroNode(columns sqlbase.ResultColumns) *zeroNode {
	return &zeroNode{columns: columns}
}

// NewZeroNode is the exported version of newZeroNode. Used by CCL.
func NewZeroNode(columns sqlbase.ResultColumns) PlanNode {
	return newZeroNode(columns)
}

func (*zeroNode) startExec(runParams) error    { return nil }
func (*zeroNode) Next(runParams) (bool, error) { return false, nil }
func (*zeroNode) Values() tree.Datums          { return nil }
func (*zeroNode) Close(context.Context)        {}
