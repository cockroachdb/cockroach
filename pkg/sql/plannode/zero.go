// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// zeroNode is a planNode with no columns and no rows and is used for nodes that
// have no results. (e.g. a table for which the filtering condition has a
// contradiction).
type ZeroNode struct {
	zeroInputPlanNode
	Columns colinfo.ResultColumns
}

func NewZeroNode(columns colinfo.ResultColumns) *ZeroNode {
	return &ZeroNode{Columns: columns}
}

func (*ZeroNode) StartExec(runParams) error    { return nil }
func (*ZeroNode) Next(runParams) (bool, error) { return false, nil }
func (*ZeroNode) Values() tree.Datums          { return nil }
func (*ZeroNode) Close(context.Context)        {}



// Lowercase alias
type zeroNode = ZeroNode

// Lowercase function alias
var newZeroNode = NewZeroNode
