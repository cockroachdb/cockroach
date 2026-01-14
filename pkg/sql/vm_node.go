// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/vm"
)

var vmNodePool = sync.Pool{
	New: func() interface{} {
		return &vmNode{}
	},
}

type vmNode struct {
	zeroInputPlanNode
	columns colinfo.ResultColumns
	rows    []tree.Datums
	p       vm.Program
	currRow int
}

func (p *planner) VMNode() *vmNode {
	n := vmNodePool.Get().(*vmNode)
	return n
}

// TODO(mgartner): Add memory accounting.
func (n *vmNode) startExec(params runParams) error {
	params.p.vm.Init(params.ctx, params.EvalContext(), n)
	params.p.vm.Exec(n.p)
	n.currRow = -1
	return nil
}

func (n *vmNode) Next(params runParams) (bool, error) {
	n.currRow++
	if n.currRow >= len(n.rows) {
		return false, nil
	}
	return true, nil
}

func (n *vmNode) Values() tree.Datums {
	return n.rows[n.currRow]
}

// OutputRow is part of the vm.Output interface.
// TODO(mgartner): Add memory accounting.
func (n *vmNode) OutputRow(row tree.Datums) {
	n.rows = append(n.rows, row)
}

func (n *vmNode) Close(context.Context) {
	*n = vmNode{}
	vmNodePool.Put(n)
}
