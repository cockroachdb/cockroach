// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// errorIfRowsNode wraps another planNode and returns an error if the wrapped
// node produces any rows.
type ErrorIfRowsNode struct {
	singleInputPlanNode

	// MkErr creates the error message, given the values of the first row
	// produced.
	MkErr exec.MkErrFn

	nexted bool
}

func (n *errorIfRowsNode) StartExec(params runParams) error {
	return nil
}

func (n *errorIfRowsNode) Next(params runParams) (bool, error) {
	if n.nexted {
		return false, nil
	}
	n.nexted = true

	ok, err := n.Source.Next(params)
	if err != nil {
		return false, err
	}
	if ok {
		return false, n.MkErr(n.Source.Values())
	}
	return false, nil
}

func (n *errorIfRowsNode) Values() tree.Datums {
	return nil
}

func (n *errorIfRowsNode) Close(ctx context.Context) {
	n.Source.Close(ctx)
}


// Lowercase alias
type errorIfRowsNode = ErrorIfRowsNode
