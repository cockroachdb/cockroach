// Copyright 2019 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// errorIfRowsNode wraps another planNode and returns an error if the wrapped
// node produces any rows.
type errorIfRowsNode struct {
	plan planNode

	// mkErr creates the error message, given the values of the first row
	// produced.
	mkErr exec.MkErrFn

	nexted bool
}

func (n *errorIfRowsNode) startExec(params runParams) error {
	return nil
}

func (n *errorIfRowsNode) Next(params runParams) (bool, error) {
	if n.nexted {
		return false, nil
	}
	n.nexted = true

	ok, err := n.plan.Next(params)
	if err != nil {
		return false, err
	}
	if ok {
		return false, n.mkErr(n.plan.Values())
	}
	return false, nil
}

func (n *errorIfRowsNode) Values() tree.Datums {
	return nil
}

func (n *errorIfRowsNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}
