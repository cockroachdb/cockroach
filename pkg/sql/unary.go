// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// unaryNode is a planNode with no columns and a single row with empty results
// which is used by select statements that have no table. It is used for its
// property as the join identity.
type unaryNode struct {
	run unaryRun
}

// unaryRun contains the run-time state of unaryNode during local execution.
type unaryRun struct {
	consumed bool
}

func (*unaryNode) startExec(runParams) error {
	return nil
}

func (*unaryNode) Values() tree.Datums { return nil }

func (u *unaryNode) Next(runParams) (bool, error) {
	r := !u.run.consumed
	u.run.consumed = true
	return r, nil
}

func (*unaryNode) Close(context.Context) {}
