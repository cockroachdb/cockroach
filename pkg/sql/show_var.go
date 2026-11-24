// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// showVarNode represents a SHOW <var> statement.
// This is reached if <var> contains a period.
type showVarNode struct {
	zeroInputPlanNode
	name  string
	shown bool
	val   string
}

func (s *showVarNode) startExec(params runParams) error {
	return nil
}

func (s *showVarNode) Next(params runParams) (bool, error) {
	if s.shown {
		return false, nil
	}
	s.shown = true

	_, v, err := getSessionVar(s.name, false /* missingOk */)
	if err != nil {
		return false, err
	}
	s.val, err = v.Get(params.extendedEvalCtx, params.p.Txn())
	return true, err
}

func (s *showVarNode) Values() tree.Datums {
	return tree.Datums{tree.NewDString(s.val)}
}

func (s *showVarNode) Close(ctx context.Context) {}

// ShowVar shows a session variable.
func (p *planner) ShowVar(ctx context.Context, n *tree.ShowVar) (planNode, error) {
	return &showVarNode{name: strings.ToLower(n.Name)}, nil
}
