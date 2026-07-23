// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type createLanguageNode struct {
	zeroInputPlanNode
	n *tree.CreateLanguage
}

// CreateLanguage handles a parameterless CREATE LANGUAGE statement. The
// grammar restricts the accepted languages to plpgsql, so execution is a
// no-op apart from a NOTICE indicating that PL/pgSQL is built in.
func (p *planner) CreateLanguage(_ context.Context, n *tree.CreateLanguage) (planNode, error) {
	return &createLanguageNode{n: n}, nil
}

func (n *createLanguageNode) startExec(params runParams) error {
	params.p.BufferClientNotice(params.ctx, pgnotice.Newf(
		"language %q is already included in CockroachDB by default", n.n.Name,
	))
	return nil
}

func (n *createLanguageNode) Next(runParams) (bool, error) { return false, nil }
func (n *createLanguageNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createLanguageNode) Close(context.Context)        {}
