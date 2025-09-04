// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type scrubNode struct {
	zeroInputPlanNode
	nonReusablePlanNode
	optColumnsSlot
}

func (p *planner) Scrub(ctx context.Context, n *tree.Scrub) (planNode, error) {
	p.BufferClientNotice(ctx, pgnotice.Newf("EXPERIMENTAL SCRUB is deprecated. Use INSPECT instead for data consistency validation."))

	return &scrubNode{}, nil
}

func (n *scrubNode) startExec(params runParams) error { return nil }

func (n *scrubNode) Next(params runParams) (bool, error) { return false, nil }

func (n *scrubNode) Values() tree.Datums { return nil }

func (n *scrubNode) Close(ctx context.Context) {}
