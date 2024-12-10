// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func (p *planner) CreatePolicy(ctx context.Context, n *tree.CreatePolicy) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"CREATE POLICY",
	); err != nil {
		return nil, err
	}
	return nil, unimplemented.NewWithIssue(136696, "CREATE POLICY is not yet implemented")
}
