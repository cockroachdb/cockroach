// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func (p *planner) AlterPolicy(ctx context.Context, n *tree.AlterPolicy) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER POLICY",
	); err != nil {
		return nil, err
	}
	if n.NewPolicy != "" {
		return nil, unimplemented.NewWithIssue(136996, "ALTER POLICY is not yet implemented")
	}
	return nil, unimplemented.NewWithIssue(136997, "ALTER POLICY is not yet implemented")
}
