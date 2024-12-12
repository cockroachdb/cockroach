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

func (p *planner) ShowPolicies(context.Context, *tree.ShowPolicies) (planNode, error) {
	return nil, unimplemented.NewWithIssue(136757, "SHOW POLICIES is not yet implemented")
}
