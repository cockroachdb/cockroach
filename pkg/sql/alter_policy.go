// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) AlterPolicy(ctx context.Context, n *tree.AlterPolicy) (planNode, error) {
	return nil, pgerror.New(pgcode.FeatureNotSupported,
		"ALTER POLICY is only implemented in the declarative schema changer")
}
