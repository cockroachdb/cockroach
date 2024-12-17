// Copyright 2020 The Cockroach Authors.
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

func (p *planner) DropPolicy(ctx context.Context, n *tree.DropPolicy) (planNode, error) {
	return nil, pgerror.New(pgcode.FeatureNotSupported,
		"DROP POLICY is only implemented in the declarative schema changer")
}
