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

// The below methods are ordered in alphabetical order. They represent statements
// which are UNIMPLEMENTED for the legacy schema changer.

func (p *planner) AlterPolicy(ctx context.Context, n *tree.AlterPolicy) (planNode, error) {
	return nil, pgerror.New(pgcode.FeatureNotSupported,
		"ALTER POLICY is only implemented in the declarative schema changer")
}

func (p *planner) CommentOnType(ctx context.Context, n *tree.CommentOnType) (planNode, error) {
	return nil, pgerror.New(pgcode.FeatureNotSupported,
		"COMMENT ON TYPE is only implemented in the declarative schema changer")
}

func (p *planner) CreatePolicy(ctx context.Context, n *tree.CreatePolicy) (planNode, error) {
	return nil, pgerror.New(pgcode.FeatureNotSupported,
		"CREATE POLICY is only implemented in the declarative schema changer")
}

func (p *planner) DropOwnedBy(ctx context.Context) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP OWNED BY",
	); err != nil {
		return nil, err
	}

	return nil, pgerror.New(pgcode.FeatureNotSupported,
		"DROP OWNED BY is only implemented in the declarative schema changer")
}

func (p *planner) DropPolicy(ctx context.Context, n *tree.DropPolicy) (planNode, error) {
	return nil, pgerror.New(pgcode.FeatureNotSupported,
		"DROP POLICY is only implemented in the declarative schema changer")
}

func (p *planner) DropTrigger(_ context.Context, _ *tree.DropTrigger) (planNode, error) {
	return nil, pgerror.New(pgcode.FeatureNotSupported,
		"DROP TRIGGER is only implemented in the declarative schema changer")
}
