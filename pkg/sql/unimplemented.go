// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// The below methods are ordered in alphabetical order. They represent statements
// which are UNIMPLEMENTED for the legacy schema changer.

func (p *planner) AlterPolicy(ctx context.Context, n *tree.AlterPolicy) (planNode, error) {
	return nil, makeUnimplementedLegacyError("ALTER POLICY")
}

func (p *planner) CommentOnType(ctx context.Context, n *tree.CommentOnType) (planNode, error) {
	return nil, makeUnimplementedLegacyError("COMMENT ON TYPE")
}

func (p *planner) CreatePolicy(ctx context.Context, n *tree.CreatePolicy) (planNode, error) {
	return nil, makeUnimplementedLegacyError("CREATE POLICY")
}

func (p *planner) CreateTrigger(_ context.Context, _ *tree.CreateTrigger) (planNode, error) {
	return nil, makeUnimplementedLegacyError("CREATE TRIGGER")
}

func (p *planner) DropOwnedBy(ctx context.Context) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP OWNED BY",
	); err != nil {
		return nil, err
	}

	return nil, makeUnimplementedLegacyError("DROP OWNED BY")
}

func (p *planner) DropPolicy(ctx context.Context, n *tree.DropPolicy) (planNode, error) {
	return nil, makeUnimplementedLegacyError("DROP POLICY")
}

func (p *planner) DropTrigger(_ context.Context, _ *tree.DropTrigger) (planNode, error) {
	return nil, makeUnimplementedLegacyError("DROP TRIGGER")
}

// makeUnimplementedLegacyError creates an error message with a hint and detail for a statement that
// is only implemented in the declarative schema changer and not in the legacy schema changer.
func makeUnimplementedLegacyError(stmtSyntax redact.SafeString) error {
	implicitTransactionHint := "This error may be happening due to running it in a multi-statement transaction." +
		" Try sending each schema change statement in its own implicit transaction."
	dscDocDetail := " See the documentation for additional details:" +
		docs.URL("online-schema-changes#declarative-schema-changer")

	return errors.WithDetail(
		errors.WithHint(
			pgerror.Newf(
				pgcode.FeatureNotSupported,
				"%s is only implemented in the declarative schema changer", stmtSyntax,
			),
			implicitTransactionHint,
		),
		dscDocDetail,
	)
}

// AlterTableSetLogged set table as unlogged or logged.
// No-op since unlogged tables are not supported.
func (p *planner) AlterTableSetLogged(
	ctx context.Context, n *tree.AlterTableSetLogged,
) (planNode, error) {
	operation := redact.SafeString("LOGGED")
	if !n.IsLogged {
		operation = redact.SafeString("UNLOGGED")
	}
	p.BufferClientNotice(
		ctx, pgnotice.Newf(
			"SET %s is not supported and has no effect",
			operation,
		),
	)
	return nil, nil
}
