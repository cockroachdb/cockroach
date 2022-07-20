// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

const dropExternalConnectionOp = "DROP EXTERNAL CONNECTION"

type dropExternalConnectionNode struct {
	n *tree.DropExternalConnection
}

// DropExternalConnection represents a DROP EXTERNAL CONNECTION statement.
func (p *planner) DropExternalConnection(
	_ context.Context, n *tree.DropExternalConnection,
) (planNode, error) {
	return &dropExternalConnectionNode{n: n}, nil
}

func (c *dropExternalConnectionNode) startExec(params runParams) error {
	return params.p.dropExternalConnection(params, c.n)
}

type dropExternalConnectionEval struct {
	externalConnectionName func() (string, error)
}

func (p *planner) makeDropExternalConnectionEval(
	ctx context.Context, n *tree.DropExternalConnection,
) (*dropExternalConnectionEval, error) {
	var err error
	eval := &dropExternalConnectionEval{}
	eval.externalConnectionName, err = p.TypeAsString(ctx, n.ConnectionLabel, externalConnectionOp)
	if err != nil {
		return nil, err
	}
	return eval, err
}

func (p *planner) dropExternalConnection(params runParams, n *tree.DropExternalConnection) error {
	// TODO(adityamaru): Check that the user has `DROP` privileges on the External
	// Connection once we add support for it. Remove admin only check.
	hasAdmin, err := params.p.HasAdminRole(params.ctx)
	if err != nil {
		return err
	}
	if !hasAdmin {
		return pgerror.New(
			pgcode.InsufficientPrivilege,
			"only users with the admin role are allowed to DROP EXTERNAL CONNECTION")
	}

	// TODO(adityamaru): Add some metrics to track DROP EXTERNAL CONNECTION
	// usage.

	eval, err := p.makeDropExternalConnectionEval(params.ctx, n)
	if err != nil {
		return err
	}

	name, err := eval.externalConnectionName()
	if err != nil {
		return errors.Wrap(err, "failed to resolve External Connection name")
	}

	if _ /* rows */, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
		params.ctx,
		dropExternalConnectionOp,
		params.p.Txn(),
		sessiondata.InternalExecutorOverride{User: params.p.User()},
		`DELETE FROM system.external_connections WHERE connection_name = $1`, name,
	); err != nil {
		return errors.Wrapf(err, "failed to delete external connection")
	}

	return nil
}

func (c *dropExternalConnectionNode) Next(_ runParams) (bool, error) { return false, nil }
func (c *dropExternalConnectionNode) Values() tree.Datums            { return nil }
func (c *dropExternalConnectionNode) Close(_ context.Context)        {}
