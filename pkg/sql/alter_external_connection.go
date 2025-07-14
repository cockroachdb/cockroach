// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

const alterExternalConnectionOp = "ALTER EXTERNAL CONNECTION"

type alterExternalConnectionNode struct {
	zeroInputPlanNode
	n *tree.AlterExternalConnection
}

func (p *planner) AlterExternalConnection(
	ctx context.Context, n *tree.AlterExternalConnection,
) (planNode, error) {
	return &alterExternalConnectionNode{n: n}, nil
}

func (alt *alterExternalConnectionNode) startExec(params runParams) error {
	return params.p.alterExternalConnection(params, alt.n)
}

func (p *planner) alterExternalConnection(params runParams, n *tree.AlterExternalConnection) error {
	txn := p.InternalSQLTxn()

	exprEval := p.ExprEvaluator(alterExternalConnectionOp)
	name, err := exprEval.String(params.ctx, n.ConnectionLabelSpec.Label)
	if err != nil {
		return err
	}
	endpoint, err := exprEval.String(params.ctx, n.As)
	if err != nil {
		return err
	}

	ecPrivilege := &syntheticprivilege.ExternalConnectionPrivilege{
		ConnectionName: name,
	}
	if err := p.CheckPrivilege(params.ctx, ecPrivilege, privilege.UPDATE); err != nil {
		return err
	}

	existingConn, err := externalconn.LoadExternalConnection(params.ctx, name, txn)
	var notFoundErr *externalconn.ExternalConnectionNotFoundError
	if err != nil {
		if errors.As(err, &notFoundErr) && n.IfExists {
			return nil
		}
		return err
	}

	ex, ok := existingConn.(*externalconn.MutableExternalConnection)
	if !ok {
		return errors.AssertionFailedf("Failed to cast externalConnection (%s) to MutableExtneralConnection type", existingConn.ConnectionName())
	}

	if err = logAndSanitizeExternalConnectionURI(params.ctx, endpoint); err != nil {
		return errors.Wrap(err, "failed to log and santitize External Connection")
	}

	env := externalconn.MakeExternalConnEnv(
		params.ExecCfg().Settings,
		&params.ExecCfg().ExternalIODirConfig,
		params.ExecCfg().InternalDB,
		p.User(),
		params.ExecCfg().DistSQLSrv.ExternalStorageFromURI,
		false,
		false,
		&params.ExecCfg().DistSQLSrv.ServerConfig,
	)

	alterConn, err := externalconn.ExternalConnectionFromURI(
		params.ctx, env, endpoint,
	)
	if err != nil {
		return errors.Wrap(err, "failed to construct External Connection details")
	}

	ex.SetConnectionDetails(*alterConn.ConnectionProto())

	if err := ex.Update(params.ctx, txn); err != nil {
		return errors.Wrap(err, "failed to alter external connection")
	}

	return nil

}

func (alt *alterExternalConnectionNode) Close(_ context.Context) {}

func (alt *alterExternalConnectionNode) Next(params runParams) (bool, error) { return false, nil }

func (alt *alterExternalConnectionNode) Values() tree.Datums { return nil }
