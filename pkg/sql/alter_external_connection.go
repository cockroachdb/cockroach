// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

const alterExternalConnectionOp = "ALTER EXTERNAL CONNECTION"

type alterExternalConnectionNode struct {
	zeroInputPlanNode
	n *tree.AlterExternalConnection
}

// AlterExternalConnection represents a ALTER EXTERNAL CONNECTION statement.
func (p *planner) AlterExternalConnection(
	ctx context.Context, n *tree.AlterExternalConnection,
) (planNode, error) {
	return &alterExternalConnectionNode{n: n}, nil
}

func (c *alterExternalConnectionNode) startExec(params runParams) error {
	return params.p.alterExternalConnection(params, c.n)
}

func (p *planner) parseAlterExternalConnectionURI(
	ctx context.Context, n *tree.AlterExternalConnection,
) (endpoint string, err error) {

	exprEval := p.ExprEvaluator(alterExternalConnectionOp)
	if endpoint, err = exprEval.String(ctx, n.As); err != nil {
		return endpoint, errors.Wrap(err, "failed to resolve External Connection endpoint")
	}
	return endpoint, nil
}

func (p *planner) parseAlterExternalConnectionName(
	ctx context.Context, n *tree.AlterExternalConnection,
) (connectionName string, err error) {

	exprEval := p.ExprEvaluator(alterExternalConnectionOp)
	if connectionName, err = exprEval.String(
		ctx, n.ConnectionLabelSpec.Label,
	); err != nil {
		return connectionName, errors.Wrap(err, "failed to resolve External Connection name")
	}
	return connectionName, nil
}

func (p *planner) alterExternalConnection(
	params runParams, n *tree.AlterExternalConnection,
) error {
	txn := p.InternalSQLTxn()

	// Check user privilege, confirm user has EXTERNALCONNECTION access before allow modify existing external connection.
	if err := params.p.CheckPrivilege(params.ctx, syntheticprivilege.GlobalPrivilegeObject,
		privilege.EXTERNALCONNECTION); err != nil {
		return pgerror.New(
			pgcode.InsufficientPrivilege,
			"only users with the EXTERNALCONNECTION system privilege are allowed to CREATE EXTERNAL CONNECTION")
	}

	endpoint, err := p.parseAlterExternalConnectionURI(params.ctx, n)
	if err != nil {
		return err
	}
	connectionName, err := p.parseAlterExternalConnectionName(params.ctx, n)

	if err = logAndSanitizeExternalConnectionURI(params.ctx, endpoint); err != nil {
		return errors.Wrap(err, "failed to log and sanitize External Connection")
	}

	ex := externalconn.NewMutableExternalConnection()
	ex.SetConnectionName(connectionName)

	if err = logAndSanitizeExternalConnectionURI(params.ctx, endpoint); err != nil {
		return errors.Wrap(err, "failed to log and sanitize External Connection")
	}
	var SkipCheckingExternalStorageConnection bool
	var SkipCheckingKMSConnection bool
	if tk := params.ExecCfg().ExternalConnectionTestingKnobs; tk != nil {
		if tk.SkipCheckingExternalStorageConnection != nil {
			SkipCheckingExternalStorageConnection = params.ExecCfg().ExternalConnectionTestingKnobs.SkipCheckingExternalStorageConnection()
		}
		if tk.SkipCheckingKMSConnection != nil {
			SkipCheckingKMSConnection = params.ExecCfg().ExternalConnectionTestingKnobs.SkipCheckingKMSConnection()
		}
	}

	env := externalconn.MakeExternalConnEnv(
		params.ExecCfg().Settings,
		&params.ExecCfg().ExternalIODirConfig,
		params.ExecCfg().InternalDB,
		p.User(),
		params.ExecCfg().DistSQLSrv.ExternalStorageFromURI,
		SkipCheckingExternalStorageConnection,
		SkipCheckingKMSConnection,
		&params.ExecCfg().DistSQLSrv.ServerConfig,
	)

	exConn, err := externalconn.ExternalConnectionFromURI(
		params.ctx, env, endpoint,
	)
	if err != nil {
		return errors.Wrap(err, "failed to construct External Connection details")
	}
	ex.SetConnectionDetails(*exConn.ConnectionProto())
	ex.SetConnectionType(exConn.ConnectionType())
	ex.SetOwner(p.User())

	row, err := txn.QueryRowEx(params.ctx, `get-user-id`, txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT user_id FROM system.users WHERE username = $1`,
		p.User(),
	)
	if err != nil {
		return errors.Wrap(err, "failed to get owner ID for External Connection")
	}
	ownerID := tree.MustBeDOid(row[0]).Oid
	ex.SetOwnerID(ownerID)

	err = externalconn.ExternalConnectionExist(params.ctx, connectionName, ex.OwnerID(), txn)
	if err != nil {
		return errors.Wrap(err, "failed to check external connection existence")
	}

	// Alter the External Connection and persist it in the
	// `system.external_connections` table.
	if err := ex.Update(params.ctx, txn); err != nil {
		ifNotExists := n.ConnectionLabelSpec.IfNotExists
		if ifNotExists && pgerror.GetPGCode(err) == pgcode.DuplicateObject {
			return nil
		}
		return errors.Wrap(err, "failed to alter external connection")
	}
	return nil
}

func (c *alterExternalConnectionNode) Next(_ runParams) (bool, error) { return false, nil }
func (c *alterExternalConnectionNode) Values() tree.Datums            { return nil }
func (c *alterExternalConnectionNode) Close(_ context.Context)        {}
