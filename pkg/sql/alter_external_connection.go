// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"

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
	if err := params.p.CheckPrivilege(params.ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.EXTERNALCONNECTION); err != nil {
		return pgerror.New(
			pgcode.InsufficientPrivilege,
			"only users with the EXTERNALCONNECTION system privilege are allowed to ALTER EXTERNAL CONNECTION",
		)
	}

	exprEval := p.ExprEvaluator(alterExternalConnectionOp)
	name, err := exprEval.String(params.ctx, n.ConnectionLabelSpec.Label)
	if err != nil {
		return err
	}
	endpoint, err := exprEval.String(params.ctx, n.As)
	if err != nil {
		return err
	}

	ec, err := externalconn.LoadExternalConnection(params.ctx, name, txn)
	if err != nil {
		if ok := strings.Contains(err.Error(), "does not exist"); ok && n.IfExists {
			return nil
		}
		return err
	}

	ex := externalconn.NewMutableExternalConnection()
	ex.SetConnectionName(ec.ConnectionName())
	if err = logAndSanitizeExternalConnectionURI(params.ctx, endpoint); err != nil {
		return errors.Wrap(err, "failed to log and santitize External Connection")
	}

	var skipCheckingExternalStorageConnection bool
	var skipCheckingKMSConnection bool
	if tk := params.ExecCfg().ExternalConnectionTestingKnobs; tk != nil {
		if tk.SkipCheckingExternalStorageConnection != nil {
			skipCheckingExternalStorageConnection = params.ExecCfg().ExternalConnectionTestingKnobs.SkipCheckingExternalStorageConnection()
		}
		if tk.SkipCheckingKMSConnection != nil {
			skipCheckingKMSConnection = params.ExecCfg().ExternalConnectionTestingKnobs.SkipCheckingKMSConnection()
		}
	}

	env := externalconn.MakeExternalConnEnv(
		params.ExecCfg().Settings,
		&params.ExecCfg().ExternalIODirConfig,
		params.ExecCfg().InternalDB,
		p.User(),
		params.ExecCfg().DistSQLSrv.ExternalStorageFromURI,
		skipCheckingExternalStorageConnection,
		skipCheckingKMSConnection,
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
		sessiondata.NoSessionDataOverride, `SELECT user_id FROM system.users WHERE username = $1`, p.User())
	if err != nil {
		return errors.Wrap(err, "failed to get owner ID for External Connection")
	}

	ownerID := tree.MustBeDOid(row[0]).Oid
	ex.SetOwnerID(ownerID)

	if err := ex.Update(params.ctx, txn); err != nil {
		return errors.Wrap(err, "failed to alter external connection")
	}

	grantStatement := fmt.Sprintf(`GRANT ALL ON EXTERNAL CONNECTION "%s" TO %s`,
		name, p.User().SQLIdentifier())
	_, err = txn.ExecEx(
		params.ctx, "grant-on-alter-external-connection", txn.KV(),
		sessiondata.NodeUserSessionDataOverride, grantStatement)

	if err != nil {
		return errors.Wrap(err, "failed to grant newly altered External Connection")
	}
	return nil

}

func (alt *alterExternalConnectionNode) Close(_ context.Context) {}

func (alt *alterExternalConnectionNode) Next(params runParams) (bool, error) { return false, nil }

func (alt *alterExternalConnectionNode) Values() tree.Datums { return nil }
