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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const externalConnectionOp = "CREATE EXTERNAL CONNECTION"

type createExternalConectionNode struct {
	n *tree.CreateExternalConnection
}

// CreateExternalConnection represents a CREATE EXTERNAL CONNECTION statement.
func (p *planner) CreateExternalConnection(
	ctx context.Context, n *tree.CreateExternalConnection,
) (planNode, error) {
	return &createExternalConectionNode{n: n}, nil
}

func (c *createExternalConectionNode) startExec(params runParams) error {
	return params.p.createExternalConnection(params, c.n)
}

type externalConnection struct {
	name     string
	endpoint string
}

func (p *planner) parseExternalConnection(
	ctx context.Context, n *tree.CreateExternalConnection,
) (ec externalConnection, err error) {
	exprEval := p.ExprEvaluator(externalConnectionOp)
	if ec.name, err = exprEval.String(
		ctx, n.ConnectionLabelSpec.Label,
	); err != nil {
		return externalConnection{}, errors.Wrap(err, "failed to resolve External Connection name")
	}
	if ec.endpoint, err = exprEval.String(ctx, n.As); err != nil {
		return externalConnection{}, errors.Wrap(err, "failed to resolve External Connection endpoint")
	}
	return ec, nil
}

func (p *planner) createExternalConnection(
	params runParams, n *tree.CreateExternalConnection,
) error {
	if !p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.V22_2SystemExternalConnectionsTable) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"version %v must be finalized to create an External Connection",
			clusterversion.ByKey(clusterversion.V22_2SystemExternalConnectionsTable))
	}

	if err := params.p.CheckPrivilege(params.ctx, syntheticprivilege.GlobalPrivilegeObject,
		privilege.EXTERNALCONNECTION); err != nil {
		return pgerror.New(
			pgcode.InsufficientPrivilege,
			"only users with the EXTERNALCONNECTION system privilege are allowed to CREATE EXTERNAL CONNECTION")
	}

	// TODO(adityamaru): Add some metrics to track CREATE EXTERNAL CONNECTION
	// usage.

	ec, err := p.parseExternalConnection(params.ctx, n)
	if err != nil {
		return err
	}

	ex := externalconn.NewMutableExternalConnection()
	// TODO(adityamaru): Revisit if we need to reject certain kinds of names.
	ex.SetConnectionName(ec.name)

	// TODO(adityamaru): Create an entry in the `system.privileges` table for the
	// newly created External Connection with the appropriate privileges. We will
	// grant root/admin, and the user that created the object ALL privileges.

	if err = logAndSanitizeExternalConnectionURI(params.ctx, ec.endpoint); err != nil {
		return errors.Wrap(err, "failed to log and sanitize External Connection")
	}

	// Construct the ConnectionDetails for the external resource represented by
	// the External Connection.
	exConn, err := externalconn.ExternalConnectionFromURI(
		params.ctx, params.ExecCfg(), p.User(), ec.endpoint,
	)
	if err != nil {
		return errors.Wrap(err, "failed to construct External Connection details")
	}
	ex.SetConnectionDetails(*exConn.ConnectionProto())
	ex.SetConnectionType(exConn.ConnectionType())
	ex.SetOwner(p.User())

	txn := p.InternalSQLTxn()
	// Create the External Connection and persist it in the
	// `system.external_connections` table.
	if err := ex.Create(params.ctx, txn, p.User()); err != nil {
		return errors.Wrap(err, "failed to create external connection")
	}

	// Grant user `ALL` on the newly created External Connection.
	grantStatement := fmt.Sprintf(`GRANT ALL ON EXTERNAL CONNECTION "%s" TO %s`,
		ec.name, p.User().SQLIdentifier())
	_, err = txn.ExecEx(params.ctx,
		"grant-on-create-external-connection", txn.KV(),
		sessiondata.NodeUserSessionDataOverride, grantStatement)
	if err != nil {
		return errors.Wrap(err, "failed to grant on newly created External Connection")
	}
	return nil
}

func logAndSanitizeExternalConnectionURI(ctx context.Context, externalConnectionURI string) error {
	clean, err := cloud.SanitizeExternalStorageURI(externalConnectionURI, nil)
	if err != nil {
		return err
	}
	log.Ops.Infof(ctx, "external connection planning on connecting to destination %v", redact.Safe(clean))
	return nil
}

func (c *createExternalConectionNode) Next(_ runParams) (bool, error) { return false, nil }
func (c *createExternalConectionNode) Values() tree.Datums            { return nil }
func (c *createExternalConectionNode) Close(_ context.Context)        {}
