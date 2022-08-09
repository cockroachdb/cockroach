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

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
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

type externalConnectionEval struct {
	externalConnectionName     func() (string, error)
	externalConnectionEndpoint func() (string, error)
}

func (p *planner) makeExternalConnectionEval(
	ctx context.Context, n *tree.CreateExternalConnection,
) (*externalConnectionEval, error) {
	var err error
	eval := &externalConnectionEval{}
	eval.externalConnectionName, err = p.TypeAsString(ctx, n.ConnectionLabelSpec.Label, externalConnectionOp)
	if err != nil {
		return nil, err
	}

	eval.externalConnectionEndpoint, err = p.TypeAsString(ctx, n.As, externalConnectionOp)
	return eval, err
}

func (p *planner) createExternalConnection(
	params runParams, n *tree.CreateExternalConnection,
) error {
	if !p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.SystemExternalConnectionsTable) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"version %v must be finalized to create an External Connection",
			clusterversion.ByKey(clusterversion.SystemExternalConnectionsTable))
	}

	if err := params.p.CheckPrivilege(params.ctx, syntheticprivilege.GlobalPrivilegeObject,
		privilege.EXTERNALCONNECTION); err != nil {
		return pgerror.New(
			pgcode.InsufficientPrivilege,
			"only users with the EXTERNALCONNECTION system privilege are allowed to CREATE EXTERNAL CONNECTION")
	}

	// TODO(adityamaru): Add some metrics to track CREATE EXTERNAL CONNECTION
	// usage.

	eval, err := p.makeExternalConnectionEval(params.ctx, n)
	if err != nil {
		return err
	}

	ex := externalconn.NewMutableExternalConnection()
	name, err := eval.externalConnectionName()
	if err != nil {
		return errors.Wrap(err, "failed to resolve External Connection name")
	}
	// TODO(adityamaru): Revisit if we need to reject certain kinds of names.
	ex.SetConnectionName(name)

	// TODO(adityamaru): Create an entry in the `system.privileges` table for the
	// newly created External Connection with the appropriate privileges. We will
	// grant root/admin, and the user that created the object ALL privileges.

	// Construct the ConnectionDetails for the external resource represented by
	// the External Connection.
	as, err := eval.externalConnectionEndpoint()
	if err != nil {
		return errors.Wrap(err, "failed to resolve External Connection endpoint")
	}
	exConn, err := externalconn.ExternalConnectionFromURI(params.ctx, params.ExecCfg(), p.User(), as)
	if err != nil {
		return errors.Wrap(err, "failed to construct External Connection details")
	}
	ex.SetConnectionDetails(*exConn.ConnectionProto())
	ex.SetConnectionType(exConn.ConnectionType())

	// Create the External Connection and persist it in the
	// `system.external_connections` table.
	return ex.Create(params.ctx, params.ExecCfg().InternalExecutor, params.p.User(), p.Txn())
}

func (c *createExternalConectionNode) Next(_ runParams) (bool, error) { return false, nil }
func (c *createExternalConectionNode) Values() tree.Datums            { return nil }
func (c *createExternalConectionNode) Close(_ context.Context)        {}
