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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
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

func (p *planner) dropExternalConnection(params runParams, n *tree.DropExternalConnection) error {
	if !p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.TODODelete_V22_2SystemExternalConnectionsTable) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"External Connections are not supported until upgrade to version %v is finalized",
			clusterversion.ByKey(clusterversion.TODODelete_V22_2SystemExternalConnectionsTable))
	}

	// TODO(adityamaru): Add some metrics to track DROP EXTERNAL CONNECTION
	// usage.

	name, err := p.ExprEvaluator(externalConnectionOp).String(
		params.ctx, n.ConnectionLabel,
	)
	if err != nil {
		return errors.Wrap(err, "failed to resolve External Connection name")
	}

	// Check that the user has DROP privileges on the External Connection object.
	ecPrivilege := &syntheticprivilege.ExternalConnectionPrivilege{
		ConnectionName: name,
	}
	if err := p.CheckPrivilege(params.ctx, ecPrivilege, privilege.DROP); err != nil {
		return err
	}

	// DROP EXTERNAL CONNECTION is only allowed for users with the `DROP`
	// privilege on this object. We run the query as `node` since the user might
	// not have `SELECT` on the system table.
	if _ /* rows */, err = params.p.InternalSQLTxn().ExecEx(
		params.ctx,
		dropExternalConnectionOp,
		params.p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.external_connections WHERE connection_name = $1`, name,
	); err != nil {
		return errors.Wrapf(err, "failed to delete external connection")
	}

	// We must also DELETE all rows from system.privileges that refer to
	// external connection.
	if _, err = params.p.InternalSQLTxn().ExecEx(
		params.ctx,
		dropExternalConnectionOp,
		params.p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.privileges WHERE path = $1`, ecPrivilege.GetPath(),
	); err != nil {
		return errors.Wrapf(err, "failed to delete external connection")
	}

	return nil
}

func (c *dropExternalConnectionNode) Next(_ runParams) (bool, error) { return false, nil }
func (c *dropExternalConnectionNode) Values() tree.Datums            { return nil }
func (c *dropExternalConnectionNode) Close(_ context.Context)        {}
