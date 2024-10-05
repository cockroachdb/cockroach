// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var showCreateExternalConnectionColumns = colinfo.ResultColumns{
	{Name: "connection_name", Typ: types.String},
	{Name: "create_statement", Typ: types.String},
}

func loadExternalConnections(
	params runParams, n *tree.ShowCreateExternalConnections,
) ([]externalconn.ExternalConnection, error) {
	var connections []externalconn.ExternalConnection
	var rows []tree.Datums

	if n.ConnectionLabel != nil {
		name, err := params.p.ExprEvaluator(externalConnectionOp).String(
			params.ctx, n.ConnectionLabel,
		)
		if err != nil {
			return nil, err
		}
		rows = append(rows, tree.Datums{tree.NewDString(name)})
	} else {
		datums, _, err := params.p.InternalSQLTxn().QueryBufferedExWithCols(
			params.ctx,
			"load-external-connections",
			params.p.Txn(), sessiondata.NodeUserSessionDataOverride,
			"SELECT connection_name FROM system.external_connections")
		if err != nil {
			return nil, err
		}
		rows = append(rows, datums...)
	}

	for _, row := range rows {
		connectionName := tree.MustBeDString(row[0])
		connection, err := externalconn.LoadExternalConnection(
			params.ctx, string(connectionName), params.p.InternalSQLTxn(),
		)
		if err != nil {
			return nil, err
		}
		connections = append(connections, connection)
	}
	return connections, nil
}

func (p *planner) ShowCreateExternalConnection(
	ctx context.Context, n *tree.ShowCreateExternalConnections,
) (planNode, error) {
	var hasPrivileges bool
	var err error
	if hasPrivileges, err = p.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA, p.User()); err != nil {
		return nil, err
	}

	// If the user does not have VIEWCLUSTERMETADATA, and is running a `SHOW
	// CREATE EXTERNAL CONNECTION foo` check if the user is the owner of the
	// object.
	exprEval := p.ExprEvaluator(externalConnectionOp)
	if !hasPrivileges && n.ConnectionLabel != nil {
		name, err := exprEval.String(ctx, n.ConnectionLabel)
		if err != nil {
			return nil, err
		}
		ecPrivilege := &syntheticprivilege.ExternalConnectionPrivilege{
			ConnectionName: name,
		}
		isOwner, err := isOwner(ctx, p, ecPrivilege, p.User())
		if err != nil {
			return nil, err
		}
		if !isOwner {
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege, "must have %s privilege or be owner of the External Connection %q", privilege.VIEWCLUSTERMETADATA, name)
		}
	} else if !hasPrivileges {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege, "must have %s privilege to run `SHOW CREATE ALL EXTERNAL CONNECTIONS`", privilege.VIEWCLUSTERMETADATA)
	}

	sqltelemetry.IncrementShowCounter(sqltelemetry.CreateExternalConnection)

	name := `SHOW CREATE ALL EXTERNAL CONNECTIONS`
	if n.ConnectionLabel != nil {
		name = fmt.Sprintf(`SHOW CREATE EXTERNAL CONNECTION %s`, n.ConnectionLabel.String())
	}

	return &delayedNode{
		name:    name,
		columns: showCreateExternalConnectionColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			connections, err := loadExternalConnections(
				runParams{ctx: ctx, p: p, extendedEvalCtx: &p.extendedEvalCtx}, n)
			if err != nil {
				return nil, err
			}

			var rows []tree.Datums
			for _, conn := range connections {
				row := tree.Datums{
					scheduleIDIdx: tree.NewDString(conn.ConnectionName()),
					createStmtIdx: tree.NewDString(conn.UnredactedConnectionStatement()),
				}
				rows = append(rows, row)
			}

			v := p.newContainerValuesNode(showCreateTableColumns, len(rows))
			for _, row := range rows {
				if _, err := v.rows.AddRow(ctx, row); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
