// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var showExternalConnectionsColumns = colinfo.ResultColumns{
	{Name: "connection_name", Typ: types.String},
	{Name: "connection_uri", Typ: types.String},
	{Name: "connection_type", Typ: types.String},
}

// ShowExternalConnection returns external connections with their URIs redacted
// along with some other useful information.
func (p *planner) ShowExternalConnection(
	ctx context.Context, n *tree.ShowExternalConnections,
) (planNode, error) {
	const (
		connNameIdx = iota
		connURIIdx
		connTypeIdx
	)

	sqltelemetry.IncrementShowCounter(sqltelemetry.ExternalConnection)

	name := `SHOW EXTERNAL CONNECTIONS`
	if n.ConnectionLabel != nil {
		name = fmt.Sprintf(`SHOW EXTERNAL CONNECTION %s`, n.ConnectionLabel.String())
	}

	return &delayedNode{
		name:    name,
		columns: showExternalConnectionsColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			params := runParams{ctx: ctx, p: p, extendedEvalCtx: &p.extendedEvalCtx}
			connectionNames, err := getConnectionNames(params, n.ConnectionLabel,
				true /* checkUsagePrivilege */)
			if err != nil {
				return nil, err
			}
			connections, err := loadExternalConnections(params, connectionNames)
			if err != nil {
				return nil, err
			}

			v := p.newContainerValuesNode(showExternalConnectionsColumns, len(connections))
			for _, conn := range connections {
				row := tree.Datums{
					connNameIdx: tree.NewDString(conn.ConnectionName()),
					connURIIdx:  tree.NewDString(conn.RedactedConnectionURI()),
					connTypeIdx: tree.NewDString(conn.ConnectionType().String()),
				}
				if _, err := v.rows.AddRow(ctx, row); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
