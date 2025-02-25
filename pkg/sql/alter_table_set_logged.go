// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) AlterTableSetLogged(ctx context.Context, n *tree.AlterTableSetLogged) (planNode, error) {
	p.BufferClientNotice(
		ctx, pgnotice.Newf(
			"ALTER TABLE %s SET %s is not supported and has no effect",
			n.Name.ToTableName().ObjectName.Normalize(),
			map[bool]string{true: "LOGGED", false: "UNLOGGED"}[n.IsLogged],
		),
	)
	return nil, nil
}
