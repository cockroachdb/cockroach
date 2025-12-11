// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// ShowStatementHints returns a SHOW STATEMENT HINTS statement.
func (p *planner) ShowStatementHints(
	ctx context.Context, n *tree.ShowStatementHints,
) (planNode, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.StatementHints)

	columns := colinfo.ShowStatementHintsColumns
	if n.WithDetails {
		columns = colinfo.ShowStatementHintsDetailsColumns
	}

	// TODO(drewk): implement this.
	return newZeroNode(columns), nil
}
