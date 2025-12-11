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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var showStatementHintsColumns = colinfo.ResultColumns{
	{Name: "row_id", Typ: types.Int},
	{Name: "fingerprint", Typ: types.String},
	{Name: "hint_type", Typ: types.String},
	{Name: "created_at", Typ: types.TimestampTZ},
}

var showStatementHintsDetailsColumns = colinfo.ResultColumns{
	{Name: "row_id", Typ: types.Int},
	{Name: "fingerprint", Typ: types.String},
	{Name: "hint_type", Typ: types.String},
	{Name: "created_at", Typ: types.TimestampTZ},
	{Name: "details", Typ: types.String},
}

// ShowStatementHints returns a SHOW STATEMENT HINTS statement.
func (p *planner) ShowStatementHints(
	ctx context.Context, n *tree.ShowStatementHints,
) (planNode, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.StatementHints)

	columns := showStatementHintsColumns
	if n.WithDetails {
		columns = showStatementHintsDetailsColumns
	}

	// TODO(drewk): implement this.
	return newZeroNode(columns), nil
}
