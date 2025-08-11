// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
)

func (d *delegator) delegateShowInspectErrors(n *tree.ShowInspectErrors) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.InspectErrors)

	if err := d.catalog.CheckPrivilege(d.ctx, syntheticprivilege.GlobalPrivilegeObject,
		d.catalog.GetCurrentUser(), privilege.INSPECT); err != nil {
		return nil, err
	}

	if n.TableName != nil {
		_, _, err := d.catalog.ResolveDataSource(d.ctx, resolveFlags, n.TableName)
		if err != nil {
			return nil, err
		}

	}

	var query strings.Builder
	query.WriteString("SELECT * FROM system.inspect_errors")

	return d.parse(query.String())
}
