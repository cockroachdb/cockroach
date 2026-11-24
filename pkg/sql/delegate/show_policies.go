// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

func (d *delegator) delegateShowPolicies(stmt *tree.ShowPolicies) (tree.Statement, error) {
	query := `
			SELECT 
			policyname AS name,
			cmd,
			permissive AS type,
			roles,
			COALESCE(qual, '') AS using_expr,
			COALESCE(with_check, '') AS with_check_expr
			FROM pg_catalog.pg_policies
			WHERE schemaname = %[5]s AND tablename = %[2]s`

	return d.showTableDetails(stmt.Table, query)
}
