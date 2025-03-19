// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

func (d *delegator) delegateShowPolicies(stmt *tree.ShowPolicies) (tree.Statement, error) {
	query := `
			SELECT 
			p.polname AS name,
			CASE p.polcmd::text
					WHEN '*' THEN 'ALL'
					WHEN 'a' THEN 'INSERT'
					WHEN 'w' THEN 'UPDATE'
					WHEN 'd' THEN 'DELETE'
					WHEN 'r' THEN 'SELECT'
			END AS cmd,
			CASE p.polpermissive
					WHEN true THEN 'permissive'
					ELSE 'restrictive'
			END AS type,
			array_agg(
					CASE 
							WHEN role_id.uid = 0 THEN 'public'
							ELSE u.usename
					END
			) AS roles,
			COALESCE(p.polqual::text, '') AS using_expr,
			COALESCE(p.polwithcheck::text, '') AS with_check_expr
			FROM pg_policy p
			LEFT JOIN LATERAL unnest(p.polroles) AS role_id(uid) ON true
			LEFT JOIN pg_catalog.pg_user u ON u.usesysid = role_id.uid
			WHERE p.polrelid = %[6]d
			GROUP BY p.polname, p.polcmd, p.polpermissive, p.polqual, p.polwithcheck`

	return d.showTableDetails(stmt.Table, query)
}
