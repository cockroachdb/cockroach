// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

func (d *delegator) delegateShowEnums() (tree.Statement, error) {
	// We can't query pg_enum directly as there are no rows in
	// pg_enum if we create an empty enum (e.g. CREATE TYPE x AS ENUM()).
	// Instead, use a CTE to aggregate enums, and use pg_type with an
	// enum filter to LEFT JOIN against the aggregated enums to ensure
	// we include these rows.
	query := `
WITH enums(enumtypid, values) AS (
	SELECT
		enums.enumtypid AS enumtypid,
		array_agg(enums.enumlabel) WITHIN GROUP (ORDER BY (enumsortorder)) AS values
	FROM pg_catalog.pg_enum AS enums
	GROUP BY enumtypid
)
SELECT
	nsp.nspname AS schema,
	types.typname AS name,
	values,
	rl.rolname AS owner
FROM
	pg_catalog.pg_type AS types
	LEFT JOIN enums ON (types.oid = enums.enumtypid)
	LEFT JOIN pg_catalog.pg_roles AS rl on (types.typowner = rl.oid)
	JOIN pg_catalog.pg_namespace AS nsp ON (types.typnamespace = nsp.oid)
WHERE types.typtype = 'e'
ORDER BY (nsp.nspname, types.typname)
`
	return parse(query)
}
