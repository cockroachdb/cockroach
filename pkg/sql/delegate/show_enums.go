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
	query := `
SELECT
	schema, name, string_agg(label, '|') AS value
FROM
	(
		SELECT
			nsp.nspname AS schema,
			type.typname AS name,
			enum.enumlabel AS label
		FROM
			pg_catalog.pg_enum AS enum
			JOIN pg_catalog.pg_type AS type ON (type.oid = enum.enumtypid)
			JOIN pg_catalog.pg_namespace AS nsp ON (type.typnamespace = nsp.oid)
		ORDER BY
			(enumtypid, enumsortorder)
	)
GROUP BY
	(schema, name)
ORDER BY
	(schema, name);
`
	return parse(query)
}
