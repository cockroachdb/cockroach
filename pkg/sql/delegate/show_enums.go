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

func (d *delegator) delegateShowEnums(n *tree.ShowEnums) (tree.Statement, error) {
	query := `
  SELECT ns.nspname AS schema_name,
         type.typname AS name,
         array_agg(enum.enumlabel ORDER BY enum.enumsortorder) AS value
    FROM pg_enum AS enum
    JOIN pg_type AS type ON (type.oid = enum.enumtypid)
    JOIN pg_catalog.pg_namespace AS ns ON (ns.oid = type.typnamespace)
GROUP BY ns.nspname, type.typname
ORDER BY ns.nspname, type.typname`

	return parse(query)
}
